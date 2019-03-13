#
# Copyright 2015, 2016  Human Longevity, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
Pipe

A "task" in disdat.

This inherits from Luigi's Abstract Task Class

The idea is that parameters are actually the parameters to the run function
requires is the tasks that have to run before this task runs

output() is basically a function that says, given the parameters, what is the output
of this task.

inputs() isn't used as much, but it says, here is a list of inputs I expect to be
available before I run.

author: Kenneth Yocum
"""

import os
import json
import six

import luigi

from disdat.pipe_base import PipeBase
from disdat.db_link import DBLink
from disdat.driver import DriverTask
from disdat.fs import DisdatFS
from disdat.common import BUNDLE_TAG_TRANSIENT, BUNDLE_TAG_PARAMS_PREFIX
from disdat import logger as _logger


class PipeTask(luigi.Task, PipeBase):
    """
    user_arg_name:
    calling_task:
    driver_output_bundle:
    force:
    output_tags:
    incremental_push:
    incremental_pull:
    """
    user_arg_name = luigi.Parameter(default=None, significant=False)  # what the outputs are referred to by downstreams
    calling_task   = luigi.Parameter(default=None, significant=False)

    # This is used for re-running in apply:resolve_bundle to manually see if
    # we need to re-run the root task.
    driver_output_bundle = luigi.Parameter(default='None', significant=False)

    force = luigi.BoolParameter(default=False, significant=False)
    output_tags = luigi.DictParameter(default={}, significant=True)

    # Each pipeline executes wrt a data context.
    data_context = luigi.Parameter(default=None, significant=False)

    # Each pipeline can be configured to commit and push intermediate values to the remote
    incremental_push = luigi.BoolParameter(default=False, significant=False)

    # Each pipeline can be configured to pull intermediate values on demand from the remote
    incremental_pull = luigi.BoolParameter(default=False, significant=False)

    def __init__(self, *args, **kwargs):
        """
        This has the same signature as luigi.Task.
        Go through this class and get the set of params we define

        Args:
            *args:
            **kwargs:
        """

        super(PipeTask, self).__init__(*args, **kwargs)

        self.user_set_human_name = None
        self.user_tags = {}
        self.add_deps  = {}
        self.db_targets = []
        self._mark_force = False

    def bundle_outputs(self):
        """
        Each pipe creates an output bundle name

        Idea:  WorkflowName + task.task_id == [Pipe Class Name + params + hash]

        For now: Apply Output Bundle "-" Pipe Class Name
        """

        output_bundles = [(self.pipe_id(), self.pfs.get_path_cache(self).uuid)]
        return output_bundles

    def bundle_inputs(self):
        """
        Given this pipe, return the set of bundles created by the input pipes.
        Mirrors Luigi task.inputs()

        NOTE: Calls task.deps which calls task._requires which calls task.requires()

        :param pipe_task:  A PipeTask or a DriverTask (both implement PipeBase)
        :return:  [(bundle_name, uuid), ... ]

        """

        input_tasks = self.deps()
        input_bundles = [(task.pipe_id(), self.pfs.get_path_cache(task).uuid) for task in input_tasks]
        return input_bundles

    def pipe_id(self):
        """
        Given a pipe instance, return a unique string based on the class name and
        the parameters.  This re-uses Luigi code for getting the unique string.

        NOTE: The PipeTask has a 'driver_output_bundle'.  This the name of the pipline output bundle given by the user.
        Because this is a Luigi parameter, it is included in the Luigi self.task_id string and hash.  So we do not
        have to append this separately.

        """
        return self.task_id

    def pipeline_id(self):
        """
        This is the "human readable" name;  a "less unique" id than the unique id.

        The pipeline_id is well-defined for the output task -- it is the output bundle name.   For intermediate outputs
        the pipeline_id defaults to the pipe_id().   Else, it may be set by the task author.

        Note: Should we provide an identify for which version of this pipe is running at which stage in the pipeline?
        Short answer, no.   Imagine if we name with the pipeline bundle output name, branch index, and level index.  In
        this case if anyone re-uses this output, the human_name for the bundle won't be meaningful.   For the pipeline
        owner, it may also not be helpful.  The system may also place different outputs at different times under those
        indices.  Too complicated.

        Returns:
            (str)

        """

        if self.driver_output_bundle is not None:
            return self.driver_output_bundle
        elif self.user_set_human_name is not None:
            return self.user_set_human_name
        else:
            id_parts = self.pipe_id().split('_')
            return "{}_{}".format(id_parts[0],id_parts[-1])

    def get_hframe_uuid(self):
        """ Return the unique ID for this tasks current output hyperframe

        Returns:
            hframe_uuid (str): The unique identifier for this task's hyperframe
        """

        pce = self.pfs.get_path_cache(self)
        assert (pce is not None)

        return pce.uuid

    def upstream_hframes(self):
        """ Convert upstream tasks to hyperframes, return list of hyperframes

        Returns:
            (:list:`hyperframe.HyperFrameRecord`): list of upstream hyperframes
        """

        tasks = self.requires()
        hfrs = []
        for t in tasks:
            hfid = t.get_hframe_uuid()
            hfrs.append(self.pfs.get_hframe_by_uuid(hfid, data_context=self.data_context))

        return hfrs

    def requires(self):
        """
        Return Tasks on which this task depends.

        Build them intelligently, however.
        1.) The input_df so far stays the same for all upstream pipes.
        2.) However, when we resolve the location of the outputs, we need to do so correctly.

        :return:
        """

        kwargs = self.prepare_pipe_kwargs()

        self.add_deps.clear()
        self.pipe_requires(**kwargs)
        rslt = self.add_deps

        if len(self.add_deps) == 0:
            return []

        tasks = []

        for user_arg_name, cls_and_params in rslt.items():
            pipe_class, params = cls_and_params[0], cls_and_params[1]

            if isinstance(pipe_class, six.string_types):
                """ if it is a string, find the Python task class """
                pipe_class = DriverTask.get_task_cls(pipe_class)

            assert isinstance(pipe_class, luigi.task_register.Register)

            # we propagate the same inputs and the same output dir for every upstream task!
            params.update({
                'user_arg_name': user_arg_name,
                'calling_task': self,
                'driver_output_bundle': None,  # allow intermediate tasks pipe_id to be independent of root task.
                'force': self.force,
                'output_tags': dict({}),  # do not pass output_tags up beyond root task
                'data_context': self.data_context,  # all operations wrt this context
                'incremental_push': self.incremental_push,  # propagate the choice to push incremental data.
                'incremental_pull': self.incremental_pull  # propagate the choice to incrementally pull data.
            })
            tasks.append(pipe_class(**params))

        return tasks

    def output(self):
        """
        This is the *only* output function for all pipes.  It declares the creation of the
        one HyperFrameRecord pb and that's it.  Remember, has to be idempotent.

        Return:
            (list:str):

        """

        return PipeBase.add_bundle_meta_files(self)

    def run(self):
        """

        Call users run function.
        1.) prepare the arguments
        2.) run and gather user result
        3.) interpret and wrap in a HyperFrame

        Returns:
            (`hyperframe.HyperFrame`):

        """

        kwargs = self.prepare_pipe_kwargs(for_run=True)

        pce = self.pfs.get_path_cache(self)

        assert(pce is not None)

        try:
            user_rtn_val = self.pipe_run(**kwargs)
        except Exception as error:
            """ If user's pipe fails for any reason, remove bundle dir and raise """
            try:
                _logger.error("User pipe_run encountered exception: {}".format(error))
                PipeBase.rm_bundle_dir(pce.path, pce.uuid, self.db_targets)
            except OSError as ose:
                _logger.error("User pipe_run encountered error, and error on remove bundle: {}".format(ose))
            raise

        try:
            hfr = PipeBase.parse_pipe_return_val(pce.uuid, user_rtn_val, self.data_context, self)

            # Add Luigi Task parameters -- Only add the class parameters.  These are Disdat special params.
            self.user_tags.update(self._get_subcls_params(self))

            if self.output_tags:
                self.user_tags.update(self.output_tags)

            if isinstance(self.calling_task, DriverTask):
                self.user_tags.update({'root_task': 'True'})

            if self.user_tags:
                hfr.replace_tags(self.user_tags)

            self.data_context.write_hframe(hfr)

            transient = False
            if hfr.get_tag(BUNDLE_TAG_TRANSIENT) is not None:
                transient = True

            if self.incremental_push and not transient:
                self.pfs.commit(None, None, uuid=pce.uuid, data_context=self.data_context)
                self.pfs.push(uuid=pce.uuid, data_context=self.data_context)

        except Exception as error:
            """ If we fail for any reason, remove bundle dir and raise """
            PipeBase.rm_bundle_dir(pce.path, pce.uuid, self.db_targets)
            raise

        return hfr

    @classmethod
    def _get_subcls_params(cls, self):
        """ Given the child class, extract user defined Luigi parameters

        The right way to do this is to use vars(cls) and filter by Luigi Parameter
        types.  Luigi get_params() gives us all parameters in the full class hierarchy.
        It would give us the parameters in this class as well.  And then we'd have to do set difference.
        See luigi.Task.get_params()

        NOTE: We do NOT keep the parameter order maintained by Luigi.  That's critical for Luigi creating the task_id.
        However, we can implicitly re-use that ordering if we re-instantiate the Luigi class.

        Args:
            cls: The subclass with defined parameters.  To tell which variables are Luigi Parameters
            self: The instance of the subclass.  To get the normalized values for the Luigi Parameters
        Returns:
            dict: (BUNDLE_TAG_PARAM_PREFIX.<name>:'string value',...)
        """
        params = []
        subcls_params = vars(cls) # vars on a class, not dir
        for p in subcls_params:
            param_obj = getattr(cls, p)
            if not isinstance(param_obj, luigi.Parameter):
                continue
            val = getattr(self,p)
            if isinstance(val, six.string_types):
                ser_val = json.dumps(val)
            else:
                ser_val = param_obj.serialize(getattr(self, p))  # serialize the param_obj.normalize(x)
            params.append(("{}{}".format(BUNDLE_TAG_PARAMS_PREFIX, p), ser_val))

        return dict(params)

        # If we put the code in self.__init__() luigi hasn't created instance variables yet.
        # This was code to do that.
        # only_pipe_params = PipeTask.get_params()
        # all_params = self.get_params()
        # only_subcls_params = tuple(set(all_params) - set(only_pipe_params))
        # This is pretty gross.  Get the params that are unique to this pipe
        # Pull them out of kwargs (that's how we build this task! we don't use args!)
        # filtered_kwargs = {k: kwargs[k] for k, v in only_subcls_params if k in kwargs}  # @UnusedVariable
        # self.only_subcls_param_values = self.get_param_values(only_subcls_params, [], filtered_kwargs)

    def prepare_pipe_kwargs(self, for_run=False):
        """ Each upstream task produces a bundle.  Prepare that bundle as input
        to the user's pipe_run function.

        Args:
            for_run (bool): prepare args for run -- at that point all upstream tasks have completed.

        Returns:
            (dict): A dictionary with the arguments.

        """

        kwargs = dict()

        # Place upstream task outputs into the kwargs.  Thus the user does not call
        # self.inputs().  If they did, they would get a list of output targets for the bundle
        # that isn't very helpful.
        if for_run:
            upstream_tasks = [(t.user_arg_name, self.pfs.get_path_cache(t)) for t in self.requires()]
            for user_arg_name, pce in [u for u in upstream_tasks if u[1] is not None]:
                hfr = self.pfs.get_hframe_by_uuid(pce.uuid, data_context=self.data_context)
                assert hfr.is_presentable()

                # Download any data that is not local (the linked files are not present).
                # This is the default behavior when running in a container.
                # The non-default is to download and localize ALL bundles in the context before we run.
                # That's in-efficient.   We only need meta-data to determine what to re-run.
                if self.incremental_pull:
                    DisdatFS()._localize_hfr(hfr, pce.uuid, self.data_context)

                if pce.instance.user_arg_name in kwargs:
                    _logger.warning('Task human name {} reused when naming task dependencies: Dependency hyperframe shadowed'.format(pce.instance.user_arg_name))

                kwargs[user_arg_name] = self.data_context.present_hfr(hfr)

        return kwargs

    """
    Pipes Interface -- A pipe implements these calls
    """

    def pipe_requires(self, **kwargs):
        """
        This is the place to put your pipeline dependencies.  Place
        the upstream pipes in an array and a dict for their params

        Args:
            **kwargs:

        Returns:

        """

        return None

    def pipe_run(self, **kwargs):
        """
        There is only one default argument "input_df" in kwargs.
        The other keys in kwargs will be identical to your Luigi parameters specified in this class.

        The input_df has the data context identifiers, e.g., sampleName, sessionId, subjectId
        The input_df has the data in either jsonData or fileData.
        A sharded task will receive a subset of all possible inputs.

        Args:
            **kwargs:

        Returns:

        """

        raise NotImplementedError()

    def add_dependency(self, name, task_class, params):
        """
        Disdat Pipe API Function

        Add a task and its parameters to our requirements

        Args:
            name (str): Name of our upstream (also name of argument in downstream)
            task_class (:object):  upstream task class
            params (:dict):  Dictionary of

        Returns:
            None

        """

        if not isinstance(params, dict):
            error = "add_dependency third argument must be a dictionary of parameters"
            raise Exception(error)

        assert (name not in self.add_deps)
        self.add_deps[name] = (task_class, params)

        return

    def add_external_dependency(self, name, task_class, params):
        """
        Disdat Pipe API Function

        Add an external task and its parameters to our requirements.   What this means is that
        there is no run function and, in that case, Luigi will ignore the results of task.deps() (which calls
        flatten(self.requires())).  And what that means is that this requirement can only be satisfied
        by the bundle actually existing.

        Args:
            name (str): Name of our upstream (also name of argument in downstream)
            task_class (:object):  upstream task class
            params (:dict):  Dictionary of

        Returns:
            None

        """

        if not isinstance(params, dict):
            error = "add_dependency third argument must be a dictionary of parameters"
            raise Exception(error)

        assert (name not in self.add_deps)
        self.add_deps[name] = (luigi.task.externalize(task_class), params)

        return

    def add_db_target(self, db_target):
        """
        Every time the user creates a db target, we add
        it to the list of db_targets in this pipe.

        Note: We add through the DBTarget object create, not through
        pipe.create_db_target() in the case that people do some hacking and don't use that API.

        Args:
            db_target (`db_target.DBTarget`):

        Returns:
            None
        """
        self.db_targets.append(db_target)

    def create_output_table(self, dsn, table_name, schema_name=None):
        """
        Create an output table target.  Use the target to parameterize queries with the
        target table name.

        Args:
            dsn (unicode): The dsn indicating the configuration to connect to the db
            table_name (unicode): The table name.
            schema_name (unicode): Optional force use of schema (default None)

        Returns:
            (`disdat.db_target.DBTarget`)

        """
        target = DBLink(self, dsn, table_name, schema_name=schema_name)

        return target

    def create_output_file(self, filename):
        """
        Disdat Pipe API Function

        Pass in the name of your file, and get back an object to which you can write.
        Under the hood, this is a Luigi.Target.

        Args:
            filename:  The name of your file, not the path.

        Returns:
            (`luigi.Target`):

        """

        return self.make_luigi_targets_from_basename(filename)

    def create_output_dir(self, dirname):
        """
        Disdat Pipe API Function

        Given basename directory name, return a fully qualified path whose prefix is the
        local output directory for this bundle in the current context.  This call creates the
        output directory as well.

        Args:
            dirname (str): The name of the output directory, i.e., "models"

        Returns:
            output_dir (str):  Fully qualified path of a directory whose prefix is the bundle's local output directory.

        """

        prefix_dir = self.get_output_dir()
        fqp = os.path.join(prefix_dir, dirname)
        try:
            os.makedirs(fqp)
        except IOError as why:
            _logger.error("Creating directory in bundle directory failed:".format(why))

        return fqp

    def get_output_dir(self):
        """
        Disdat Pipe API Function

        Retrieve the output directory for this task's bundle.  You may place
        files directly into this directory.

        Returns:
            output_dir (str):  The bundle's output directory

        """

        # Find the path cache entry for this pipe to find its output path
        pce = self.pfs.get_path_cache(self)
        assert(pce is not None)

        return pce.path

    def set_bundle_name(self, human_name):
        """
        Disdat Pipe API Function

        Set the human name for this bundle.  If not called, then intermediate outputs
        will have human names identical to their process names.

        Args:
            human_name (str):  The human name of this pipe's output bundle.

        Returns:
            None

        """

        self.user_set_human_name = human_name

    def add_tags(self, tags):
        """
        Disdat Pipe API Function

        Adds tags to bundle.

        Args:
            tags (dict (str, str)): key value pairs (string, string)

        Returns:
            None
        """
        assert (isinstance(tags, dict))
        self.user_tags.update(tags)

    def mark_force(self):
        """
        Disdat Pipe API Function

        Mark pipe to force recompution of this particular task.   This means that Disdat/Luigi will
        always re-run this particular pipe / task.

        We mark the pipe with a particular flag so that apply.resolve_bundle()

        Returns:
            None
        """
        self._mark_force = True

    def mark_transient(self):
        """
        Disdat Pipe API Function

        Mark output bundle as transient.   This means that during execution Disdat will not
        write (push) this bundle back to the remote.  That only happens in two cases:
        1.) Started the pipeline with incremental_push=True
        2.) Running the pipeline in a container with no_push or no_push_intermediates False

        We mark the bundle with a tag.   Incremental push investigates the tag before pushing.
        And the entrypoint investigates the tag if we are not pushing incrementally.
        Otherwise, normal push commands from the CLI or api will work, i.e., manual pushes continue to work.

        Returns:
            None
        """
        self.add_tags({BUNDLE_TAG_TRANSIENT: 'True'})
