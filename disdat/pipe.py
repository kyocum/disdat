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
import time

import luigi

from disdat.pipe_base import PipeBase
from disdat.db_link import DBLink
from disdat.driver import DriverTask
from disdat.fs import DisdatFS
from disdat.common import BUNDLE_TAG_TRANSIENT, BUNDLE_TAG_PARAMS_PREFIX, ExtDepError
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
    output_tags = luigi.DictParameter(default={}, significant=False)

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

        # Instance variables to track various user wishes
        self.user_set_human_name = None  # self.set_bundle_name()
        self.user_tags = {}              # self.add_tags()
        self.add_deps = {}               # self.add(_external)_dependency()
        self.db_targets = []             # Deprecating
        self._input_tags = {}            # self.get_tags() of upstream tasks
        self._input_bundle_uuids = {}    # self.get_bundle_uuid() of upstream tasks
        self._mark_force = False         # self.mark_force()

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
        Given a pipe instance, return the "processing_name" -- a unique string based on the class name and
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

        """ NOTE: If a user changes a task param in run(), and that param parameterizes a dependency in requires(), 
        then running requires() post run() will give different tasks.  To be safe we record the inputs before run() 
        """
        cached_bundle_inputs = self.bundle_inputs()

        try:
            start = time.time()  # P3 datetime.now().timestamp()
            user_rtn_val = self.pipe_run(**kwargs)
            stop = time.time()  # P3 datetime.now().timestamp()
        except Exception as error:
            """ If user's pipe fails for any reason, remove bundle dir and raise """
            try:
                _logger.error("User pipe_run encountered exception: {}".format(error))
                PipeBase.rm_bundle_dir(pce.path, pce.uuid, self.db_targets)
            except OSError as ose:
                _logger.error("User pipe_run encountered error, and error on remove bundle: {}".format(ose))
            raise

        try:
            presentation, frames = PipeBase.parse_return_val(pce.uuid, user_rtn_val, self.data_context)

            hfr = PipeBase.make_hframe(frames,
                                       pce.uuid,
                                       cached_bundle_inputs,
                                       self.pipeline_id(),
                                       self.pipe_id(),
                                       self,
                                       start_ts=start,
                                       stop_ts=stop,
                                       tags={"presentable": "True"},
                                       presentation=presentation)

            # Add any output tags to the user tag dict
            if self.output_tags:
                self.user_tags.update(self.output_tags)

            # If this is the root_task, identify it as so in the tag dict
            if isinstance(self.calling_task, DriverTask):
                self.user_tags.update({'root_task': 'True'})

            # Lastly add any parameters associated with this class as tags.
            # They are differentiated by a special prefix in the key
            self.user_tags.update(self._get_subcls_params())

            # Overwrite the hyperframe tags with the complete set of tags
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

    def _get_subcls_params(self):
        """ Given the child class, extract user defined Luigi parameters

        This function uses vars(cls) and filters by Luigi Parameter
        types.  Luigi get_params() gives us all parameters in the full class hierarchy.
        It would give us the parameters in this class as well.  And then we'd have to do set difference.
        See luigi.Task.get_params()

        NOTE: We do NOT keep the parameter order maintained by Luigi.  That's critical for Luigi creating the task_id.
        However, we can implicitly re-use that ordering if we re-instantiate the Luigi class.

        Args:
            self: The instance of the subclass.  To get the normalized values for the Luigi Parameters
        Returns:
            dict: {BUNDLE_TAG_PARAM_PREFIX.<name>:'string value',...}
        """
        cls = self.__class__
        params = {}
        for param in vars(cls):
            attribute = getattr(cls, param)
            if isinstance(attribute, luigi.Parameter):
                params["{}{}".format(BUNDLE_TAG_PARAMS_PREFIX, param)] = attribute.serialize(getattr(self, param))
        return params

    @classmethod
    def _put_subcls_params(cls, ser_params):
        """ Given the child class, create the Luigi parameter dictionary

        Assume that ser_params dictionary keys are the attribute names in the Disdat task class.

        Args:
            self: The instance of the subclass.  To get the normalized values for the Luigi Parameters
            ser_params (dict): Dictionary <str>:<str>
        Returns:
            deser_params (dict): {<name>: Luigi.Parameter,...}
        """
        # cls = self.__class__
        deser_params = {}
        for param, ser_value in ser_params.items():
            try:
                attribute = getattr(cls, param)
                assert isinstance(attribute, luigi.Parameter)
                deser_params[param] = attribute.parse(ser_value)
            except Exception as e:
                _logger.warning("Bundle parameter ({}:{}) unable to be deserialized by class({}): {}".format(param,
                                                                                                             ser_value,
                                                                                                             cls.__name__,
                                                                                                             e))
                raise e
        return deser_params

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

            # Reset the stored tags, in case this instance is run multiple times.
            self._input_tags = {}
            self._input_bundle_uuids = {}

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

                self._input_tags[user_arg_name] = hfr.tag_dict
                self._input_bundle_uuids[user_arg_name] = pce.uuid
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

    def add_external_dependency(self, param_name, task_class, params, human_name=None, uuid=None):
        """
        Disdat Pipe API Function

        Add an external task and its parameters to our requirements.   What this means is that
        there is no run function and, in that case, Luigi will ignore the results of task.deps() (which calls
        flatten(self.requires())).  And what that means is that this requirement can only be satisfied
        by the bundle actually existing.

        NOTE: if you add an external dependency by name, it is possible that someone adds a bundle during
        execution and that your requires function is no longer deterministic.   You must add caching to your
        requires function to handle this scenario.

        Example with class variable bundle_uuid:
        ``
        if self.bundle_uuid is None:
            bundle = self.add_external_dependency('_', MyTaskClass, {}, human_name='some_result')
            self.bundle_uuid = bundle.uuid
        else:
            bundle = self.add_external_dependency('_', MyTaskClass, {}, uuid=self.bundle_uuid)
        ``

        Args:
            param_name (str): The parameter name this bundle assumes when passed to Pipe.run
            task_class (:object):  Must always set class name of upstream task if it was created from a PipeTask.   May be None if made by API.
            params (:dict):  Dictionary of parameters for this task.  Note if UUID is set, then params are ignored!
            human_name (str): Resolve dependency by human_name, return the latest bundle with that humman_name.  Trumps task_class and params.
            uuid (str): Resolve dependency by explicit UUID, trumps task_class and params, and human_name.

        Returns:
            None

        """
        # TODO: Store PipeTask class name so look ups by name or uuid do not require user to specify it.

        # for the bundle object
        import disdat.api as api

        if not isinstance(params, dict):
            error = "add_dependency third argument must be a dictionary of parameters"
            raise Exception(error)

        if task_class is None:
            task_class = api.BundleWrapperTask

        assert (param_name not in self.add_deps)

        try:
            if uuid is not None:
                hfr = self.pfs.get_hframe_by_uuid(uuid, data_context=self.data_context)
            elif human_name is not None:
                hfr = self.pfs.get_latest_hframe(human_name, data_context=self.data_context)
            else:
                p = task_class(**params)
                hfr = self.pfs.get_hframe_by_proc(p.pipe_id(), data_context=self.data_context)

            if hfr is None:
                raise ExtDepError("Unable to resolve external bundle made by class ({})".format(task_class))

            bundle = api.Bundle(self.data_context.get_local_name(), 'unknown')

            bundle.fill_from_hfr(hfr)

            # if we found by uuid or human name, the hfr should have the params with which
            # the task was called, so we need to grab them.
            if uuid is not None or human_name is not None:
                params = task_class._put_subcls_params(bundle.params)

        except ExtDepError as error:
            bundle = None
        except Exception as error:
            _logger.warning("Unable to resolve external bundle class[{}] name[{}] uuid[{}]: error [{}]".format(task_class,
                                                                                                               human_name,
                                                                                                               uuid,
                                                                                                               error))
            bundle = None
        finally:
            self.add_deps[param_name] = (luigi.task.externalize(task_class), params)

        return bundle

    def add_db_target(self, db_target):
        """
        Unimplemented: deprecated until we revisit semantics of DB bundle links.

        Allow user to record a versioned database table.   Behind the scenes the system records
        the dsn, database, schema, and table (information sufficient to operate on the table).  On "commit' a
        db link will create a view to the latest version of the database table.

        Note: We add through the DBTarget object create, not through
        pipe.create_db_target() in the case that people do some hacking and don't use that API.

        Args:
            db_target (`db_target.DBTarget`):

        Returns:
            None
        """
        assert False, "add_db_target is deprecated until we revisit semantics of DB bundle links."
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

    def get_tags(self, user_arg_name):
        """
        Disdat Pipe API Function

        Retrieve the tag dictionary from an upstream task.

        Args:
            user_arg_name (str): keyword arg name of input bundle data for which to return tags

        Returns:
            tags (dict (str, str)): key value pairs (string, string)
        """
        assert user_arg_name in self._input_tags
        return self._input_tags[user_arg_name]

    def get_bundle_uuid(self, user_arg_name):
        """
        Disdat Pipe API Function

        Retrieve the UUID from an upstream task.

        Args:
            user_arg_name (str): keyword arg name of input bundle data for which to return tags

        Returns:
            uuid (str)
        """
        assert user_arg_name in self._input_bundle_uuids
        return self._input_bundle_uuids[user_arg_name]

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
