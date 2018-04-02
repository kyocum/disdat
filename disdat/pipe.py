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

from disdat.pipe_base import PipeBase
from disdat.db_target import DBTarget
import disdat.common as common
from disdat.driver import DriverTask
import shutil
import luigi
import logging
import json


CLOSURE_PIPE_INPUT = 'pipeline_input'

_logger = logging.getLogger(__name__)


class PipesExternalBundle(luigi.ExternalTask, PipeBase):
    """
    DEPRECATED
    """

    input_bundle_name = luigi.Parameter(default='None')

    def bundle_outputs(self):
        """
        Given this pipe, return output bundle and uuid.
        Returns:  [(bundle_name, uuid), ... ]
        """
        output_bundles = [(self.pipe_id(), self.pfs.get_path_cache(self).uuid)]
        return output_bundles

    def bundle_inputs(self):
        """
        Given this pipe, return the set of bundles used as inputs

        Returns: [(bundle_name, uuid), ... ]

        """

        return []

    def pipe_id(self):
        """

        Returns:
        """
        return self.input_bundle_name

    def pipeline_id(self):
        """

        Returns:
        """
        return self.input_bundle_name

    def requires(self):
        """

        Returns:

        """
        return None

    def output(self):
        """
        DEPRECATED
        Read the latest version of the given input_bundle
        and transform the first row into luigi file objects as outputs
        Returns: {'col': luigitarget, ... }
        """
        pass


class PipeTask(luigi.Task, PipeBase):
    """
    Every pipe is given:
    inputs -- 0 or more json records we convert into an input_df.  This is the input closure.

    Every pipe emits files to a directory:
    outpath -- the directory to which outputs will be sent

    NOTE:  We pass in closure_bundle_proc_name and closure_bundle_uuid because a.) they are the params
      for this invocation, and it allows us to return them as dependencies in bundle_inputs().  We don't
      use these to resolve the json inputs because that's the driver's job (apply only passes single row).
    """
    user_arg_name = luigi.Parameter(default=None, significant=False)  # what the outputs are referred to by downstreams
    calling_task   = luigi.Parameter(default=None, significant=False)
    closure_hframe = luigi.Parameter(default=None, significant=False)

    # Driver and PipeTask always set these
    closure_bundle_proc_name = luigi.Parameter(default='None', significant=False)
    closure_bundle_uuid = luigi.Parameter(default='None', significant=False)

    # Driver is the only one who sets these (thus only on the first task)
    # They are only used to drive re-running -- to make unique task_id
    closure_bundle_proc_name_root = luigi.Parameter(default='None', significant=True)
    closure_bundle_uuid_root = luigi.Parameter(default='None', significant=True)

    # This is used for re-running in apply:resolve_bundle to manually see if
    # we need to re-run the root task.
    driver_output_bundle = luigi.Parameter(default='None', significant=False)

    force = luigi.BoolParameter(default=False, significant=False)
    output_tags = luigi.DictParameter(default={}, significant=True)

    def __init__(self, *args, **kwargs):
        """
        This has the same signature as luigi.Task.
        Go through this class and get the set of params we define

        Args:
            *args:
            **kwargs:
        """

        super(PipeTask, self).__init__(*args, **kwargs)

        if common.PUT_LUIGI_PARAMS_IN_FUNC_PARAMS:
            # NOTE: Default is not to do this.
            # Set up param values to be used in prepare_pipe_kwargs
            # Determine all params -- these are lists of tuples [('name', param value),]
            # Note: using get_param_values, not get_params, which returns parameter objects.
            only_pipe_params = PipeTask.get_params()
            all_params = self.get_params()
            only_subcls_params = tuple(set(all_params) - set(only_pipe_params))

            # This is pretty gross.  Get the params that are unique to this pipe
            # Pull them out of kwargs (that's how we build this task! we don't use args!)
            filtered_kwargs = {k: kwargs[k] for k, v in only_subcls_params if k in kwargs}  # @UnusedVariable
            self.only_subcls_param_values = self.get_param_values(only_subcls_params, [], filtered_kwargs)

        # Get the presentable input params from the closure_hframe
        if self.closure_hframe is not None:
            assert self.closure_hframe.is_presentable()
            self.presentable_inputs = self.pfs.get_curr_context().present_hfr(self.closure_hframe)
        else:
            self.presentable_inputs = None
        self.user_set_human_name = None
        self.user_tags = {}
        self.add_deps  = {}
        self.db_targets = []

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

        # The lineage does *not* drive re-running.  So we can safely say this pipe depends on
        # the input bundle, because it did.   Note that the re-run logic does depend on the task_id() and
        # that is a hash of all the args.  And the first (bottom / root) task of a transform has a
        # closure_bundle and closure_uuid.   If either change, then we re-run.
        if self.closure_bundle_proc_name is not None:
            # if we had a closure object -- a pipeline input bundle -- then record it.
            input_bundles.append((self.closure_bundle_proc_name, self.closure_bundle_uuid))

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
        This is a "less unique" id than the unique id.  It is supposed to be the "human readable" name of the stage
        this pipe occupies in the pipesline.

        The pipeline_id is well-defined for the output task -- it is output bundle name.   For intermediate outputs
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
            hfrs.append(self.pfs.get_hframe_by_uuid(hfid))

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

        for user_arg_name, cls_and_params in rslt.iteritems():
            pipe_class, params = cls_and_params[0], cls_and_params[1]

            if isinstance(pipe_class, str) or isinstance(pipe_class, unicode):
                """ if it is a string, find the Python task class """
                pipe_class = DriverTask.get_task_cls(pipe_class)

            assert isinstance(pipe_class, luigi.task_register.Register)

            # we propagate the same inputs and the same output dir for every upstream task!
            if pipe_class.__name__ is 'PipesExternalBundle':
                tasks.append(pipe_class(**params))
            else:
                params.update({
                    'user_arg_name': user_arg_name,
                    'calling_task': self,
                    'closure_hframe': self.closure_hframe,
                    'closure_bundle_proc_name': self.closure_bundle_proc_name,
                    'closure_bundle_uuid': self.closure_bundle_uuid,
                    'closure_bundle_proc_name_root': self.closure_bundle_proc_name,
                    'closure_bundle_uuid_root': self.closure_bundle_uuid,
                    'driver_output_bundle': None,  # allow intermediate tasks pipe_id to be independent of root task.
                    'force': self.force,
                    'output_tags': dict({})  # do not pass output_tags up beyond root task
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

        def rm_bundle_dir():
            """
            We created a directory (managed path) to hold the bundle and any files.   The files have been
            copied in.   Removing the directory removes any created files.  However we also need to
            clean up any temporary tables as well.

            TODO: Integrate with data_context bundle remove.   That deals with information already
            stored in the local DB.

            ASSUMES:  That we haven't actually updated the local DB with information on this bundle.

            Returns:
                None
            """
            try:
                shutil.rmtree(pce.path)

                # if people create s3 files, s3 file targets, inside of an s3 context,
                # then we will have to clean those up as well.

                for t in self.db_targets:
                    t.drop_table()

            except IOError as why:
                _logger.error("Removal of hyperframe directory {} failed with error {}. Continuing removal...".format(
                    pce.uuid, why))

        kwargs = self.prepare_pipe_kwargs(for_run=True)

        pce = self.pfs.get_path_cache(self)

        assert(pce is not None)

        try:
            user_rtn_val = self.pipe_run(**kwargs)
        except Exception as error:
            """ If user's pipe fails for any reason, remove bundle dir and raise """
            rm_bundle_dir()
            raise

        try:
            hfr = self.parse_pipe_return_val(pce.uuid, user_rtn_val, human_name=self.pipeline_id())

            if self.output_tags:
                self.user_tags.update(self.output_tags)

            if isinstance(self.calling_task, DriverTask):
                self.user_tags.update({'root_task': 'True'})

            if self.user_tags:
                hfr.replace_tags(self.user_tags)

            self.pfs.write_hframe(hfr)
        except Exception as error:
            """ If we fail for any reason, remove bundle dir and raise """
            rm_bundle_dir()
            raise

        return hfr

    def prepare_pipe_kwargs(self, for_run=False):
        """
        Convert the Luigi Parameters that are buried in "self" to a set of
        named arguments that are passed through the function call.

        The first parameter is the set of presentables from the input hyperframe.  It is keyed under
        CLOSURE_PIPE_INPUT.

        Args:
            for_run (bool): prepare args for run -- at that point all upstream tasks have completed.

        Returns:
            (dict): A dictionary with the arguments.

        """

        kwargs = dict()

        # 1.) Place input hyperframe presentable into the users's run / requires function
        kwargs[CLOSURE_PIPE_INPUT] = self.presentable_inputs

        if common.PUT_LUIGI_PARAMS_IN_FUNC_PARAMS:
            # 2.) Transparently place user's Luigi Params (Task object variables)
            # This is a list of tuples of name, parameter object.  If from command line, it is json,
            # if it is the users default value it should throw an exception.
            for user_pipe_param_name, user_pipe_param in self.only_subcls_param_values:
                if user_pipe_param is not None:
                    try:
                        deserialized = json.loads(user_pipe_param)
                    except ValueError:
                        deserialized = user_pipe_param
                    except TypeError:
                        deserialized = user_pipe_param
                    # NOTE: old code made DF if deserialized was a dict: pd.DataFrame.from_dict(deserialized)
                    kwargs[user_pipe_param_name] = deserialized

        # Place upstream task outputs into the kwargs.  Thus the user does not call
        # self.inputs().  If they did, they would get a list of output targets for the bundle
        # this isn't very helpful.
        if for_run:
            upstream_tasks = [(t.user_arg_name, self.pfs.get_path_cache(t)) for t in self.requires()]
            for user_arg_name, pce in [u for u in upstream_tasks if u[1] is not None]:
                hfr = self.pfs.get_hframe_by_uuid(pce.uuid)
                assert hfr.is_presentable()
                if pce.instance.user_arg_name in kwargs:
                    _logger.warning('Task human name {} reused when naming task dependencies: Dependency hyperframe shadowed'.format(pce.instance.user_arg_name))
                kwargs[user_arg_name] = self.pfs.get_curr_context().present_hfr(hfr)
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
        target = DBTarget(self, dsn, table_name, schema_name=schema_name)

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
