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
import sys
import time
import hashlib

import luigi

from disdat.pipe_base import PipeBase
from disdat.driver import DriverTask
from disdat.common import BUNDLE_TAG_TRANSIENT, BUNDLE_TAG_PUSH_META, ExtDepError
from disdat import logger as _logger
from disdat.fs import DisdatFS
from disdat.path_cache import PathCache
import disdat.api as api


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

        self._cached_processing_id = None

        # Instance variables to track various user wishes
        self.user_set_human_name = None  # self.set_bundle_name()
        self.user_tags = {}              # self.add_tags()
        self.add_deps = {}               # self.add(_external)_dependency()
        self._input_tags = {}            # self.get_tags() of upstream tasks
        self._input_bundle_uuids = {}    # self.get_bundle_uuid() of upstream tasks
        self._mark_force = False         # self.mark_force()

    def bundle_output(self):
        """
        Return the bundle being made or re-used by this task.
        NOTE: Currently un-used.  Consider removing.
        Returns:
            (dict): <user_arg_name>:<bundle>
        """
        pce = PathCache.get_path_cache(self)
        return {self.user_arg_name: pce.b}

    def bundle_inputs(self):
        """
        Given this pipe, return the set of bundles that this task used as input.
        Return a list of tuples that contain (processing_name, uuid, arg_name)

        NOTE: Calls task.deps which calls task._requires which calls task.requires()

        Args:
            self (disdat.PipeTask):  The pipe task in question

        Returns:
            (dict(str:`disdat.api.Bundle`)):  {arg_name: bundle, ...}
        """

        input_bundles = {}
        for task in self.deps():
            if isinstance(task, ExternalDepTask):
                b = api.get(self.data_context.get_local_name(), None, uuid=task.uuid)
            else:
                b = PathCache.get_path_cache(task).bundle
            assert b is not None
            input_bundles[task.user_arg_name] = b
        return input_bundles

    def processing_id(self):
        """
        Given a pipe instance, return the "processing_name" -- a unique string based on the class name and
        the parameters.  This re-uses Luigi code for getting the unique string.

        processing_id = task_class_name + hash(task params +  [ dep0_task_name + hash(dep0_params),
                        dep1_task_name + hash(dep1_params),...])

        In typical Luigi workflows, a task's parameters uniquely determine the requires function outputs.

        Thus there is no need to include the data inputs in the uniqueness of the processing name.

        But, some facilities on top of Disdat might build dynamic classes.   In these cases, upper layer code
        might define a pipeline in python like:
        ```
        @punch.task
        def adder(a,b):
            return a+b

        def pipeline(X):
            u = adder(a=1, b=2)
            v = adder(a=u, b=X+1)
            w = adder(a=v, b=X+2)
        ```

        In this case, there is a new task for each `adder`.  Each`adder`'s requirements are defined in a scope outside
         of the dynamic classes requires method.

        NOTE: The PipeTask has a 'driver_output_bundle'.  This the name of the pipeline output bundle given by the user.
        Because this is a Luigi parameter, it is included in the Luigi self.task_id string and hash.  So we do not
        have to append this separately.

        """

        if self._cached_processing_id is not None:
            return self._cached_processing_id

        deps = self.requires()

        assert(isinstance(deps, dict))

        input_task_processing_ids = [(deps[k]).processing_id() for k in sorted(deps)]

        as_one_str = '.'.join(input_task_processing_ids)

        param_hash = hashlib.md5(as_one_str.encode('utf-8')).hexdigest()

        processing_id = self.task_id + '_' + param_hash[:luigi.task.TASK_ID_TRUNCATE_HASH]

        self._cached_processing_id = processing_id

        return processing_id

    def human_id(self):
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
            default_human_name = type(self).__name__
            return "{}".format(default_human_name)

    def get_hframe_uuid(self):
        """ Return the unique ID for this tasks current output hyperframe

        Returns:
            hframe_uuid (str): The unique identifier for this task's hyperframe
        """

        pce = PathCache.get_path_cache(self)
        assert (pce is not None)

        return pce.uuid

    def upstream_hframes(self):
        """ Convert upstream tasks to hyperframes, return list of hyperframes

        Returns:
            (:list:`hyperframe.HyperFrameRecord`): list of upstream hyperframes
        """

        tasks = self.deps()
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

        Args:

        Returns:
            (dict): arg_name: task_class
        """

        kwargs = self.prepare_pipe_kwargs()

        self.add_deps.clear()
        self.pipe_requires(**kwargs)
        rslt = self.add_deps

        if len(self.add_deps) == 0:
            return {}

        tasks = {}

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
            tasks[user_arg_name] = pipe_class(**params)

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
            None
        """
        kwargs = self.prepare_pipe_kwargs(for_run=True)
        pce = PathCache.get_path_cache(self)
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
                pce.bundle.abandon()
            except OSError as ose:
                _logger.error("User pipe_run encountered error, and error on remove bundle: {}".format(ose))
            raise

        try:
            # Add any output tags to the user tag dict
            if self.output_tags:
                self.user_tags.update(self.output_tags)

            # If this is the root_task, identify it as so in the tag dict
            if isinstance(self.calling_task, DriverTask):
                self.user_tags.update({'root_task': 'True'})

            """ if we have a pce, we have a new bundle that we need to add info to and close """
            pce.bundle.add_data(user_rtn_val)

            pce.bundle.add_timing(start, stop)

            pce.bundle.add_dependencies(cached_bundle_inputs.values(), cached_bundle_inputs.keys())

            pce.bundle.name = self.human_id()

            pce.bundle.processing_name = self.processing_id()

            pce.bundle.add_params(self._get_subcls_params())

            pce.bundle.add_tags(self.user_tags)

            pce.bundle.add_code_ref('{}.{}'.format(self.__class__.__module__, self.__class__.__name__))

            pipeline_path = os.path.dirname(sys.modules[self.__class__.__module__].__file__)
            cv = DisdatFS.get_pipe_version(pipeline_path)
            pce.bundle.add_git_info(cv.url, cv.hash, cv.branch)

            pce.bundle.close()  # Write out the bundle

            """ Incrementally push the completed bundle """
            if self.incremental_push and (BUNDLE_TAG_TRANSIENT not in pce.bundle.tags):
                self.pfs.commit(None, None, uuid=pce.bundle.uuid, data_context=self.data_context)
                self.pfs.push(uuid=pce.uuid, data_context=self.data_context)

        except Exception as error:
            """ If we fail for any reason, remove bundle dir and raise """
            pce.bundle.abandon()
            raise

        return None

    def _get_subcls_params(self):
        """ Given the child class, extract user defined Luigi parameters

        This function uses vars(cls) and filters by Luigi Parameter
        types.  Luigi get_params() gives us all parameters in the full class hierarchy.
        It would give us the parameters in this class as well.  And then we'd have to do set difference.
        See luigi.Task.get_params()

        Args:
            self: The instance of the subclass.  To get the normalized values for the Luigi Parameters
        Returns:
            dict: {<name>:'string value',...}
        """
        cls = self.__class__
        params = {}
        for param in vars(cls):
            attribute = getattr(cls, param)
            if isinstance(attribute, luigi.Parameter):
                params["{}".format(param)] = attribute.serialize(getattr(self, param))
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
        deser_params = {}
        for param, ser_value in ser_params.items():
            try:
                attribute = getattr(cls, param)
                assert isinstance(attribute, luigi.Parameter)
                deser_params[param] = attribute.parse(ser_value)
            except Exception as e:
                _logger.warning("Bundle parameter ({}:{}) can't be deserialized by class({}): {}".format(param,
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
        if for_run:

            # Reset the stored tags, in case this instance is run multiple times.
            self._input_tags = {}
            self._input_bundle_uuids = {}

            upstream_tasks = [(t.user_arg_name, PathCache.get_path_cache(t)) for t in self.deps()]
            for user_arg_name, pce in [u for u in upstream_tasks if u[1] is not None]:

                b = api.get(self.data_context.get_local_name(), None, uuid=pce.uuid)
                assert b.is_presentable

                # Download data that is not local (the linked files are not present).
                # This is the default behavior when running in a container.
                if self.incremental_pull:
                    b.pull(localize=True)

                if pce.instance.user_arg_name in kwargs:
                    _logger.warning('Task human name {} reused when naming task dependencies: Dependency hyperframe shadowed'.format(pce.instance.user_arg_name))

                self._input_tags[user_arg_name] = b.tags
                self._input_bundle_uuids[user_arg_name] = pce.uuid
                kwargs[user_arg_name] = b.data

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

        Create ersatz ExternalDepTask parameterized by uuid and processing_name
        Note: it is possible to use class/params when searching by class, params, but this makes all external
        dependencies look the same in the code.  Win.

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

        TODO: Consider pushing caching into this layer.

        Args:
            param_name (str): The parameter name this bundle assumes when passed to Pipe.run
            task_class (object):  Class name of upstream task if looking for external bundle by processing_id.
            params (dict):  Dictionary of parameters if looking for external bundle by processing_id.
            human_name (str): Resolve dependency by human_name, return the latest bundle with that humman_name.  Trumps task_class and params.
            uuid (str): Resolve dependency by explicit UUID, trumps task_class, params and human_name.

        Returns:
            `api.Bundle` or None

        """
        import disdat.api as api

        if task_class is not None and not isinstance(params, dict):
            error = "add_external_dependency requires parameter dictionary"
            raise Exception(error)

        assert (param_name not in self.add_deps)

        try:
            if uuid is not None:
                hfr = self.pfs.get_hframe_by_uuid(uuid, data_context=self.data_context)
            elif human_name is not None:
                hfr = self.pfs.get_latest_hframe(human_name, data_context=self.data_context)
            else:
                # we propagate the same inputs and the same output dir for every upstream task!
                params.update({
                    'user_arg_name': param_name,
                    'data_context': self.data_context
                })
                p = task_class(**params)
                hfr = self.pfs.get_hframe_by_proc(p.processing_id(), data_context=self.data_context)

            if hfr is None:
                error_str = "Disdat can't resolve external bundle from class[{}] params[{}] name[{}] uuid[{}]".format(task_class,
                                                                                                                   params,
                                                                                                                   human_name,
                                                                                                                   uuid)
                raise ExtDepError(error_str)

            bundle = api.Bundle(self.data_context.get_local_name()).fill_from_hfr(hfr)

        except ExtDepError as error:  # Swallow and allow Luigi to determine task is not available.
            _logger.error(error_str)
            bundle = None

        except Exception as error:
            _logger.error(error)
            bundle = None

        finally:
            if bundle is None:
                self.add_deps[param_name] = (luigi.task.externalize(ExternalDepTask), {'uuid': 'None',
                                                                                       'processing_name': 'None'})
            else:
                self.add_deps[param_name] = (luigi.task.externalize(ExternalDepTask), {'uuid': bundle.uuid,
                                                                                       'processing_name': bundle.processing_name})

        return bundle

    def create_output_file(self, filename):
        """
        Disdat Pipe API Function

        Pass in the name of your file, and get back a Luigi target object to which you can write.

        Args:
            filename (str, dict, list): A basename, dictionary of basenames, or list of basenames.

        Returns:
            (`luigi.LocalTarget`): Singleton, list, or dictionary of Luigi Target objects.
        """

        pce = PathCache.get_path_cache(self)
        assert (pce is not None)
        output_dir = pce.path
        return self.filename_to_luigi_targets(output_dir, filename)

    def create_remote_output_file(self, filename):
        """
        Disdat Pipe API Function

        Pass in the name of your file, and get back an object to which you can write on S3.

        NOTE: Managed S3 paths are created only if a) remote is set (otherwise where would we put them?)
        and b) incremental_push flag is True  (if we don't push bundle metadata, then the locations may be lost).

        Args:
            filename (str, dict, list): A basename, dictionary of basenames, or list of basenames.

        Returns:
            (`luigi.contrib.s3.S3Target`): Singleton, list, or dictionary of Luigi Target objects.

        """
        pce = PathCache.get_path_cache(self)
        assert (pce is not None)
        output_dir = self.get_remote_output_dir()
        return self.filename_to_luigi_targets(output_dir, filename)

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

    def create_remote_output_dir(self, dirname):
        """
        Disdat Pipe API Function

        Given basename directory name, return a fully qualified path whose prefix is the
        remote output directory for this bundle in the current context.

        NOTE: The current context must have a remote attached.

        Args:
            dirname (str): The name of the output directory, i.e., "models"

        Returns:
            output_dir (str):  Fully qualified path of a directory whose prefix is the bundle's remote output directory.

        """
        prefix_dir = self.get_remote_output_dir()
        fqp = os.path.join(prefix_dir, dirname)
        return fqp

    def get_output_dir(self):
        """
        Disdat Pipe API Function

        Retrieve the output directory for this task's bundle.  You may place
        files directly into this directory.

        Returns:
            output_dir (str):  The bundle's output directory

        """
        pce = PathCache.get_path_cache(self)
        assert(pce is not None)
        return pce.path

    def get_remote_output_dir(self):
        """
        Disdat Pipe API Function

        Retrieve the output directory for this task's bundle.  You may place
        files directly into this directory.

        Returns:
            output_dir (str):  The bundle's output directory on S3

        """
        pce = PathCache.get_path_cache(self)
        assert(pce is not None)
        if self.data_context.remote_ctxt_url and self.incremental_push:
            output_dir = os.path.join(self.data_context.get_remote_object_dir(), pce.uuid)
        else:
            raise Exception('Managed S3 path creation needs a) remote context and b) incremental push to be set')
        return output_dir

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

    def mark_transient(self, push_meta=True):
        """
        Disdat Pipe API Function

        Mark output bundle as transient.   This means that during execution Disdat will not
        write (push) this bundle back to the remote.  That only happens in two cases:
        1.) Started the pipeline with incremental_push=True
        2.) Running the pipeline in a container with no_push or no_push_intermediates False

        We mark the bundle with a tag.   Incremental push investigates the tag before pushing.
        And the entrypoint investigates the tag if we are not pushing incrementally.
        Otherwise, normal push commands from the CLI or api will work, i.e., manual pushes continue to work.

        Args:
            push_meta (bool): Push the meta-data but not the data. Else, push nothing.

        Returns:
            None
        """
        if push_meta:
            self.add_tags({BUNDLE_TAG_TRANSIENT: 'True', BUNDLE_TAG_PUSH_META: 'True'})
        else:
            self.add_tags({BUNDLE_TAG_TRANSIENT: 'True'})


class ExternalDepTask(PipeTask):
    """ This task is only here as a shell.
    If the user specifies an external dependency, we look it up in add_external_dependency.
    We look it up, b/c we want to hand them the bundle in requires.
    If they look up the bundle via UUID or human name, there is no reason for them to
    pass in the class.  Especially for human name, where they cannot know it.
    And, if it exists, there is no reason to look it up again in apply.resolve_bundle().
    Thus we create an ExternalDepTask() parameterized by the UUID and apply.resolve_bundle()
    The default output() function will create this tasks processing_id() which will be a hash
    of this task's params, which will include a unique UUID.   And so should be unique.
    """
    uuid = luigi.Parameter(default=None)
    processing_name = luigi.Parameter(default=None)

    def bundle_output(self):
        """ External bundles have no outputs
        """
        return None

    def bundle_inputs(self):
        """ External bundles output are in lineage.
        Note: this is only called in apply for now.  And this task can never
        be called by apply directly.
        """
        assert False, "An ExternalDepTask should never be run directly."
