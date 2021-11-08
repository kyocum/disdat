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
from types import GeneratorType

import luigi
from luigi import worker

from disdat.pipe_base import PipeBase, YIELD_PIPETASK_ARG_NAME, MISSING_EXT_DEP_UUID
from disdat.common import BUNDLE_TAG_TRANSIENT, BUNDLE_TAG_PUSH_META, ExtDepError
from disdat.hyperframe import HyperFrameRecord
from disdat import logger as _logger
from disdat.fs import DisdatFS
import disdat.api as api
from disdat.apply import different_code_versions


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
    is_root_task   = luigi.BoolParameter(default=None, significant=False)

    root_output_bundle_name = luigi.Parameter(default=None, significant=False)
    forced_output_bundle_uuid = luigi.Parameter(default=None, significant=False)

    force = luigi.BoolParameter(default=False, significant=False)
    output_tags = luigi.DictParameter(default={}, significant=False)

    # Each pipeline executes wrt a data context.
    data_context_name = luigi.Parameter(default=None, significant=False)

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

        # Luigi may copy the task by first serializing an existing task using task.to_str_params()
        # This forces string parameters set to None to 'None'
        if self.root_output_bundle_name == 'None':
            self.root_output_bundle_name = None
        if self.forced_output_bundle_uuid == 'None':
            self.forced_output_bundle_uuid = None

        self._cached_processing_id = None
        self._cached_output_bundle = None  # refers to either a new bundle or existing bundle: b.uuid and b.local_dir
        self._is_new_output = None

        # Instance variables to track various user wishes
        self.user_set_human_name = None  # self.set_bundle_name()
        self.user_tags = {}              # self.add_tags()
        self.add_deps = {}               # self.add(_external)_dependency()
        self.yield_deps = {}             # self.yield_dependency()
        self._input_tags = {}            # self.get_tags() of upstream tasks
        self._input_bundle_uuids = {}    # self.get_bundle_uuid() of upstream tasks
        self._mark_force = False         # self.mark_force()

    def input_bundles(self):
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
            b = task.output_bundle
            assert b is not None
            input_bundles[task.user_arg_name] = b
        return input_bundles

    @property
    def output_bundle(self):
        """ Resolve this task to an existing bundle or get a new bundle.

        If this is a new bundle (a bundle does not yet exist for this processing name)
        then we will create an output directory and cache a pointer to this bundle in this object.
        Otherwise cache the closed bundle with the result.

        However, in the case of yielded tasks, Luigi re-creates the task by serializing the parameters and
        deserializing them during task creation, often creating a new object.  Now we have another task
        that represents this same data (and processing name), but when the original asks for its output bundle it
        doesn't have the *same* uuid that the new task has.   Yikes!

        So we need a remote db to store the processingname -> bundle uuid binding.  It's the data context (and
        the sqlite db).  But we only push to the db on bundle write (it's either closed / on disk / in db, or nothing).
        So storing an additional bundle state would require some changes to the db.

        In lieu of that, we adopt the following slightly more expensive policy:
        * if cache empty: cache returned bundle from resolve.
        * if cached and closed: return bundle
        * if cached and open:  call resolve.  [Did an identical task write this logical bundle (by processing name)?]
        *     if new_bundle is closed:  set cached to closed bundle
        *     elif new_bundle is open: abondon bundle.
        *      if closed: return cached closed bundle.  There should be no situation where resolve returns a different bundle.

        It is possible that two tasks do the same work and write to two different bundles with the same processing name.
        This won't be incorrect, just inefficient.   Future fix would be to synchronize on the DB vis-a-vis above.
        """
        if self._cached_output_bundle is None:
            self._cached_output_bundle = self._resolve_bundle(self.pfs.get_context(self.data_context_name))
            if not self._cached_output_bundle.closed:
                self._cached_output_bundle.open(force_uuid=self.forced_output_bundle_uuid)
        else:
            if not self._cached_output_bundle.closed:
                possible_output = self._resolve_bundle(self.pfs.get_context(self.data_context_name))
                # <------- RACE ------->   between not closed and check, i.e., not thread safe.
                if possible_output.closed:
                    if possible_output.uuid == self._cached_output_bundle.uuid:
                        # Note, we cannot "clean up" the bundle if we GC one. See Bundle.abandon()
                        del self._cached_output_bundle
                    else:
                        self._cached_output_bundle.abandon()
                    self._cached_output_bundle = possible_output

        return self._cached_output_bundle

    @property
    def pipe_output(self):
        """ Return the data in the bundle presented as the Python type with which it was stored """
        return self.output_bundle.data

    @property
    def is_new_output(self):
        if self._is_new_output is None:
            self._is_new_output = not self.output_bundle.closed
        return self._is_new_output

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

        if self.root_output_bundle_name is not None:
            return self.root_output_bundle_name
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
        return self.output_bundle.uuid

    def upstream_hframes(self):
        """ Convert upstream tasks to hyperframes, return list of hyperframes

        Returns:
            (:list:`hyperframe.HyperFrameRecord`): list of upstream hyperframes
        """

        tasks = self.deps()
        hfrs = []
        for t in tasks:
            hfid = t.get_hframe_uuid()
            hfrs.append(self.pfs.get_hframe_by_uuid(hfid, data_context=self.pfs.get_context(self.data_context_name)))

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
            self._update_dependency_pipe_params(params, user_arg_name)
            tasks[user_arg_name] = pipe_class(**params)

        return tasks

    def _update_dependency_pipe_params(self, params, user_arg_name):
        """
        Given a parameter dictionary, update with PipeTask parameters
        Never called on the user's root task.  It's important to delete
        root_output_bundle_name and forced_output_bundle_uuid b/c if someone
        yields a PipeTask, Luigi doesn't take the instance, they make a new one
        and the parameter serializer replaces None with 'None' for string params.

        Args:
            params (dict): a dictionary of str: arg for a new PipeTask class
            user_arg_name (str): The user's name of the resulting task output

        Returns:
            (dict)
        """
        # we propagate the same inputs and the same output dir for every upstream task!
        params.update({
            'user_arg_name': user_arg_name,
            'is_root_task': False,
            'force': self.force,
            'output_tags': dict({}),  # do not pass output_tags up beyond root task
            'data_context_name': self.data_context_name,  # all operations wrt this context
            'incremental_push': self.incremental_push,  # propagate the choice to push incremental data.
            'incremental_pull': self.incremental_pull  # propagate the choice to incrementally pull data.
        })

        return params

    def output(self):
        """
        This is the *only* output function for all pipes.  It declares the creation of the
        one HyperFrameRecord pb and that's it.  Remember, has to be idempotent.

        Note: By checking self.output_bundle we are implicitly also looking up our output bundle.

        Return:
            (list:str):

        """
        if self.output_bundle is None:
            assert(self.uuid == MISSING_EXT_DEP_UUID)  # only reason we should be here.
            output_path = DisdatFS().get_context(self.data_context_name).get_object_dir()
            output_uuid = "this_file_should_not_exist"  # look for a file that should not exist
        else:
            output_path = self.output_bundle.local_dir
            output_uuid = self.output_bundle.uuid

        return luigi.LocalTarget(os.path.join(output_path, HyperFrameRecord.make_filename(output_uuid)))

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
        bundle = self.output_bundle
        assert(bundle is not None)

        """ NOTE: If a user changes a task param in run(), and that param parameterizes a dependency in requires(), 
        then running requires() post run() will give different tasks.  To be safe we record the inputs before run() 
        """
        add_dep_bundle_inputs = self.input_bundles()

        try:
            start = time.time()  # P3 datetime.now().timestamp()
            user_rtn_val = self.pipe_run(**kwargs)
            if isinstance(user_rtn_val, GeneratorType):
                try:
                    while True:  # while the user's task is yielding
                        task_list = next(user_rtn_val)
                        if not isinstance(task_list, list): task_list = [task_list]
                        # have we been here before?  If so, don't yield.
                        if all(task.complete() for task in task_list):
                            pass
                        else:
                            yield task_list  # yield the task or list of tasks
                except StopIteration as si:
                    user_rtn_val = si.value # we always have a return value, it is in si.value
            stop = time.time()  # P3 datetime.now().timestamp()
        except Exception as error:
            """ If user's pipe fails for any reason, remove bundle dir and raise """
            try:
                _logger.error("User pipe_run encountered exception: {}".format(error))
                bundle.abandon()
            except OSError as ose:
                _logger.error("User pipe_run encountered error, and error on remove bundle: {}".format(ose))
            raise

        try:
            # Add any output tags to the user tag dict
            if self.output_tags:
                self.user_tags.update(self.output_tags)

            # If this is the root_task, identify it as so in the tag dict
            if self.is_root_task:
                self.user_tags.update({'root_task': 'True'})

            """ if we have a pce, we have a new bundle that we need to add info to and close """
            bundle.add_data(user_rtn_val)

            bundle.add_timing(start, stop)

            bundle.add_dependencies(add_dep_bundle_inputs.values(), add_dep_bundle_inputs.keys())

            yield_output_bundles = [t.output_bundle for t in self.yield_deps.values()]
            yield_output_names = self.yield_deps.keys()
            bundle.add_dependencies(yield_output_bundles, yield_output_names)

            bundle.name = self.human_id()

            bundle.processing_name = self.processing_id()

            bundle.add_params(self._get_subcls_params())

            bundle.add_tags(self.user_tags)

            bundle.add_code_ref('{}.{}'.format(self.__class__.__module__, self.__class__.__name__))

            pipeline_path = os.path.dirname(sys.modules[self.__class__.__module__].__file__)
            cv = DisdatFS.get_pipe_version(pipeline_path)
            bundle.add_git_info(cv.url, cv.hash, cv.branch)

            bundle.close()  # Write out the bundle

            """ Incrementally push the completed bundle """
            if self.incremental_push and (BUNDLE_TAG_TRANSIENT not in bundle.tags):
                self.pfs.commit(None, None, uuid=bundle.uuid, data_context=self.pfs.get_context(self.data_context_name))
                self.pfs.push(uuid=bundle.uuid, data_context=self.pfs.get_context(self.data_context_name))

        except Exception as error:
            """ If we fail for any reason, remove bundle dir and raise """
            bundle.abandon()
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

            upstream_tasks = [(t.user_arg_name, t.output_bundle) for t in self.deps()]

            for user_arg_name, b in [u for u in upstream_tasks if u[1] is not None]:
                assert b.is_presentable

                # Download data that is not local (the linked files not present). Default when running in container.
                if self.incremental_pull:
                    b.pull(localize=True)

                if user_arg_name in kwargs:
                    _logger.warning('Task human name {} reused when naming task dependencies'.format(user_arg_name))

                self._input_tags[user_arg_name] = b.tags
                self._input_bundle_uuids[user_arg_name] = b.uuid
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

    def yield_dependency(self, pipe_class, params):
        """
        Disdat Pipe API Function

        Use this function to dynamically yield dependencies in the pipe_run function.  Access these dependency
        results by retaining a reference to the PipeTask and calling PipeTask.pipe_output.
        ``
        def pipe_run(self, some_arg=None):
            pipe = self.yield_dependency(IsFish, params=None)
            yield pipe
            if pipe.pipe_output is True:
              print("It's a fish.")
        ``

        Args:
            pipe_class (object):  Class name of upstream task if looking for external bundle by processing_id.
            params (dict):  Dictionary of parameters if looking for external bundle by processing_id.

        Returns:
            `disdat.PipeTask`
        """
        if not isinstance(params, dict):
            error = "yield_dependency: params argument must be a dictionary"
            raise Exception(error)

        assert isinstance(pipe_class, luigi.task_register.Register)

        # Note: we instantiate a new class each time to get the Luigi Task.task_id.  we don't need the
        # PipeTask.processing_name because we only need tasks that are unique to this parent task (the one yielding).
        # But we can't just use yielded task order to name them if the task yields in different orders.
        self._update_dependency_pipe_params(params, YIELD_PIPETASK_ARG_NAME)
        to_yield = pipe_class(**params)
        id = to_yield.task_id

        if id not in self.yield_deps:
            self.yield_deps[id] = to_yield
        else:
            if self.yield_deps[id] is not to_yield: del to_yield

        return self.yield_deps[id]

    def add_dependency(self, param_name, pipe_class, params):
        """
        Disdat Pipe API Function

        Add a task and its parameters to our requirements

        Args:
            param_name (str): The parameter name this bundle assumes when passed to Pipe.run
            pipe_class (object):  Class name of upstream task if looking for external bundle by processing_id.
            params (dict):  Dictionary of parameters if looking for external bundle by processing_id.

        Returns:
            None

        """
        if not isinstance(params, dict):
            error = "add_dependency third argument must be a dictionary of parameters"
            raise Exception(error)

        assert (param_name not in self.add_deps)
        self.add_deps[param_name] = (pipe_class, params)

        return

    def add_external_dependency(self, param_name, pipe_class, params, human_name=None, uuid=None):
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
            pipe_class (object):  Class name of upstream task if looking for external bundle by processing_id.
            params (dict):  Dictionary of parameters if looking for external bundle by processing_id.
            human_name (str): Resolve dependency by human_name, return the latest bundle with that humman_name.  Trumps task_class and params.
            uuid (str): Resolve dependency by explicit UUID, trumps task_class, params and human_name.

        Returns:
            `api.Bundle` or None

        """
        import disdat.api as api

        if pipe_class is not None and not isinstance(params, dict):
            error = "add_external_dependency requires parameter dictionary"
            raise Exception(error)

        assert (param_name not in self.add_deps)

        try:
            if uuid is not None:
                hfr = self.pfs.get_hframe_by_uuid(uuid, data_context=self.pfs.get_context(self.data_context_name))
            elif human_name is not None:
                hfr = self.pfs.get_latest_hframe(human_name, data_context=self.pfs.get_context(self.data_context_name))
            else:
                # we propagate the same inputs and the same output dir for every upstream task!
                params.update({
                    'user_arg_name': param_name,
                    'data_context_name': self.data_context_name
                })
                p = pipe_class(**params)
                hfr = self.pfs.get_hframe_by_proc(p.processing_id(), data_context=self.pfs.get_context(self.data_context_name))

            if hfr is None:
                error_str = "Disdat can't resolve external bundle from class[{}] params[{}] name[{}] uuid[{}]".format(pipe_class,
                                                                                                                      params,
                                                                                                                      human_name,
                                                                                                                      uuid)
                raise ExtDepError(error_str)

            bundle = api.Bundle(self.data_context_name).fill_from_hfr(hfr)

        except ExtDepError as error:  # Swallow and allow Luigi to determine task is not available.
            _logger.error(error_str)
            bundle = None

        except Exception as error:
            _logger.error(error)
            bundle = None

        finally:
            if bundle is None:
                self.add_deps[param_name] = (luigi.task.externalize(ExternalDepTask), {'uuid': MISSING_EXT_DEP_UUID,
                                                                                       'processing_name': 'None'})
            else:
                # When a task requires an external dep, this can be called multiple times.  And then
                # we return to the pipe.run which creates the class.  Note that calling task.deps() will cause
                # the requires() to be called.  But calling deps() doesn't mean that task.output() will be called
                # And that means is that tasks that have been required might not have their task.cached_output_bundle
                # set by calling resolve_bundle.  Now, in this case, because we are calling luigi.task.externalize,
                # we are actually creating *copies* of the class object and so luigi object caching isn't going to work.
                # This means that resolve_bundle must be called when using the cached_output_bundle field.
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
        output_dir = self.output_bundle.local_dir

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
        return self.output_bundle.local_dir

    def get_remote_output_dir(self):
        """
        Disdat Pipe API Function

        Retrieve the output directory for this task's bundle.  You may place
        files directly into this directory.

        Returns:
            output_dir (str):  The bundle's output directory on S3

        """
        uuid = self.output_bundle.uuid

        data_context = self.pfs.get_context(self.data_context_name)
        if data_context.remote_ctxt_url and self.incremental_push:
            output_dir = os.path.join(data_context.get_remote_object_dir(), uuid)
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

    def _resolve_bundle(self, data_context):
        """
        Instead of resolving before we run, this resolve can be issued
        from within the pipe.output() function.

        Note: Only returns None with an unfound external dependency

        Note: If returning a new bundle, we do *not* open it in this function. Bundle.open()
        creates the output directory.  Only do that if we know we're going to actually use the
        bundle in pipe.output().

        Args:
            self: the pipe to investigate
            data_context: the data context object from which we should resolve bundles.

        Returns:
            Bundle: bundle to use. If open, new bundle, if closed, re-using
        """
        verbose = False

        if verbose:
            print("resolve_bundle: looking up bundle {}".format(self.processing_id()))

        if (self._mark_force or self.force) and not isinstance(self, ExternalDepTask):
            # Forcing recomputation through a manual annotation or --force directive, unless external
            _logger.debug("resolve_bundle: pipe.mark_force forcing a new output bundle.")
            if verbose: print("resolve_bundle: pipe.mark_force forcing a new output bundle.\n")
            return api.Bundle(data_context)

        if isinstance(self, ExternalDepTask):
            # NOTE: Even if add_external_dependency() fails to find the bundle we still succeed here.
            # Thus it can look like we reuse a bundle, when in fact we don't.  We error either
            # within the user's requires, add_external_dependency(), or when Luigi can't find the task (current approach)
            assert worker._is_external(self)
            b = api.get(data_context.get_local_name(), None, uuid=str(self.uuid))
            if b is not None:
                if verbose: print("resolve_bundle: found ExternalDepTask re-using bundle with UUID[{}].\n".format(self.uuid))
            else:
                if verbose: print("resolve_bundle: No ExternalDepTask found with UUID[{}].\n".format(self.uuid))
            return b

        bndls = api.search(data_context.get_local_name(), processing_name=self.processing_id())

        if bndls is None or len(bndls) <= 0:
            if verbose: print("resolve_bundle: No bundle with proc_name {}, getting new output bundle.\n".format(self.processing_id()))
            return api.Bundle(data_context)

        bndl = bndls[0]  # our best guess is the most recent bundle with the same processing_id()

        # 2.) Bundle exists - lineage object tells us input bundles.
        lng = bndl.get_lineage()
        if lng is None:
            if verbose: print("resolve_bundle: No lineage present, getting new output bundle.\n")
            return api.Bundle(data_context)

        # 3.) Lineage record exists -- if new code, re-run
        pipeline_path = os.path.dirname(sys.modules[self.__module__].__file__)
        current_version = DisdatFS().get_pipe_version(pipeline_path)

        if different_code_versions(current_version, lng):
            if verbose: print("resolve_bundle: New code version, getting new output bundle.\n")
            return api.Bundle(data_context)

        # 3.5.) Have we changed the output human bundle name?  If so, re-run task.
        # Note: we need to go through all the bundle versions with that processing_id.
        # because, at the moment, we make new bundles when we change name.  When in some sense
        # it's just a tag set that should include other names and the data should be the same.

        current_human_name = self.human_id()
        found = False
        for bndl in bndls:
            if current_human_name == bndl.get_human_name():
                found = True
                break
        if not found:
            if verbose: print("resolve_bundle: New human name {} (prior {}), getting new output bundle.\n".format(
                current_human_name, bndl.get_human_name()))
            return api.Bundle(data_context)

        # 4.) Check the inputs -- assumes we have processed upstream tasks already
        for task in self.deps():
            """ Are we re-running an upstream input?
            Look through its *current* list of possible upstream tasks, not the ones it had
            on its prior run.   If the UUID has changed relative to lineage, then we need to re-run.
            
            In general, the only reason we should re-run an upstream is b/c of a code change.  And that change
            did not change the tasks parameters.  So it looks the same, but it is actually different.  OR someone 
            deletes and re-runs (maybe sql query result changes though parameters are the same).
            
            But if an output exists and we want to ignore code version and ignore data changes then
            while we do this, we should re-use our bundle independent of whether an upstream needs to re-run 
            or whether one of our inputs is out of date. 
            
            So one option is to ignore upstreams that need to be re-run.  Re-use blindly.  Like Luigi.  
            
            Another option is that anytime we don't have an input bundle, we attempt to read it not just
            locally, but remotely as well.               
            """
            LUIGI_RERUN = False

            if LUIGI_RERUN:
                # Ignore whether upstreams had to be re-run b/c they didn't have bundles.
                # Ignore whether this has to be re-run because existing inputs are newer
                continue

            if task.output_bundle is None:
                # this can happen with bundles created by other pipelines.
                # still surface the warning, but no longer raise exception
                _logger.info(
                    "Resolve bundles: this pipe's dep {} has no input bundle. Likely an externally produced bundle".format(
                        task.processing_id()))
            else:
                if task.is_new_output:
                    if verbose: print("Resolve_bundle: upstream task is being re-run, so rerun with new output bundle.\n")
                    return api.Bundle(data_context)

                # Upstream Task
                # Resolve to a bundle, UUID and a processing name
                # If it is an ordinary task in a workflow, we resolve via the processing name
                if worker._is_external(task) and isinstance(task, ExternalDepTask):
                    upstream_dep_uuid = task.uuid
                    upstream_dep_processing_name = task.processing_name
                else:
                    found = api.search(data_context.get_local_name(), processing_name=task.processing_id())
                    assert len(found) > 0
                    local_bundle = found[0]  # the most recent with this processing_name
                    upstream_dep_uuid = local_bundle.pb.uuid
                    upstream_dep_processing_name = local_bundle.pb.processing_name
                    assert(upstream_dep_processing_name == task.processing_id())

                """ Now we need to check if we should re-run this task because an upstream input exists and has been updated        
                Go through each of the inputs used for this current task.  
                POLICY
                1.) if the date is more recent, it is "new" data.
                2.) if it is older, we should require force (but currently do not and re-run).
                XXX TODO: Add date to the depends_on pb data structure to enforce 2 XXX
                """
                for tup in lng.pb.depends_on:
                    if tup.hframe_proc_name == upstream_dep_processing_name and tup.hframe_uuid != upstream_dep_uuid:
                        if verbose: print("Resolve_bundle: prior input bundle {} uuid {} has new uuid {}\n".format(
                            task.processing_id(),
                            tup.hframe_uuid,
                            upstream_dep_uuid))
                        return api.Bundle(data_context)

        # 5.) Woot!  Reuse the found bundle.
        if verbose: print("resolve_bundle: reusing bundle\n")
        return bndl


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

    def input_bundles(self):
        """ External bundles output are in lineage.
        Note: this is only called in apply for now.  And this task can never
        be called by apply directly.
        """
        assert False, "An ExternalDepTask should never be run directly."
