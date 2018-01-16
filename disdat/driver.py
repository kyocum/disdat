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
Driver

This is the bottom-most root of all the pipes tasks that will run.
Disdat apply creates this one task.  This will start to create all the Luigi
tasks needed to run the pipe.  The WrapperTask is complete when all requirements are met.

Depending on the kind of pipe given, change our behavior between 1:1,
1:n, and n:1.

author: Kenneth Yocum
"""
from disdat.fs import PipeCacheEntry, DisdatFS
from pipe_base import PipeBase

from luigi.task_register import load_task
from collections import defaultdict, deque
import luigi
import json
import logging

_logger = logging.getLogger(__name__)


class LiteralParam(object):
    """
    Only store literals: str, int, float, etc.  here.
    There is no checking that you're doing the right thing.
    """

    def __init__(self, thing):
        self.thing = thing

    def to_json(self):
        """
        Do *not* encode.  Convenience wrapper only.
        :return:
        """
        return self.thing  # json.dumps(self.thing)


class DriverTask(luigi.WrapperTask, PipeBase):
    """
    Properties:
         input_bundle:  The data set to be processed
         output_bundle: The name of the collection of resulting data items
         param_bundles: A dictionary of parameter:bundle_name entries for the wrapped pipe
         pipe_cls:      The name of pipe.  It is string: <module>.<class>
         input_tags:
         output_tags:
         force:         Force recompute of dependencies (requires)
    """
    input_bundle = luigi.Parameter(default=None)
    output_bundle = luigi.Parameter(default=None)
    param_bundles = luigi.Parameter(default=None)
    pipe_cls = luigi.Parameter(default=None)
    input_tags = luigi.DictParameter()
    output_tags = luigi.DictParameter()
    force = luigi.BoolParameter(default=False)

    def __init__(self, *args, **kwargs):
        """
        Initialize some variables we'll use to track state.
        Before we run anything, we need to get the latest input_bundle.
        It could theoretically be updated while we run, meaning that calls to
        requires and bundle_inputs get different bundle objects.  That is *not* kosher.

        Returns:
        """
        super(DriverTask, self).__init__(*args, **kwargs)

        if self.input_bundle != '-':  # '-' means no input bundle
            self.input_bundle_obj = self.pfs.get_latest_hframe(self.input_bundle, tags=self.input_tags)
            if self.input_bundle_obj is None:
                raise Exception("Driver unable to find input bundle {}".format(self.input_bundle))
        else:
            self.input_bundle_obj = None

    def bundle_outputs(self):
        """
        Override PipeBase.bundle_outputs

        NOTE: This really provides the set up bundles produced by upstream tasks.  NOT the bundles
        produced by this task -- which is a single bundle.

        The DriverTask defines the global name of the output of this analysis.
        This is the output_bundle parameter.

        The output bundle contains the bundles made only by the "right" edge (execute left to right) of the DAG.

        So I get the list of tasks that must run directly before the driver with requires.

        NOTE: Calls task.deps which calls task._requires which calls task.requires()
        NOTE: Unlike Pipe.bundle_outputs(), we do not return the final bundle, but the bundles it contains.

        Returns: [(bundle_name, uuid), ... ]
        """

        output_tasks = self.deps()
        output_bundles = [(task.pipe_id(), self.pfs.get_path_cache(task).uuid) for task in output_tasks]

        return output_bundles

    def bundle_inputs(self):
        """
        Override PipeBase.bundle_inputs

        NOTE: This provides lineage for the driver's output bundle.  RIGHT-EDGE POLICY says that the directly upstream
        bundles from our right-edge tasks are the ones this bundle depends on.  Alternative reality is that
        we note that this is a *pipeline* bundle and that it depends on the pipeline's input bundle and the inputs of
        the left-edge tasks.   BUT this means that we will disconnect nodes in the lineage graph.

        We have the "closure" or "parameter" input bundle from the apply command.

        The driver represents the entire pipeline.  In a sense the input bundles from the "left edge" of the pipesline
        are also "inputs."  However, this function returns the bundle_inputs used to fill lineage "depends_upon."
        And that is used to determine what to check to see if we need to re-run.

        Returns: [(bundle_name, uuid), ... ]
        """

        # LEFT-EDGE POLICY We don't want the left edge output bundles, we want their REQUIRES externalTask bundles
        # This is old code.   use the path cache, find all the left-edge-bundles.  But those should be
        # already found by the tasks at the left edge.   We re-run the driver if the input bundle has changed
        # OR the pipe above us needed to run.
        # pc = self.pfs.path_cache()
        # input_bundles = [ (task, pce.uuid) for task, pce in pc.iteritems() if pce.is_left_edge_task ]

        input_tasks = self.deps()
        input_bundles = [(task.pipe_id(), self.pfs.get_path_cache(task).uuid) for task in input_tasks]
        input_bundles.append((self.input_bundle_obj.pb.processing_name, self.input_bundle_obj.pb.uuid))

        return input_bundles

    def pipe_id(self):
        """
        The driver is a wrappertask.  It isn't a real pipe.

        Returns: processing_name

        """

        assert False

    def pipeline_id(self):
        """
        The driver is a wrappertask.  It isn't a real pipe.

        Returns:
            (str)
        """

        assert False

    @staticmethod
    def get_task_cls(mod_cls):
        """ Resolve string to task class object

        Reuse Luigi's service that registers task types

        Like Luigi, we assume that the mod_cls is 'module.class' and we assume
        that the user has put there pipe location on PYTHONPATH.

        :param mod_cls: '<module>.<class>'
        :return:        Task class object
        """

        from luigi.task_register import Register

        mod_path = mod_cls.split('.')
        mod = '.'.join(mod_path[:-1])
        cls = mod_path[-1]

        if mod is not None:
            __import__(mod)

        task_cls = Register.get_task_cls(cls)

        return task_cls

    @staticmethod
    def inflate_cls(mod_cls, params):
        """
        Reuse Luigi's service that registers task types
        so that we can dynamically instantiate an instance of this class.

        Like Luigi, we assume that the mod_cls is 'module.class' and we assume
        that the user has put there pipe location on PYTHONPATH.

        :param mod_cls: '<module>.<class>'
        :param params:  Dictionary of parameter to value
        :return:        Instance of the task in question
        """

        mod_path = mod_cls.split('.')
        mod = '.'.join(mod_path[:-1])
        cls = mod_path[-1]

        if mod == '':
            mod = None

        task = load_task(mod, cls, params)
        return task

    @staticmethod
    def df_to_json(param_dfs):
        """
        Convert a dictionary of bundle_name -> df
        into a dictionary of bundle_name -> df json strings

        :param param_dfs:
        :return: dictionary of json'd dataframes
        """
        return {k: v.to_json() for k, v in param_dfs.iteritems()}

    def prep_param_bundles(self, pfs):
        """
        For all the parameters beyond the "inputs" create dataframes.
        self.param_bundles is None or json string dictionary

        :param pfs:           pipesfs handle
        :return:              dict of parameter:bundle_df -- if none return empty dict
        """

        resolved = {}

        if self.param_bundles is None:
            return resolved

        param_bundles = json.loads(self.param_bundles)

        for p in param_bundles:
            bundle_name = param_bundles[p]
            #print "p {}".format(p)
            #print "bundle_name {}".format(bundle_name)

            #
            # TODO:  We used to automatically find strings that looked like bundle names
            # and then automatically convert them to dataframes.  However, we need a more robust
            # way to identify possible bundle arguments and we should use the Bundle presentation
            # layer to create the Python object.  So for now we just treat everything as literal values.
            # 12-17-2017 KGY
            #

            # if DisdatFS.is_input_param_bundle_name(bundle_name):
            #     ''' Get dataframe '''
            #     bundle_df = pfs.cat(bundle_name)
            #     if bundle_df is None:
            #         _logger.warn("Bundle {} does not exist".format(bundle_name))
            #         raise exceptions.BundleError("Bundle {} does not exist".format(bundle_name))
            #     else:
            #         resolved[p] = bundle_df
            # else:

            ''' Get a literal '''
            resolved[p] = LiteralParam(bundle_name)

        return resolved

    def requires(self):
        """
        The driver orchestrates the execution of a user's transform (a sequence of Luigi tasks) on an
        input hyperframe.

        Every DisDat command is logically a single execution and produces a set of outputs, thus it creates
        a single HyperFrame.

        Every DisDat command creates a HF and starts N pipelines.
        Each pipeline creates a single HF and starts M tasks.
        Each task creates a single HF and creates >0 presentables.

        Apply runs the transform on the highest-level presentable.   If this bundle contains other bundles,
        then the user's root task should deal with the contained bundles.

        """

        # Get the set of presentables for this hyperframe
        if self.input_bundle_obj is not None:
            presentables = self.get_presentables(self.input_bundle_obj, level=0)
            assert (len(presentables) == 1)
            presentable_hfr = presentables[0]
            closure_bundle_proc_name = self.input_bundle_obj.pb.processing_name
            closure_bundle_uuid = self.input_bundle_obj.pb.uuid
        else:
            presentable_hfr = None
            closure_bundle_proc_name = None
            closure_bundle_uuid = None

        # Force root task to take an explicit bundle name?
        if self.output_bundle == '-':
            self.output_bundle = None

        # Get the parameter bundles -- empty dict if none specified
        param_dfs = self.prep_param_bundles(self.pfs)
        param_dfs_json = DriverTask.df_to_json(param_dfs)

        task_params = {'closure_hframe': presentable_hfr,
                       'calling_task':  self,
                       'closure_bundle_proc_name': closure_bundle_proc_name,
                       'closure_bundle_uuid': closure_bundle_uuid,
                       'closure_bundle_proc_name_root': closure_bundle_proc_name,
                       'closure_bundle_uuid_root': closure_bundle_uuid,
                       'driver_output_bundle': self.output_bundle,
                       'force': self.force,
                       'output_tags': json.dumps(dict(self.output_tags))}  # Ugly re-stringifying dict
        task_params.update(param_dfs_json)

        t = DriverTask.inflate_cls(self.pipe_cls, task_params)

        return t

    def get_presentables(self, outer_hf, level=0):
        """
        Iterate through HF until the bottom in breadth-first fashion.

        Iterate through this hyperframe finding all presentable contained
        hyperframes.  For BFS use FIFO by removing from right (pop) and add
        on the left (appendleft).

        Args:
            outer_hf (hyperframe) : Root hyperframe to begin search.
            level (int): The depth of the search.  level=0 means just return the top most presentable.

        Returns:
            (list): list of HyperFrames that are presentable.
        """

        curr_level = 0
        hf_frontier = deque([outer_hf, ])
        presentables = []
        while len(hf_frontier) > 0:

            # Take next hyperframe
            next_hf = hf_frontier.pop()
            curr_level += 1

            # Explore hyperframes contained in the next level
            if level >= curr_level:
                for fr in next_hf.get_frames(self.pfs.get_curr_context()):
                    if fr.is_hfr_frame():
                        for hfr in fr.get_hframes():  # making a copy from the pb in this frame
                            hf_frontier.appendleft(hfr)

            # Add current-level hyperframe
            if next_hf.is_presentable():
                presentables.append(next_hf)

        return presentables

    @staticmethod
    def get_all_pipesline_output_bundles():
        """
        Find all output bundles for the pipes attached to the driver task

        The DisdatFS object has a cache of [(pipe instance, path, rerun)]

        Note: This does not include the driver's output bundle.

        :return: list of [(bundle_name, PipeCacheEntry) ... ]
        """
        all_bundles = defaultdict(PipeCacheEntry)

        pcache = DisdatFS.path_cache()

        for p_name, p_entry in pcache.iteritems():  # @UnusedVariable
            all_bundles[p_entry.instance.name_output_bundle()] = p_entry

        return all_bundles


if __name__ == '__main__':
    luigi.run()
