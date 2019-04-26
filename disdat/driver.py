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
import json
from collections import defaultdict, deque

import luigi
from luigi.task_register import load_task

from disdat.fs import PipeCacheEntry, DisdatFS
from disdat.pipe_base import PipeBase


class DriverTask(luigi.WrapperTask, PipeBase):
    """
    Properties:
         output_bundle: The name of the collection of resulting data items
         param_bundles: A dictionary of arguments to the first underlying Luigi Task
         pipe_cls:      The name of pipe.  It is string: <module>.<class>
         input_tags:
         output_tags:
         force:         Force recompute of dependencies (requires)
    """
    output_bundle = luigi.Parameter(default=None)
    pipe_params = luigi.Parameter(default=None)
    pipe_cls = luigi.Parameter(default=None)
    input_tags = luigi.DictParameter()
    output_tags = luigi.DictParameter()
    force = luigi.BoolParameter(default=False)
    data_context = luigi.Parameter(significant=False)
    incremental_push = luigi.BoolParameter(default=False, significant=False)
    incremental_pull = luigi.BoolParameter(default=False, significant=False)

    def __init__(self, *args, **kwargs):
        super(DriverTask, self).__init__(*args, **kwargs)

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

        The driver represents the entire pipeline.  In a sense the input bundles from the "left edge" of the pipesline
        are also "inputs."  However, this function returns the bundle_inputs used to fill lineage "depends_upon."
        And that is used to determine what to check to see if we need to re-run.

        Returns: [(bundle_name, uuid), ... ]
        """

        input_tasks = self.deps()
        input_bundles = [(task.pipe_id(), self.pfs.get_path_cache(task).uuid) for task in input_tasks]
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

        # Force root task to take an explicit bundle name?
        if self.output_bundle == '-':
            self.output_bundle = None

        task_params = {'calling_task':  self,
                       'driver_output_bundle': self.output_bundle,
                       'force': self.force,
                       'output_tags': json.dumps(dict(self.output_tags)), # Ugly re-stringifying dict
                       'data_context': self.data_context,
                       'incremental_push': self.incremental_push,
                       'incremental_pull': self.incremental_pull
                       }

        # Get user pipeline parameters for this Pipe / Luigi Task
        if self.pipe_params is not None:
            task_params.update(json.loads(self.pipe_params))

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
                for fr in next_hf.get_frames(self.data_context):
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

        for p_name, p_entry in pcache.items():  # @UnusedVariable
            all_bundles[p_entry.instance.name_output_bundle()] = p_entry

        return all_bundles


if __name__ == '__main__':
    luigi.run()
