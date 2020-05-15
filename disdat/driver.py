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
from collections import deque

import luigi

from disdat.pipe_base import PipeBase


class DriverTask(luigi.WrapperTask, PipeBase):
    """
    Properties:
         output_bundle: The name of the collection of resulting data items
         param_bundles: A dictionary of arguments to the first underlying Luigi Task
         pipe_cls:      The pipe's class, type[disdat.Pipe.PipeTask]
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
                       'output_tags': self.output_tags,
                       'data_context': self.data_context,
                       'incremental_push': self.incremental_push,
                       'incremental_pull': self.incremental_pull
                       }

        # Get user pipeline parameters for this Pipe / Luigi Task
        if self.pipe_params:
            task_params.update(self.pipe_params)

        # Instantiate and return the class directly with the parameters
        # Instance caching is taken care of automatically by luigi
        return self.pipe_cls(**task_params)

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


if __name__ == '__main__':
    luigi.run()
