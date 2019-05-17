#
# Copyright 2017 Human Longevity, Inc.
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

import numpy as np
from disdat.pipe import PipeTask
import disdat.api as api

"""
Demo Pipeline

Simple pipeline of two stages.  First stage GenData creates an array.  Second stage
Average reads array and returns an average.

This examples shows:
1.) A simple single upstream dependency
2.) How to return an ndarray
3.) Uses self.set_bundle_name(<name>) to declare the bundle name for the GenData task

Pre Execution:
$export PYTHONPATH=$DISDAT_HOME/disdat/examples/pipelines
$dsdt context examples; dsdt switch examples

Execution:
$python ./demo_pipeline.py
or:
$dsdt apply - - demo_pipeline.Average

"""


class GenData(PipeTask):
    """
    Generate a small data set of possible basketball scores
    """

    def pipe_requires(self):
        self.set_bundle_name("GenData")

    def pipe_run(self):
        return np.array([77, 100, 88])


class Average(PipeTask):
    """
    Average scores of an upstream task
    """

    def pipe_requires(self):
        """ Depend on GenData """
        self.add_dependency('my_input_data', GenData, {})

    def pipe_run(self, my_input_data=None):
        """ Compute average and return as a dictionary """
        return {'average': [np.average(my_input_data)]}


if __name__ == "__main__":
    api.apply('examples', 'Average', params={})
