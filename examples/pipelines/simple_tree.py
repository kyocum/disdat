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

from disdat.pipe import PipeTask
import disdat.api as api
import luigi
import logging
from pipelines.tree_leaves import B

"""
SimpleTree

A simple tree of pipes:   Task C <- Task B <- Task A

This example illustrates a slightly more complicated chain of tasks.
In this case SimpleTree depends on B, which then depends on two C's.

See tree_leaves.py for the definitions of B and C.
B and C also add their own tags to their bundles with self.add_tags

Pre Execution:
$export PYTHONPATH=$DISDAT_HOME/disdat/examples/pipelines
$dsdt context examples; dsdt switch examples

Execution:
$python ./simple_tree.py
or:
$dsdt apply - SimpleTree.example.output simple_tree.SimpleTree

You can look at specific outputs by tag:
$ dsdt ls -iv -pt -t NumInputs:2
'-i' means print final and intermediate bundles
'-v' means print verbose
'-pt' means print tags
'-t <key>:<value>' means with these tags

"""

_logger = logging.getLogger(__name__)


class SimpleTree(PipeTask):
    """
    Create a simple binary tree of tasks.   First level is B, last level is C. 
    """
    uuid          = luigi.Parameter(default='None')

    def pipe_requires(self):
        """

        Args:
            pipeline_input:

        Returns:

        """

        for i in range(4):
            self.add_dependency('B_{}'.format(i), B, {'task_label': str(i), 'uuid': '12340000'})
        return

    def pipe_run(self, **kwargs):
        """

        Args:
            pipeline_input:
            B_0:
            B_1:

        Returns:

        """

        return "Shark Bait"


if __name__ == "__main__":
    api.apply('examples', 'SimpleTree')
