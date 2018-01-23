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
"""
SimpleTree

A simple tree of pipes:   Task C <- Task B <- Task A

author: Kenneth Yocum
"""

from disdat.pipe import PipeTask, PipesExternalBundle
import luigi
import logging

_logger = logging.getLogger(__name__)


class C(PipeTask):
    """
    Adding a comment
    """
    task_label    = luigi.Parameter(default='None')
    uuid          = luigi.Parameter(default='None')

    def pipe_requires(self, pipeline_input=None):
        """

        Args:
            pipeline_input:

        Returns:

        """
        return None

    def pipe_run(self, pipeline_input=None):
        """

        Args:
            pipeline_input:

        Returns:
            `luigi.Target`
        """

        fh = self.create_output_file('output_'.format(self.uuid))
        with fh.open('w') as outputfile:
            outputfile.write("Task C index {} uuid {} finished".format(self.task_label, self.uuid))

        self.set_bundle_name("TaskC_label{}_uuid{}".format(self.task_label, self.uuid))
        self.add_tags({'TopLevelTask':'True','NeededBy':'TaskB'})

        return fh.path


class B(PipeTask):
    """
    """
    task_label    = luigi.Parameter(default='None')
    uuid          = luigi.Parameter(default='None')

    def pipe_requires(self, pipeline_input=None):
        """
        Args:
            pipeline_input:

        Return:
            (:tuple) tasks (dict), params (dict:)
        """

        for i in range(2):
            self.add_dependency("task_{}".format(i), C, {'task_label': str(i) + str(self.task_label), 'uuid': 0xdeadbeef})

        return

    def pipe_run(self, pipeline_input=None, task_0=None, task_1=None):
        """
        Args:
            pipeline_input:
            task_0:
            task_1:

        Returns:
            `luigi.Target`
        """

        fh = self.create_output_file('output_'.format(self.uuid))
        with fh.open('w') as outputfile:
            outputfile.write("Task B index {} uuid {} finished".format(self.task_label, self.uuid))

        self.set_bundle_name("TaskB_label{}_uuid{}".format(self.task_label, self.uuid))
        self.add_tags({'TopLevelTask':'False','NeededBy':'SimpleTree'})

        return fh.path
