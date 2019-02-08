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
import luigi
import logging

"""
TreeLeaves

This file contains a couple of the tasks used by simple_tree.py
See simple_tree.py for instructions on how to run.

These tasks use self.set_bundle_name to set the human output bundle name.
They also add their own tags to their bundles with self.add_tags.

"""


_logger = logging.getLogger(__name__)


class C(PipeTask):

    task_label    = luigi.Parameter(default='None')
    uuid          = luigi.Parameter(default='None')

    def pipe_requires(self):
        pass

    def pipe_run(self):
        """

        Args:
            pipeline_input: unused

        Returns:
            path(str): The path to the output file
        """

        fh = self.create_output_file('output_'.format(self.uuid))
        with fh.open('w') as outputfile:
            outputfile.write("Task C index {} uuid {} finished".format(self.task_label, self.uuid))

        self.add_tags({'SimpleTreeTask':'True', 'NumInputs':'0'})

        return fh.path


class B(PipeTask):

    task_label    = luigi.Parameter(default='None')
    uuid          = luigi.Parameter(default='None')

    def pipe_requires(self):
        for i in range(1):
            self.add_dependency("task_{}".format(i), C, {'task_label': str(i) + str(self.task_label), 'uuid': 0xdeadbeef})

    def pipe_run(self, task_0=None, task_1=None):
        """
        Args:
            pipeline_input: unused
            task_0: Output of one instance of task C
            task_1: Output of second instance of task C

        Returns:
            `luigi.Target`
        """

        fh = self.create_output_file('output_'.format(self.uuid))
        with fh.open('w') as outputfile:
            outputfile.write("Task B index {} uuid {} finished".format(self.task_label, self.uuid))

        self.add_tags({'SimpleTreeTask':'True', 'NumInputs':'2'})

        return fh.path
