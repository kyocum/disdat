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
Copy Pipe

Take an input HyperFrame (aka, bundle), and output an identical HyperFrame

author: Kenneth Yocum
"""

from disdat.pipe import PipeTask
import luigi
import json


class RowPrint(PipeTask):
    """ Expect a json string in input

    Luigi Parameters:
        input_row (str): json string

    """
    input_row = luigi.Parameter(default=None)
    input_idx = luigi.IntParameter(default=None)

    def pipe_requires(self, pipeline_input=None):
        """ No new tasks
        Returns:
            None
        """
        self.set_bundle_name("RowPrintTest")
        return None

    def pipe_run(self, pipeline_input=None):
        """ Print out input json string, output string.

        Args:
            pipeline_input: Input bundle data

        Returns:
            json (str): output json string as a row of data

        """

        print "RowPrint: json loads: {}".format(json.loads(self.input_row)[0])

        return json.loads(self.input_row)[0]
    