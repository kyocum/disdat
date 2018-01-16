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
DF Duplicate

Simple Pipe Task example: take an input dataframe or dictionary and double in size.

author: Kenneth Yocum
"""

from disdat.pipe import PipeTask
import pandas as pd
import luigi


class DFDup(PipeTask):
    stop = luigi.BoolParameter(default=False)

    def pipe_requires(self, pipeline_input=None):
        """No upstream dependencies

        Args:
            pipeline_input: Input bundle data

        Returns:
            None
        """
        return

    def pipe_run(self, pipeline_input=None):
        """Doubles data in a dataframe or dictionary and writes to the output

        Args:
            pipeline_input:

        """

        if isinstance(pipeline_input, dict):
            pipeline_input.update({"{}_copy".format(k): v for k, v in pipeline_input.iteritems()})
            output = pipeline_input
        elif isinstance(pipeline_input, pd.DataFrame):
            output = pd.concat([pipeline_input, pipeline_input], axis=0)
        else:
            print "Copy Task requires an input DataFrame or an input dictionary, not {}".format(type(pipeline_input))
            output = None

        return output
