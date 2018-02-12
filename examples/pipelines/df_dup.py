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
import pandas as pd
import luigi

"""
DF Duplicate Example

Double the size of an input dataframe or dictionary by replicating its rows.
Note, this pipeline has no upstream dependencies.

Pre Execution:
$export PYTHONPATH=$DISDAT_HOME/disdat/examples/pipelines
$dsdt context examples; dsdt switch examples

Execution:
$python ./df_dup.py
or:
$dsdt apply - - df_dup.DFDup

"""


class DataMaker(PipeTask):
    def pipe_run(self, pipeline_input=None):
        data = pd.DataFrame({'heart_rate': [60,70,100,55], 'age':[30,44,18,77]})
        return data


class DFDup(PipeTask):
    stop = luigi.BoolParameter(default=False)

    def pipe_requires(self, pipeline_input=None):
        if pipeline_input is None:
            self.add_dependency('example_data', DataMaker, {})

    def pipe_run(self, pipeline_input=None, example_data=None):
        """
        Doubles data in a dataframe or dictionary and writes to the output

        Args:
            pipeline_input:  The user's input
            example_data:  Data if the user doesn't give us anything

        """

        if pipeline_input is None:
            pipeline_input = example_data

        if isinstance(pipeline_input, dict):
            pipeline_input.update({"{}_copy".format(k): v for k, v in pipeline_input.iteritems()})
            output = pipeline_input
        elif isinstance(pipeline_input, pd.DataFrame):
            output = pd.concat([pipeline_input, pipeline_input], axis=0)
        else:
            print "Copy Task requires an input DataFrame or an input dictionary, not {}".format(type(pipeline_input))
            output = None

        return output


if __name__ == "__main__":
    api.apply('examples', '-', '-', 'DFDup', params={})
