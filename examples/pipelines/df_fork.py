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
from df_dup import DataMaker
import luigi
import pandas as pd

"""
Fork Pipe

Take an input DataFrame.  For each row, convert to a json string, and send to an upstream
task specified by the string fork_task_str ('module.Class').

This examples shows:
1.) How the input bundle shows up automatically as the parameter "pipeline_input" in upstream tasks.
2.) How you can pass arguments when calling apply
3.) And how an argument may be a string indicating an upstream task: '__main__.PrintRow'
4.) How you may use **kwargs to find the dynamic set of inputs

Pre Execution:
$export PYTHONPATH=$DISDAT_HOME/disdat/examples/pipelines
$dsdt context examples; dsdt switch examples
$dsdt apply - DataMaker df_dup.DataMaker

Execution:
$python ./df_fork.py
or:
$dsdt apply DataMaker - df_fork.DFFork --fork_task df_fork.PrintRow

"""


class PrintRow(PipeTask):
    """ Given an iloc, pretty print row in dataframe """

    iloc = luigi.IntParameter(default=0)
    loc  = luigi.IntParameter(default=0)

    def pipe_run(self, pipeline_input=None):
        print "PrintRow[{}]: {}".format(self.iloc, pipeline_input.iloc(self.iloc))
        return True


class DFFork(PipeTask):
    """ Given a dataframe, fork a task for each presentable

    Luigi Parameters:
        fork_task (str):  Disdat task 'module.Class' string.  This task should have two parameters: input_row (json string)
            and row_index (int).

    Returns:
        A list of produced HyperFrames

    """
    fork_task = luigi.Parameter(default=None)

    def pipe_requires(self, pipeline_input=None):
        """ For each row in the dataframe, create an upstream task with the row

        Here we simply hand an index to each upstream, and they pull it out of
        the input dataframe present in 'pipeline_input'.   The input bundle gets handed
        to all the tasks in the workflow automatically.

        Args:
            pipeline_input: Input bundle data

        Returns:
            None
        """

        if not isinstance(pipeline_input, pd.DataFrame):
            print "DFFork expects DataFrame input bundle."
            return

        if pipeline_input is None:
            print "DFFork requires an input bundle"
            return

        if self.fork_task is None:
            print "DFFork requires upstream tasks to fork to.  Specify '--fork_task <module.Class>'"
            return

        for i in range(0, len(pipeline_input.index)):
            iloc   = i
            loc = pipeline_input.index[i]
            self.add_dependency("task_{}".format(iloc), self.fork_task,
                                {'iloc': iloc, 'loc': loc})

        return

    def pipe_run(self, pipeline_input=None, **kwargs):
        """ Given a dataframe, split each row into the fork_task_str task class.

        The body of DFFork simply prints out the return value of the upstream
        tasks.

        Args:
            pipeline_input: Input bundle data
            **kwargs: keyword args from input hyperframe and inputs to transform

        Returns:
            Array of Bundles (aka HyperFrames) from the forked tasks

        """

        for k, v in kwargs.iteritems():
            if 'task_' in k:
                print "DFFork finished[{}] with output[{}]".format(k, v)


if __name__ == "__main__":
    api.apply('examples', 'DataMaker', '-', 'DFFork', params={'fork_task': '__main__.PrintRow'})
