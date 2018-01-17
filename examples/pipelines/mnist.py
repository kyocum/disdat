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
MNIST

This is a direct adaptation of https://www.tensorflow.org/get_started/mnist/pros


"""

from disdat.pipe import PipeTask
import disdat.api as api
from tensorflow.examples.tutorials.mnist import input_data
import luigi
import pandas as pd


class GetData(PipeTask):
    """
    Get the data for MNIST
    """
    def pipe_requires(self, pipeline_input):
        """
        We have no requires but we wish to name this output bundle
        """

        self.set_bundle_name("MNIST.data")

        return

    def pipe_run(self, pipeline_input):
        """

        Returns:

        """

        mnist = input_data.read_data_sets('MNIST_data', one_hot=True)

        print "Grabbed mnist data of type {}".format(type(mnist))

        return None


class MNIST(PipeTask):
    """

    Luigi Parameters:
        fork_task (str):  Disdat task 'module.Class' string.  This task should have two parameters: input_row (json string)
            and row_index (int).

    Returns:
        A list of produced HyperFrames

    """
    make_hyperframes = luigi.BoolParameter(default=False)
    fork_task_str = luigi.Parameter(default=None)

    def pipe_requires(self, pipeline_input=None):
        """ For each row in the dataframe, create an upstream task with the row

        Args:
            pipeline_input: Input bundle data

        Returns:
            None
        """

        if not isinstance(pipeline_input, pd.DataFrame):
            print "DFFork expects DataFrame input bundle."
            return

        if self.fork_task_str is None:
            print "DFFork requires upstream tasks to fork to.  Specify '--fork_task_str <module.Class>'"
            return

        for i in range(0, len(pipeline_input.index)):
            json_row = pipeline_input[i:i + 1].to_json(orient='records')
            row_index = pipeline_input.index[i]
            self.add_dependency("output_{}".format(row_index),  self.fork_task_str,
                                {'input_row': json_row, 'input_idx': row_index})

        return

    def pipe_run(self, pipeline_input=None, **kwargs):
        """ Given a dataframe, split each row into the fork_task_str task class.

        Return a bundle of each tasks bundle.

        Args:
            pipeline_input: Input bundle data
            **kwargs: keyword args from input hyperframe and inputs to transform

        Returns:
            Array of Bundles (aka HyperFrames) from the forked tasks

        """

        # Note: There are two ways to propagate our outputs to our final output bundle

        if self.make_hyperframes:
            """ 1.) We can grab our upstream output hyperframes """
            hfrs  = self.upstream_hframes()
            return hfrs
        else:
            """ 2.) Or we can simply return our outputs """
            data = []
            for k, v in kwargs.iteritems():
                if 'output_' in k:
                    data.append(v)

            print "DFFork: Returning data {}".format(data)

            return data


if __name__ == "__main__":
    print "Using Disdat API to run the pipeline"
    api.apply('tflow', '-', '-', 'GetData')
