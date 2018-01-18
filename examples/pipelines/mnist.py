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

It also takes code from
https://github.com/tensorflow/tensorflow/blob/r1.4/tensorflow/examples/how_tos/reading_data/convert_to_records.py

"""

from disdat.pipe import PipeTask
import disdat.api as api
import tensorflow as tf
from tensorflow.examples.tutorials.mnist import input_data
from tensorflow.contrib.learn.python.learn.datasets import mnist
import luigi
import pandas as pd
import os


def convert_to(data_set, name):
  """Converts a dataset to tfrecords."""
  images = data_set.images
  labels = data_set.labels
  num_examples = data_set.num_examples

  if images.shape[0] != num_examples:
    raise ValueError('Images size %d does not match label size %d.' %
                     (images.shape[0], num_examples))
  rows = images.shape[1]
  cols = images.shape[2]
  depth = images.shape[3]

  filename = os.path.join(FLAGS.directory, name + '.tfrecords')
  print('Writing', filename)
  writer = tf.python_io.TFRecordWriter(filename)
  for index in range(num_examples):
    image_raw = images[index].tostring()
    example = tf.train.Example(features=tf.train.Features(feature={
        'height': _int64_feature(rows),
        'width': _int64_feature(cols),
        'depth': _int64_feature(depth),
        'label': _int64_feature(int(labels[index])),
        'image_raw': _bytes_feature(image_raw)}))
    writer.write(example.SerializeToString())
  writer.close()




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

#        mnist = input_data.read_data_sets('MNIST_data', one_hot=True)

        data_sets = mnist.read_data_sets('MNIST_data', #FLAGS.directory,
                                         dtype=tf.uint8,
                                         reshape=False)
                                         #validation_size=FLAGS.validation_size)

        # Convert to Examples and write the result to TFRecords.
#        convert_to(data_sets.train, 'train')
#        convert_to(data_sets.validation, 'validation')
#        convert_to(data_sets.test, 'test')

        print "Grabbed mnist data of type {}".format(type(data_sets.train))
        print "Grabbed mnist data in files {}".format(data_sets.train.list_files())

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
