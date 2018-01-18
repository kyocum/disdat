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
import shutil
import os

from tensorflow.contrib.learn.python.learn.datasets import base
from tensorflow.python.framework import dtypes
from tensorflow.python.framework import random_seed
from tensorflow.python.platform import gfile

# CVDF mirror of http://yann.lecun.com/exdb/mnist/
DEFAULT_SOURCE_URL = 'https://storage.googleapis.com/cvdf-datasets/mnist/'


def _int64_feature(value):
    return tf.train.Feature(int64_list=tf.train.Int64List(value=[value]))


def _bytes_feature(value):
    return tf.train.Feature(bytes_list=tf.train.BytesList(value=[value]))


def convert_to(task, data_set, name):
    """Converts a dataset to tfrecords.

    """
    images = data_set.images
    labels = data_set.labels
    num_examples = data_set.num_examples

    if images.shape[0] != num_examples:
        raise ValueError('Images size %d does not match label size %d.' %
        (images.shape[0], num_examples))
    rows = images.shape[1]
    cols = images.shape[2]
    depth = images.shape[3]

    filename = task.create_output_file(name + '.tfrecords').path
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

    return filename


def download_files(task):
    """
    Use task to download files

    Args:
        task:

    Returns:

    """
    pass


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

        data_sets = mnist.read_data_sets('MNIST_data_tmp',
                                         dtype=tf.uint8,
                                         reshape=False)

        # Convert to Examples and write the result to TFRecords.
        files = {'train': [convert_to(self, data_sets.train, 'train')]}
        files['validation'] = [convert_to(self, data_sets.validation, 'validation')]
        files['test'] = [convert_to(self, data_sets.test, 'test')]

        shutil.rmtree('MNIST_data_tmp')

        return files


class MNIST(PipeTask):
    """

    Luigi Parameters:
        fork_task (str):  Disdat task 'module.Class' string.  This task should have two parameters: input_row (json string)
            and row_index (int).

    Returns:
        A list of produced HyperFrames

    """

    def pipe_requires(self, pipeline_input=None):
        """ For each row in the dataframe, create an upstream task with the row

        Args:
            pipeline_input: Input bundle data

        Returns:
            None
        """

        self.add_dependency("input_tfrecords", GetData, {})

    def pipe_run(self, pipeline_input=None, input_tfrecords=None):
        """ Given a dataframe, split each row into the fork_task_str task class.

        Return a bundle of each tasks bundle.

        Args:
            pipeline_input: Input bundle data
            input_tfrecords: dictionary with our tfrecord files

        Returns:
            Array of Bundles (aka HyperFrames) from the forked tasks

        """

        # Note: There are two ways to propagate our outputs to our final output bundle

        for k,v in input_tfrecords.iteritems():
            print "k{} v{}".format(k,v)

        with tf.Session() as sess:
            x = tf.placeholder(tf.float32, shape=[None, 784])
            y_ = tf.placeholder(tf.float32, shape=[None, 10])

            W = tf.Variable(tf.zeros([784, 10]))
            b = tf.Variable(tf.zeros([10]))

            sess.run(tf.global_variables_initializer())

            y = tf.matmul(x, W) + b

            cross_entropy = tf.reduce_mean(
                tf.nn.softmax_cross_entropy_with_logits(labels=y_, logits=y))

            train_step = tf.train.GradientDescentOptimizer(0.5).minimize(cross_entropy)



        return None


if __name__ == "__main__":
    print "Using Disdat API to run the pipeline"
    api.apply('tflow', '-', '-', 'MNIST')
