from __future__ import print_function
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

from builtins import range
from disdat.pipe import PipeTask
import disdat.api as api
import tensorflow as tf
from tensorflow.contrib.learn.python.learn.datasets import mnist as tf_mnist
from tensorflow.contrib.learn.python.learn.datasets import base
from tensorflow.python.framework import dtypes
from tensorflow.python.platform import gfile
import os

"""
MNIST

This is a direct adaptation of https://www.tensorflow.org/get_started/mnist/pros

It also takes code from
https://github.com/tensorflow/tensorflow/blob/r1.4/tensorflow/examples/how_tos/reading_data/convert_to_records.py

This example shows how you can save TensorFlow outputs into bundles.

Pre Execution:
$export PYTHONPATH=$DISDAT_HOME/disdat/examples/pipelines
$cd ${DISDAT_HOME}/examples; pip install -e .
$dsdt context examples; dsdt switch examples

Execution:
$python ./mnist.py
or:
$dsdt apply - - mnist.Evaluate

"""


# CVDF mirror of http://yann.lecun.com/exdb/mnist/
DEFAULT_SOURCE_URL = 'https://storage.googleapis.com/cvdf-datasets/mnist/'

"""-----------------------------"""
""" TensorFlow Helper Functions """
"""-----------------------------"""


def _int64_feature(value):
    return tf.train.Feature(int64_list=tf.train.Int64List(value=[value]))


def _bytes_feature(value):
    return tf.train.Feature(bytes_list=tf.train.BytesList(value=[value]))


def dl_data_sets(train_dir, source_url=DEFAULT_SOURCE_URL):
    """ Modified version of tensorflow/tensorflow/contrib/learn/python/learn/datasets/mnist.py
    Returns:
        (dict): dataset name to local file path
    """

    if not source_url:  # empty string check
        source_url = DEFAULT_SOURCE_URL

    TRAIN_IMAGES = 'train-images-idx3-ubyte.gz'
    TRAIN_LABELS = 'train-labels-idx1-ubyte.gz'
    TEST_IMAGES = 't10k-images-idx3-ubyte.gz'
    TEST_LABELS = 't10k-labels-idx1-ubyte.gz'

    local_files = {'train-images': [base.maybe_download(TRAIN_IMAGES, train_dir, source_url + TRAIN_IMAGES)]}

    local_files['train-labels'] = [base.maybe_download(TRAIN_LABELS, train_dir,
                                   source_url + TRAIN_LABELS)]

    local_files['t10k-images'] = [base.maybe_download(TEST_IMAGES, train_dir,
                                   source_url + TEST_IMAGES)]

    local_files['t10k-labels'] = [base.maybe_download(TEST_LABELS, train_dir,
                                   source_url + TEST_LABELS)]

    return local_files


def convert_to_data_sets(data_gzs,
                         one_hot=False,
                         dtype=dtypes.float32,
                         reshape=True,
                         validation_size=5000,
                         seed=None):
    """ Modified version of tensorflow/tensorflow/contrib/learn/python/learn/datasets/mnist.py """

    with gfile.Open(data_gzs['train-images'][0], 'rb') as f:
        train_images = tf_mnist.extract_images(f)

    with gfile.Open(data_gzs['train-labels'][0], 'rb') as f:
        train_labels = tf_mnist.extract_labels(f, one_hot=one_hot)

    with gfile.Open(data_gzs['t10k-images'][0], 'rb') as f:
        test_images = tf_mnist.extract_images(f)

    with gfile.Open(data_gzs['t10k-labels'][0], 'rb') as f:
        test_labels = tf_mnist.extract_labels(f, one_hot=one_hot)

    if not 0 <= validation_size <= len(train_images):
        raise ValueError('Validation size should be between 0 and {}. Received: {}.'.format(len(train_images),
                                                                                            validation_size))

    validation_images = train_images[:validation_size]
    validation_labels = train_labels[:validation_size]
    train_images = train_images[validation_size:]
    train_labels = train_labels[validation_size:]

    options = dict(dtype=dtype, reshape=reshape, seed=seed)

    train = tf_mnist.DataSet(train_images, train_labels, **options)
    validation = tf_mnist.DataSet(validation_images, validation_labels, **options)
    test = tf_mnist.DataSet(test_images, test_labels, **options)

    return base.Datasets(train=train, validation=validation, test=test)


def make_model():
    """ Create single layer NN  -- softmax output """

    # Add to the default tf graph
    x = tf.placeholder(tf.float32, shape=[None, 784])
    y_ = tf.placeholder(tf.float32, shape=[None, 10])

    W = tf.get_variable("W", [784,10], initializer=tf.zeros_initializer)
    b = tf.get_variable("b", [10], initializer = tf.zeros_initializer)

    y = tf.matmul(x, W, name="linmatmul") + b

    cross_entropy = tf.reduce_mean(tf.nn.softmax_cross_entropy_with_logits(labels=y_, logits=y))

    train_step = tf.train.GradientDescentOptimizer(0.5).minimize(cross_entropy)

    return train_step, cross_entropy, x, y_, y


"""-----------------------------"""
"""      Disdat Pipe Tasks      """
"""-----------------------------"""


class GetDataGz(PipeTask):
    """ Pipe Task 1
    Get the data for MNIST as raw gz files
    """
    def pipe_requires(self):
        """ Simply name the output of this PipeTask """
        self.set_bundle_name("MNIST.data.gz")

    def pipe_run(self):
        """
        Returns:
            (dict): Dictionary of name, path pairs
        """
        files = dl_data_sets(self.get_output_dir())
        return files


class Train(PipeTask):
    """ Pipe Task 2
    Train the softmax layer.
    Returns:
        (dict): all the model files in 'save_files' and the name of the dir in 'save_dir'
    """

    def pipe_requires(self):
        """ Depend on the gzip files being downloaded  """
        self.add_dependency("input_gzs", GetDataGz, {})
        self.set_bundle_name("MNIST.trained")

    def pipe_run(self, input_gzs=None):
        """        """

        print("Beginning training . . . ")

        # Bring the data in as an mnist tutorial dataset
        mnist = convert_to_data_sets(input_gzs, one_hot=True)

        g = tf.Graph()
        with g.as_default():
            train_step, cross_entropy, x, y_, _ = make_model()
            saver = tf.train.Saver()

        with tf.Session(graph=g) as sess:
            sess.run(tf.global_variables_initializer())

            for _ in range(1000):
                batch = mnist.train.next_batch(100)
                train_step.run(feed_dict={x: batch[0], y_: batch[1]})

            save_dir = os.path.join(self.get_output_dir(), 'MNIST')
            saver.save(sess, os.path.join(save_dir, 'MNIST_tf_model'))

        print("End training.")

        # Note 1: When returning a dictionary, disdat requires you to use a sequence
        # in the value.

        # Note 2: You can return file paths, directories, or luigi.LocalTarget.
        # If a directory, Disdat takes all files directly under the directory.

        return {'save_files': [save_dir], 'save_dir': ['MNIST_tf_model']}


class Evaluate(PipeTask):
    """ Pipe Task 2
    Evaluate model from Train
    """
    def pipe_requires(self):
        """ """
        self.add_dependency("model", Train, {})
        self.add_dependency("input_gzs", GetDataGz, {})
        self.set_bundle_name("MNIST.eval")

    def pipe_run(self, model=None, input_gzs=None):
        """
        Args:
            pipeline_input:
            model:

        Returns:

        """

        print ("Begin evaluation . . . ")

        mnist = convert_to_data_sets(input_gzs, one_hot=True)

        g = tf.Graph()
        with g.as_default():
            train_step, cross_entropy, x, y_, y = make_model()

        with tf.Session(graph=g) as sess:
            save_dir = os.path.dirname(model['save_files'][0])
            tf.train.Saver().restore(sess, tf.train.latest_checkpoint(save_dir))
            correct_prediction = tf.equal(tf.argmax(y, 1), tf.argmax(y_, 1))
            accuracy = tf.reduce_mean(tf.cast(correct_prediction, tf.float32))
            report = accuracy.eval(feed_dict={x: mnist.test.images, y_: mnist.test.labels})
            print(report)

        print ("End evaluation.")

        return report

if __name__ == "__main__":
    print ("Using Disdat API to run the pipeline")
    api.apply('examples', 'Evaluate')
