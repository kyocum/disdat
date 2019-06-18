Examples
--------

This directory contains the examples package.   Contents include:

* a ``pipelines`` directory: Inside are 6 different simple example pipelines
* a ``setup.py`` file: All the requirements to run our pipelines

The ``setup.py`` file is used by ``dsdt dockerize`` to create a Docker container that contains all of the requirements for
your pipelines.  Thus, if you want to run dockerized Disdat pipelines, you should setup your project directory in this way.

Here we explore a couple of examples to get a feel for the Disdat / Luigi pipeline programming model.

Setup
=====

We assume you've installed the package into a local virtual environment following the top-level README.  Now we install
the requirements for our pipelines and setup the ``PYTHONPATH`` environment variable so Disdat can find our modules.
I assume you have cloned the disdat repository at ``DISDAT_HOME``, e.g., ``/Users/kyocum/code/disdat-repo``.

.. code-block:: console

    $ cd $DISDAT_HOME/examples
    $ pip install -e .
    $ export PYTHONPATH=$PYTHONPATH:$DISDAT_HOME/examples/pipelines

Next create an ``examples`` data context into which we'll place our data.

.. code-block:: console

    $ dsdt context example
    $ dsdt switch example


Example: MNIST
==============

We've shamelessly adapted the Tensorflow example `here <https://www.tensorflow.org/get_started/mnist/pros>`_.  Here we've
broken the example down into three steps in `mnist.py <pipelines/mnist.py>`_, which you will see as three classes:

* ``GetDataGz``: This downloads four gzip files and stores them in a bundle called ``MNIST.data.gz``

* ``Train``: This PipeTask depends on the ``GetDataGz`` tasks, gets the gzip files, builds a Tensorflow graph and trains it.  It stores the saved model into an output bundle called ``MNIST.trained``.

* ``Evaluate``: This PipeTask depends on both upstream tasks.  It rebuilds the graph, restores the values, and evaluates the model.  It returns a single accuracy float in it's output bundle ``MNIST.eval``


Dependencies
============

A Disdat PipeTask consists of two functions: ``pipe_requires`` and ``pipe_run``.   If you know Luigi, these are analagous to
``requires`` and ``run``.  Let's first describe what they do, and then we can describe how they are different from those analogs.

* ``pipe_requires``: Here you declare the tasks that must run before this task.  To do so you write statements like: ``self.add_dependency("input_gzs", GetDataGz, {})``.   This says that the current task needs a ``GetDataGz`` instance to run with no parameters.  It also says that Disdat should setup the output of that task as a named parameter to ``pipe_run`` called ``'input_gzs'``.

  Users may optionally name the output bundle in this function with ``set_bundle_name(<your name>)``.

* ``pipe_run``: Here you perform the task's main work.  This function gets a set of named parameters.
    - ``pipeline_input``:  This contains the *input* bundle data.
    - <your dependency names>:  Disdat prepares named parameters for each upstream dependency.  The variable will be they same type as the upstream task returned.


Outputs
=======

In Disdat, the ``pipe_run`` function may return scalars, lists, dictionaries, tuples, Numpy ndarrays, or Pandas
DataFrames.  Downstream tasks will be called with the same type.   Under the hood, Disdat bundles those data types,
storing them as Protocol Buffers in a managed directory (typically ``~/.disdat``). Note that, unlike Luigi, Disdat does not have an ``output`` function.

While you can store data directly into a bundle, Disdat was primarily designed to *wrap* other data formats -- not
re-invent them.   Thus tasks typically produce one or more files as output (in whatever format they choose), and
they pass the names of those files as return values.

However, Disdat manages your output paths for you -- you just need to name the files, not worry about the directory names.
To do so, you can:

* Call ``self.create_output_file("my_results.txt")``: this returns a ``luigi.LocalTarget`` object.  You can open and write to it.
* Call ``self.get_output_dir()``:  This returns a fully-qualified path to your bundle's output directory.  You can place files directly into this directory.

When you're done making files, you need to return the file paths (if you want them to be in your output bundle).  You can return:

* The ``luigi.LocalTarget``: Disdat knows how to interpet them.
* The full paths of any file: Maybe ``os.path.join(self.get_output_dir(), "my_results.txt")``
* A directory: Disdat will include the files in that sub-directory automatically.  (Currently one-level deep).

The example ``files.py`` illustrates a few different ways you can save output files.


MNIST Pipeline
==============

* In ``GetDataGz`` you'll notice that we keep track of the files we write, and we return a dictionary of name to path.
* ``Train`` consumes that dictionary and produces a TensorFlow DataSet from those gzip files.  Note that TensorFlow's ``Saver`` object just wants an output directory -- it's not very easy to get the names of those files it produced.  So we create an output sub-directory in line 190:

  .. code-block:: console

    save_dir = os.path.join(self.get_output_dir(), 'MNIST')

  And then we pass that directory as an element in our return dictionary.  Disdat will save all the files in that directory into our output bundle.

* Finally ``Evaluate`` uses the gzip files and the model saved by ``Train``.   Since TensorFlow's ``Saver`` just wants a directory, we take the dirname of the first file in ``Train``'s output in line 233.

Running the Pipeline
====================

Let's assume that have either installed Disdat into your own virtualenv, or you have ``pip install -e .`` into a Disdat
developer virtualenv.

We can now just use the Disdat.api to run MNIST (See the end of the file for the ``api.apply()`` call).

.. code-block:: console

    $ cd $DISDAT_HOME/examples/pipelines
    $ python mnist.py
    Using Disdat API to run the pipeline
    curr context name examples
    Successfully downloaded train-images-idx3-ubyte.gz 9912422 bytes.
    Successfully downloaded train-labels-idx1-ubyte.gz 28881 bytes.
    Successfully downloaded t10k-images-idx3-ubyte.gz 1648877 bytes.
    Successfully downloaded t10k-labels-idx1-ubyte.gz 4542 bytes.
    Beginning training . . .
    Extracting file:///Users/kyocum/.disdat/context/examples/objects/fcc264dc-d21b-41f3-81e2-8ee60a527f53/train-images-idx3-ubyte.gz
    Extracting file:///Users/kyocum/.disdat/context/examples/objects/fcc264dc-d21b-41f3-81e2-8ee60a527f53/train-labels-idx1-ubyte.gz
    Extracting file:///Users/kyocum/.disdat/context/examples/objects/fcc264dc-d21b-41f3-81e2-8ee60a527f53/t10k-images-idx3-ubyte.gz
    Extracting file:///Users/kyocum/.disdat/context/examples/objects/fcc264dc-d21b-41f3-81e2-8ee60a527f53/t10k-labels-idx1-ubyte.gz
    2018-01-23 01:15:50.939566: I tensorflow/core/platform/cpu_feature_guard.cc:137] Your CPU supports instructions that this TensorFlow binary was not compiled to use: SSE4.2 AVX AVX2 FMA
    End training.
    Begin evaluation . . .
    Extracting file:///Users/kyocum/.disdat/context/examples/objects/fcc264dc-d21b-41f3-81e2-8ee60a527f53/train-images-idx3-ubyte.gz
    Extracting file:///Users/kyocum/.disdat/context/examples/objects/fcc264dc-d21b-41f3-81e2-8ee60a527f53/train-labels-idx1-ubyte.gz
    Extracting file:///Users/kyocum/.disdat/context/examples/objects/fcc264dc-d21b-41f3-81e2-8ee60a527f53/t10k-images-idx3-ubyte.gz
    Extracting file:///Users/kyocum/.disdat/context/examples/objects/fcc264dc-d21b-41f3-81e2-8ee60a527f53/t10k-labels-idx1-ubyte.gz
    0.9169
    End evaluation.

Now you've produced three bundles.   By default ``dsdt ls`` only shows the final bundle, but we can use ``-i`` to list
intermediate bundles as well.   You can ``cat`` each bundle to see what's inside.  There you'll find all of our output files and
values.

.. code-block:: console

    $ dsdt ls -i
    MNIST.eval
    MNIST.data.gz
    MNIST.trained
    $ dsdt cat MNIST.eval
    unnamed:0
    0  0.9169

Finally, let's say that you're ready to share the training data, model, and results.   To do so we need to *bind* your local examples
context to an s3 path.   I'm going to assume that you have installed the AWS CLI and setup your keys in ``~/.aws/credentials``.
I'm going to assume you've made an s3 bucket ``s3://<your vpc name>/dsdt/dsdt_test/``

We are first going to bind that s3 path to your context, and then we are going to *commit* each of our output bundles.  Committing
is simply setting a flag that tells Disdat, hey, don't throw this away.

.. code-block:: console

    $ dsdt remote --force examples s3://<your vpc name>/dsdt/dsdt_test/
    $ dsdt commit MNIST.eval; dsdt commit MNIST.data.gz; dsdt commit MNIST.trained
    $ dsdt push -b MNIST.eval; dsdt push -b MNIST.data.gz; dsdt push -b MNIST.trained

Now all of your data is safely on S3.   To illustrate, let's delete our local copies and pull it back.

.. code-block:: console

    $ dsdt rm --all MNI.*
    $ dsdt pull MNIST.eval; dsdt pull MNIST.data.gz; dsdt pull MNIST.trained


If you ``dsdt cat MNIST.data.gz`` you'll notice something interesting.   Your bundle now has a bunch of s3 paths! That's because Disdat leaves your data on S3 unless you really want it locally.   To localize:

.. code-block:: console

    $ dsdt pull --localize MNIST.data.gz

Now all of your data is also local.














