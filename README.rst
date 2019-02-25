
.. figure:: ./docs/DisdatTitleFig.jpg
   :alt: Disdat Logo
   :align: center

Disdat is a Python (2.7 / 3.6) package for data versioning and pipeline authoring that allows data scientists to create,
share, and track data products.  Disdat organizes data into *bundles*, collections of literal values and files --
bundles are the unit at which data is versioned and shared.   Disdat pipelines automatically create bundles, making
it easy to produce and then share the latest outputs with other users.  Instead of lengthy email conversations with
multiple file attachments, searching through Slack for the most recent S3 file path, users can instead
``dsdt pull awesome_data`` to get the latest 'awesome_data.'

In addition, Disdat can 'dockerize' pipelines into containers that run locally, on AWS Batch, or on AWS SageMaker.   Whether running as a container or running the pipeline natively, Disdat manages the data produced by your pipeline so you don't have to.  Instead of having to find new names for the same logical dataset, e.g., "the-most-recent-today-with-the-latest-fix.csv" , Disdat manages the outputs in your local FS or S3 for you.  


Getting Started
---------------

Disdat includes both an API and a CLI (command-line interface).  To install both into your Python environment:

.. code-block:: console
    
    $ pip install disdat

At this point you can start using Disdat to author and share pipelines and data sets.  Check that you're running at least this version:

.. code-block:: console

    $ dsdt --version
    Running Disdat version 0.7.1rc0

Install for developers
----------------------

Clone the disdat code repository and create a virtual environment in which to develop:

.. code-block:: console

    $ git clone git@github.com:kyocum/disdat.git

Install disdat

.. code-block:: console

    $ mkvirtualenv disdat
    $ cd disdat
    $ pip install -e .

Execute disdat:

.. code-block:: console

    $ dsdt -h

Tutorial
--------

We've implemented a simple TensorFlow example as a `three-task Disdat pipeline <examples/pipelines/mnist.py>`_.   The
directory `examples <examples>`_ has more information.

Short Test Drive
----------------

First, Disdat needs to setup some minimal configuration.   Second, create a data *context* in which to store the data
you will track and share.  Finally, switch into that context.   The commands ``dsdt context`` and ``dsdt switch`` are kind of like
``git branch`` and ``git checkout``.  However, we use different terms because contexts don't always behave like code repositories.

.. code-block:: console

    $ dsdt init
    $ dsdt context mycontext
    $ dsdt switch mycontext

Now let's add some data.  Disdat wraps up collections of literals and files into a *bundle*.   You can make bundles
from files, directories, or csv/tsv files.   We'll add `hello_data.csv <examples/hello_data.csv>`_, which contains different literals and
some publicly available files on s3.

.. code-block:: console

    $ dsdt add my.first.bundle examples/hello_data.csv
    $ dsdt ls -v
    NAME                	PROC_NAME           	OWNER     	DATE              	COMMITTED 	TAGS
    my.first.bundle     	AddTask_examples_hel	kyocum    	01-16-18 07:17:37 	False
    $ dsdt cat my.first.bundle
                                                                                                                                                    s3paths  someints  somefloats  bool     somestr
    0  file:///Users/kyocum/.disdat/context/mycontext/objects/43b153db-14a2-45f4-91b0-a0280525c588/LC08_L1TP_233248_20170525_20170614_01_T1_thumb_large.jpg  7        -0.446733    True  dagxmyptkh
    1  file:///Users/kyocum/.disdat/context/mycontext/objects/43b153db-14a2-45f4-91b0-a0280525c588/LC08_L1TP_233248_20170525_20170614_01_T1_MTL.txt          8         0.115150    True  uwvmcmbjpg


Great!  You've created your first data context and bundle.  In the tutorial we'll look at how you can use a bundle as an input to a pipeline, and how you can push/pull your bundles to/from AWS S3 to share data with colleagues.

Other Documentation
-------------------

For an overview of bundles, contexts, the CLI, and pipelines please look at this overview of the
`Disdat architecture <https://docs.google.com/document/d/1Egw0KoEF6-L-dPK5nSKqMJXQwAVrAmpY2lkLNar6MY4/edit?usp=sharing>`_.


Background
----------

Disdat provides an ecosystem for data creation, versioning, and sharing.  Data scientists create a variety of data
artifacts: model features, trained models, and predictions. Effective data science teams must share data to use it as
inputs into other pipelines.  Today data scientists share data by sending spreadsheets on email, sharing
thumbdrives, or emailing AWS S3 links. Maintaining these loose ad-hoc data collections quickly becomes difficult
-- data is lost, remade, or consumed without knowing how it was made.   Shared storage systems, such as S3, often
become polluted with data that is hard to discard.

At its core Disdat provides an API for creating and publishing sets of data files and scalars -- a Disdat bundle.
Disdat instruments an existing pipelining system (Spotify's `Luigi <https://luigi.readthedocs.io/en/stable/>`_) with this API
to enable pipelines to automatically create versioned data sets.  Disdat pipelines maintain coarse-grain lineage for
every processing step, allowing users to determine the input data and code used to produce each data set.  The Disdat
CLI allows users to share datasets with one another, allowing other team members to download the most recent version of features and models.

Disdat's bundle API and pipelines provide:

* **Simplified pipelines** -- Users implement two functions per task: `requires` and `run`.

* **Enhanced re-execution logic** -- Disdat re-runs processing steps when code or data changes.

* **Data versioning/lineage** -- Disdat records code and data versions for each output data set.

* **Share data sets** -- Users may push and pull data to remote contexts hosted in AWS S3.

* **Auto-docking** -- Disdat *dockerizes* pipelines so that they can run locally or execute on the cloud.

Authors
-------

Disdat could not have come to be without the support of `Human Longevity, Inc. <https://www.humanlongevity.com>`_  It
has benefited from numerous discussions, code contributions, and emotional support from Sean Rowan, Ted Wong, Jonathon Lunt, 
Jason Knight, Axel Bernel, and `Intuit, Inc. <https://www.intuit.com>`_.
