
.. figure:: ./docs/DisdatTitleFig.jpg
   :alt: Disdat Logo
   :align: center

Disdat is a Python (2.7) package for data versioning and pipeline authoring that allows data scientists to create,
share, and track data products.  Disdat organizes data into *bundles*, collections of literal values and files --
bundles are the unit at which data is versioned and shared.   Disdat pipelines automatically create bundles, making
it easy to produce and then share the latest outputs with other users.  Instead of lengthy email conversations with
multiple file attachments, searching through Slack for the most recent S3 file path, users can instead
``dsdt pull awesome_data`` to get the latest 'awesome_data.'


Getting Started
---------------

Disdat includes both an API and a CLI (command-line interface).  To install either, first clone the disdat code repository:

.. code-block:: console

    $ git clone git@github.com:kyocum/disdat.git

At this point you can either install the stand-alone version ( `Install the CLI`_) or you can install the package (`Install the API/Package`_).
The first will install a ``dsdt`` binary that can be used independent from any Python virtual environment.   The second
will install the CLI within a particular virtual environment, but also install the Disdat API so that you can manage data
and run pipelines within Python code.


Install the CLI
---------------

Build and install the single-file ``dsdt`` executable.   The deactivate command is optional if you're currently in an existing Python virtual environment.  We assume that you have installed virtualenvwrapper in your base Python environment.  See http://virtualenvwrapper.readthedocs.io/en/latest/install.html for instructions. 

.. code-block:: console

    $ deactivate
    $ cd disdat
    $ ./build-dist.sh
    $ cp dist/dsdt /usr/local/bin/.
    $ chmod u+x /usr/local/bin/dsdt

You now have a functioning ``dsdt`` excutable that you can use create and pull bundles, as well as run Disdat pipelines.


Install the API/Package
-----------------------

If you want to write Python code that works directly with bundles and pipelines, then you'll need to install the
source distribution.  Assume you are already in your virtual environment.

Install disdat

.. code-block:: console

    $ cd disdat
    $ python setup.py install


Install for developers
----------------------

Create a new virtual environment for Disdat:

.. code-block:: console

    $ mkvirtualenv disdat

Install disdat

.. code-block:: console

    $ cd disdat
    $ pip install -e .

Execute disdat:

.. code-block:: console

    $ dsdt

Tutorial
--------

We've implemented a simple TensorFlow example as a three-task Disdat pipeline in `examples/pipelines/mnist.py`.   The
README in `examples` has more information.


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
from files, directories, or csv/tsv files.   We'll add ``examples/hello_data.csv``, which contains different literals and
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
has benefited from numerous discussions, code contributions, and emotional support from Jonathon Lunt, Ted Wong,
Jason Knight, Axel Bernel, and `Intuit, Inc. <https://www.intuit.com>`_.
