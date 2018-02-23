.. Disdat documentation master file, created by
   sphinx-quickstart on Sat Aug 26 23:10:40 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

This document describes core pieces of the Disdat architecture. It first
outlines the core data construct of Disdat, a bundle, and how we
organize bundles into data contexts. We then describe how Disdat
provides versioning as well as how it organizes collections of bundles
into data contexts. Disdat integrates bundle creation with Luigi, and it
changes the re-execution logic of standard Luigi pipelines. Finally, we
discuss the Disdat docker compiler.

Table Of Contents
-----------------

**`Introduction <#introduction>`__** **1**

    `Table Of Contents <#table-of-contents>`__ 1

**`Data Version Control <#data-version-control>`__** **2**

    `Working with bundles <#working-with-bundles>`__ 2

    `Data contexts <#data-contexts>`__ 2

**`Disdat API and CLI <#disdat-api-and-cli>`__** **3**

    `Bundle Creation <#bundle-creation>`__ 3

    `Bundle Management <#bundle-management>`__ 5

    `Bundle Collections <#bundle-collections>`__ 7

    `Pipeline Operations <#pipeline-operations>`__ 8

**`Bundles and HyperFrames <#bundles-and-hyperframes>`__** **9**

    `Supporting Relative Bundle
    Links <#supporting-relative-bundle-links>`__ 9

    `Supporting Polymorphic Transforms -- Command and pipeline level
    HyperFrame
    processing <#supporting-polymorphic-transforms----command-and-pipeline-level-hyperframe-processing>`__
    10

    `Task-Level Data Presentation <#task-level-data-presentation>`__ 11

**`Pipeline Re-execution Logic <#pipeline-re-execution-logic>`__**
**12**

    `Luigi Review <#luigi-review>`__ 12

    `Disdat Re-execution <#disdat-re-execution>`__ 13

**`Dockerizer: The Disdat Container Compiler <#dockerizer-the-disdat-container-compiler>`__** **13**

    `Container Calling Convention <#container-calling-convention>`__ 13

    `Autodock process <#_w1bohbz8ns5i>`__ 14

    `TL;DR <#tldr>`__ 14

    `Storage Configuration <#storage-configuration>`__ 15

Data Version Control
====================

Working with bundles
--------------------

Disdat organizes data into *bundles*, named collections of files and
literal values. For example, consider a company that runs a data
analysis pipeline on every customer. The pipeline creates a results file
in a proprietary binary format and reports individual run statistics.
The Disdat bundle consists of a user id, pipeline statistics, and output
file for each user. A bundle may be viewed or *presented* as a dataframe
-- in this example there is a row per user, a column for each literal,
and a column of paths to the results file.

In Disdat each bundle has a canonical ‘human’ name and users may tag
them with arbitrary key,value pairs. Each bundle retains information
about its time of creation, the pipeline code (repository URL and
githash), and the upstream input bundles. Each pipeline execution
produces a new version of the output bundle -- Disdat retains the last N
(default 2) versions of each bundle. This is called the *uncommitted
history*. Users *commit* bundles that they wish to retain; this tells
Disdat to retain this bundle version until the user issues an explicit
remove operation.

[integrate text] Note, the human bundle name refers to a logical entity,
of which there should be one latest. However, consider a pipeline that
re-uses TaskA N times with different params. If we give each the
human_name ‘TaskA’ Disdat will produce N versions (uncommitted) for that
human name. For simplicity the case of intermediate results, the
human_name is the processing_name. However, we allow programmers to set
the human name of their result in their code. Any scheme we develop to
automatically name the intermediate outputs either a.) requires branch
index and level index and is still hard for users to understand and b.)
may not have the same branch/level if re-used in another pipeline.

Data contexts
--------------

DisDat uses a simple model for sharing versioned bundles among users by
organizing bundles into collections called data *contexts*. Users create
local data contexts that contain their own bundles. Users can browse
bundle history, add new bundles, and remove bundles. The DisDat
pipelining system (built on Luigi) searches for existing bundles on the
local context to avoid re-processing.

However, users may also share data by creating remote contexts. Users
can associate a local context (also called a branch) with the name of a
remote context that many individuals contribute to. Users bind the
remote context name to a URL that contains the context’s data, i.e., it
is a tuple (<context name>, <s3 path>). Users may create multiple local
contexts that each attach to the same remote context.

There are three commands that take advantage of bound contexts: commit,
push, pull.

-  commit <bundle name> -- indicates that this bundle should be
       retained.

   -  unbound context -- Update local db with a commit bit

   -  bound context -- No action.

-  push <bundle name> -- publish latest committed version of this
       bundle.

   -  unbound context -- Inform user that there is no push target.

   -  bound context -- Move all data to binding site, e.g., S3. If there
          are local files, move to S3.

..

    Note: A push only guarantees that the bundle is now visible, not
    that it is the “latest” to any users later ‘pull’ request.

-  pull -- Will synchronize local db with all bundle meta data available
       at a bound context

   -  unbound context -- Inform user there is no pull source

   -  bound context -- Load HyperFrame pb’s onto local FS. If need
          frames, they may be fetched on demand from binding site.

Disdat API and CLI
==================

Disdat provides a way to create bundles, collections of files and scalar
values that may represent inputs, intermediate outputs, and final
outputs of data and machine learning pipelines.

Disdat provides commands through its command-line interface to create
bundles, create data contexts, and to manage bundles within a context.
These commands may also be used by importing a Python module that
mirrors this functionality.

Bundle Creation
---------------

-  add: Create a new bundle from a .csv, .tsv., file, or directory of
       files. Produces a bundle that can be presented as a dataframe.
       Any file links are copied-in to the Disdat local context.

-  apply: Run the given pipeline on the input bundle and produce an
       output bundle. This runs the Luigi pipeline locally.

-  run: Identical to apply except that this command runs the dockerized
       version of the pipeline. Users can choose to run the container
       locally or remotely.

**add**

usage: dsdt add [-h] [-t TAG] bundle path_name

Create a bundle from a .csv, .tsv, or a directory of files.

positional arguments:

bundle The destination bundle in the current context

path_name File or directory of files to add to the bundle

optional arguments:

-h, --help show this help message and exit

-t TAG, --tag TAG Set one or more tags: 'dsdt add -t

    authoritative:True -t version:0.7.1'

**apply**

usage: dsdt apply [-h] [-it INPUT_TAG] [-ot OUTPUT_TAG] [--local]
[--force]

input_bundle output_bundle pipe_cls ...

Apply a transform to an input bundle to produce an output bundle.

positional arguments:

input_bundle Name of source data bundle

output_bundle Name of destination bundle

pipe_cls User-defined transform, e.g.,

    module.PipeClass

params Optional set of parameters for this pipe

    '--parameter value'

optional arguments:

-h, --help show this help message and exit

-it INPUT_TAG, --input-tag INPUT_TAG

Input bundle tags: '-it authoritative:True

    -it version:0.7.1'

-ot OUTPUT_TAG, --output-tag OUTPUT_TAG

Output bundle tags: '-ot authoritative:True

    -ot version:0.7.1'

--local Run the class locally (even if dockered)

--force If there are dependencies, force

    re-computation.

**run**

usage: dsdt run [-h] [--backend {Local,AWSBatch}] [--force]
[--no-push-input]

[-it INPUT_TAG] [-ot OUTPUT_TAG]

input_bundle output_bundle pipe_cls ...

Run containerized version of transform.

positional arguments:

input_bundle Name of source data bundle

output_bundle Name of destination bundle

pipe_cls User-defined transform, e.g., module.PipeClass

pipeline_args Optional set of parameters for this pipe '--parameter

value'

optional arguments:

-h, --help show this help message and exit

--backend {Local,AWSBatch}

An optional batch execution back-end to use

--force If there are dependencies, force

    re-computation.

--no-push-input Do not push the current committed input

    bundle before execution (default is to

    push)

-it INPUT_TAG, --input-tag INPUT_TAG

Input bundle tags: '-it authoritative:True

    -it version:0.7.1'

-ot OUTPUT_TAG, --output-tag OUTPUT_TAG

Output bundle tags: '-ot authoritative:True -ot

version:0.7.1'

Bundle Management
-----------------

-  ls: List all bundles in a context.

-  rm: Remove a bundle from a context.

-  cat: Attempt to present the bundle as a dataframe.

-  commit: Retain a bundle. Otherwise the bundle will be replaced if the
       same pipeline is run.

-  push: Push a bundle to a remote.

-  pull: Pull a bundle from a remote.

**ls**

usage: dsdt ls [-h] [-pt] [-t TAG] [bundle [bundle ...]]

positional arguments:

bundle Show all bundles 'dsdt ls' or explicit bundle

    'dsdt ls <somebundle>' in current context

optional arguments:

-h, --help show this help message and exit

-pt, --print-tags Print each bundle's tags.

-t TAG, --tag TAG Having a specific tag: 'dsdt ls -t

    committed:True -t version:0.7.1'

**rm**

usage: dsdt rm [-h] [-t TAG] [--all] bundle

positional arguments:

bundle The destination bundle in the current context

optional arguments:

-h, --help show this help message and exit

-t TAG, --tag TAG Having a specific tag: 'dsdt rm -t

    committed:True -t version:0.7.1'

--all Remove the current version and all history.

    Otherwise just remove history

**cat**

usage: dsdt cat [-h] [-t TAG] bundle

positional arguments:

bundle A bundle in the current context

optional arguments:

-h, --help show this help message and exit

-t TAG, --tag TAG Having a specific tag: 'dsdt ls -t

    committed:True -t version:0.7.1'

**commit**

usage: dsdt commit [-h] [-t TAG] bundle

Commit most recent bundle of name <bundle>.

positional arguments:

bundle The name of the bundle to commit in the current context

optional arguments:

-h, --help show this help message and exit

-t TAG, --tag TAG Having a specific tag: 'dsdt rm -t

    committed:True -t version:0.7.1'

**push**

usage: dsdt push [-h] [-b BUNDLE] [-u UUID] [-t TAG]

optional arguments:

-h, --help show this help message and exit

-b BUNDLE, --bundle BUNDLE

The bundle name in the current context

-u UUID, --uuid UUID A UUID of a bundle in the current context

-t TAG, --tag TAG Having a specific tag: 'dsdt ls -t

    committed:True -t version:0.7.1'

**pull**

usage: dsdt pull [-h] [-b BUNDLE] [-u UUID] [-l]

optional arguments:

-h, --help show this help message and exit

-b BUNDLE, --bundle BUNDLE

The bundle name in the current context

-u UUID, --uuid UUID A UUID of a bundle in the current context

-l, --localize Pull files with the bundle. Default to leaving files

at remote.

Bundle Collections
------------------

-  status: Report current context and branch.

-  branch: Create a new local branch from some context.

-  checkout: Move to a different local branch.

-  remote: Attach an s3 path to this local context.

**status**

usage: dsdt status [-h] [-b BUNDLE]

optional arguments:

-h, --help show this help message and exit

-b BUNDLE, --bundle BUNDLE

A bundle in the current context

**context**

usage: dsdt context [-h] [-f] [-d] [context]

positional arguments:

context Create a new data context using <data repo>/<local

    context name> or <local context name>

optional arguments:

-h, --help show this help message and exit

-f, --force Force remove of a dirty local context

-d, --delete Delete local context

**switch**

usage: dsdt switch [-h] context

positional arguments:

context Change or checkout the context "<local context name>".

optional arguments:

-h, --help show this help message and exit

**remote**

usage: dsdt remote [-h] [-f] context s3_url

positional arguments:

context Name of the remote context

s3_url Remote context site, i.e, 's3://<bucket>/dsdt/'

optional arguments:

-h, --help show this help message and exit

-f, --force Force re-binding of remote. Executes 'dsdt pull --localize'
to

resolve files, which might take awhile.

Pipeline Operations
-------------------

-  dockerize: Create a container from this pipeline.

**dockerize**

usage: dsdt dockerize [-h] [--config-dir CONFIG_DIR] [--os-type OS_TYPE]

[--os-version OS_VERSION] [--push] [--no-build]

pipe_root pipe_cls

Dockerizer a particular transform.

positional arguments:

pipe_root Root of the Python source tree containing the

    user-defined transform; must have a

    setuptools-style setup.py file

pipe_cls User-defined transform: module.PipeTask

optional arguments:

-h, --help show this help message and exit

--config-dir CONFIG_DIR

A directory containing configuration files

    for the operating system within the Docker

    image

--os-type OS_TYPE The base operating system type for the Docker

image

--os-version OS_VERSION

The base operating system version for the Docker image

--push Push the image to a remote Docker registry

(default is to not push; must set

    'docker_registry' in Disdat config)

--no-build Do not build an image (only copy files into

    the Docker build context)

Bundles and HyperFrames
=======================

Disdat users work with *bundles* -- logical collections of related data.
Under the hood, Disdat stores bundles in *hyperframes*, a
column-oriented data structure. Hyperframes are a wrapper data object
format for storing workflow data. They can store scalars and
n-dimensional data, as well as pointers or *links* to external data
elements such as local files, database tables, or even other bundles.
Here we describe how Disdat uses hyperframes to implement bundles by
encapsulating workflow outputs (production) and presenting bundles to
user pipeline tasks through hyperframe interpretation.
