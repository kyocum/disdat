.. Disdat documentation master file, created by
   sphinx-quickstart on Sat Aug 26 23:10:40 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.



Examples
--------
.. .. autoclass:: examples.flagstat.Flagstat
   :members:
   :member-order: bysource


MNIST TensorFlow Tutorial
-------------------------

In this tutorial we build a pipeline of three tasks.









First CLI Example
-----------------
This example shows how to create and checkout branches, add remotes, and push and pull from s3.

1.) Build or download the binary: ``dsdt``

2.) ``cp dsdt /usr/local/bin/dsdt``

3.) ``chmod u+x /usr/local/bin/dsdt``

4.)  Install aws cli have your aws_access_key_id ,  aws_secret_acces_key, and region=us-west-2 in ~/.aws/credentials .   See:
http://docs.aws.amazon.com/cli/latest/userguide/cli-install-macos.html
and
http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html

5.) ``dsdt init``


Now you should be ready to play with disdat.    I’ve also included an example file: demo.csv

Demo commands

* Try to list the bundles you currently have:
.. code-block:: console

    $ dsdt ls

* Create a new context::
.. code-block:: console

    $ dsdt branch demo-context/demo
    $ ls ~/.disdat/context

* Change into your context
.. code-block:: console

    $ dsdt checkout demo
    $ dsdt branch

* No bundles.  Add one.   Run this command as much as you like
.. code-block:: console

    $ dsdt add demo ~/Downloads/demo.csv

* You can see the bundles in your context
.. code-block:: console

    $ dsdt ls

* And list more information, including date and whether it is committed
.. code-block:: console

    $ dsdt ls demo
    $ dsdt cat demo

* Add our S3 remote — have you set up your keys?
.. code-block:: console

    $ dsdt remote demot-context s3://vpc-0971016e-ds-shared/dsdt/dsdt_test/

* Commit and push to s3 and then look at the latest bundle locally:
.. code-block:: console

    $ dsdt commit demo
    $ dsdt push -b demo
    $ dsdt ls demo
    $ dsdt cat demo

    $ dsdt rm —all demo
    $ dsdt pull
    $ dsdt ls