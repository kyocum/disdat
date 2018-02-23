``Run``: The Docker container executor
======================================

Disdat provides a ``run`` command for executing Docker containers from images
created with the ``dockerize`` command. ``run`` can launch containers on a local
Docker server, or queue remote jobs to run on the AWS Batch service.

Disdat assumes that images running in containers do not have access to persistent
local storage, and thus reads input bundles from a remote and writes output bundles
to a remote. The user should first configure a remote bundle repository with the
``remote`` command before using ``run`` to execute transforms.

::

	$ dsdt remote local_distat_context  s3://s3-bucket/path/to/remote/s3/folder
	$ dsdt run input_bundle output_bundle user.module.PipeClass

Using ``run``
-------------

Users execute transforms in Docker containers using the ``run`` command:

::

	dsdt run [-h] [--backend {Local,AWSBatch}] [--force] [--no-push-input]
	         [--use-aws-session-token AWS_SESSION_TOKEN_DURATION]
             [-it INPUT_TAG] [-ot OUTPUT_TAG]
             input_bundle output_bundle pipe_cls ...

``input_bundle`` specifies the name of the bundle in the current context to
send to the transform as input.  ``run`` will push the input bundle to
the remote before executing the transform; the user must first commit the bundle
with the ``commit`` command if it is not already committed.

``output_bundle`` specifies the name of the bundle in the current context to
receive from the transform as output. ``run`` will push the output bundle from
inside the Docker container of the transform to the remote, and then pull the
bundle back to the local context. If the bundle does not already exist, ``run``
will create a new bundle, otherwise it will create a new version of the existing
bundle.

``pipe_class`` specifies the fully-qualified class name of the transform. The
module and class name of the transform should follow standard Python naming
conventions, i.e., the transform named `module.submodule.PipeClass` should
be defined as `class PipeClass` in the file `module/submodule.py` under
`pipe_root`.

``--backend`` specifies the execution backend for the Docker containers.

- ``Local`` launches containers on a local Docker server.
- ``AWSBatch`` queues launch requests at an AWS Batch queue.

``--no-push-input`` instructs ``run`` not to push the input bundle to the
remote. The user should ensure that the remote copy of the bundle contains
the correct version of the input to use.

``--use-aws-session-token AWS_SESSION_TOKEN_DURATION`` creates and uses
temporary AWS credentials within a container running on AWS Batch to obtain
read/write access to S3 from within the container.

To push and pull bundles to and from a remote, the user needs to have
appropriate AWS credentials to access the S3 bucket holding the remote. We
assume that the user has first followed the instructions in the AWS
command-line documentation for setting up AWS credentials
and profiles required to access AWS services, as ``run`` uses the same
mechanisms as the AWS command-line tool to access S3.

Configuring AWS Batch to run Disdat images
------------------------------------------

To queue remote jobs to run Disdat images on AWS Batch, the user needs to
have access to a configured Batch compute environment and job queue. The user
should refer to the AWS Batch documentation for instruction on how to configure
Batch. Once the user has a configured Batch compute environment and job queue,
they should set the following configuration option in the Disdat configuration
file in the ``[run]`` stanza:

- ``aws_batch_queue``: The name of the AWS Batch queue to which the user will
  submit jobs. Disdat will automatically create a Batch "job definition" as
  necessary to queue jobs for a transform.

.. caution::

	As stated before, Disdat reads input bundles from a remote and writes
	output bundles to a remote when executing images in a container. The
	EC2 instances in the Batch compute environment must therefore have read/
	write access to the S3 bucket holding the remote. The easiest way to
	accomplish this is the ensure that the AWS IAM role that owns the EC2
	instances (the "Compute resources: Instance role", which by default is
	``ecsInstanceRole``) has S3 read/write access. Alternatively, the user
	can use the ``--use-aws-session-token`` to create and use temporary
	AWS credentials within the container to obtain read/write access to S3.

Once the user has configured Batch, they can queue jobs using the
``--backend AWSBatch`` option to the ``run`` command:

::

	dsdt run --backend AWSBatch input_bundle output_bundle pipe_cls

Given a transform named ``module.submodule[.submodule].PipeClass``, ``run``
will create a Batch job definition named ``disdat-module-submodule-job-definition``
that eventually launches a container from the ECR images with the suffix
``disdat-module-submodule[-submodule]:latest``, sends ``input_bundle`` to the
transform in the container as the input, and receive ``output_bundle `` at the
remote as the output.

.. note::

	Because Batch is by design a non-interactive execution backend, ``run``
	will not block to pull the output bundle back to the local context.
	Instead, the user must monitor the job status using the Batch dashboard,
	and pull the output bundle with ``dsdt pull``.
