``Dockerize``: The Disdat Container Compiler
============================================

Disdat provides a ``dockerize`` command for creating self-contained Docker
images of user-defined transforms. An image includes:

- A base operating system installation (e.g., Python 2.7.14 over Ubuntu, or Ubuntu 16.04)
- A full Disdat Python environment
- Optional user-selected O/S-specific binary packages (e.g., `.deb` packages) and `pip`-managed Python packages
- The user-defined transform

Having created the image, the Disdat `run` command loads and executes the
transform within the image on an input bundle. The user should first install
the Docker platform before running the ``dockerize`` command.

::

	$ dsdt dockerize path/to/user/defined/transform/source/tree user.module.PipeClass
	$ dsdt run input_bundle output_bundle user.module.PipeClass

Using the dockerizer to create images
-------------------------------------

Users create Docker images for their transforms using the ``dockerize`` command:

::

	dsdt dockerize [-h] [--config-dir CONFIG_DIR] [--os-type OS_TYPE]
	               [--os-version OS_VERSION] [--push] [--no-build]
	               pipe_root pipe_cls

`pipe_root` specifies the root of the Python source tree containing the user-
defined transform. The root of the tree must have a `setuptools`-style
`setup.py` that, at the minimum, declares any external Python packages
dependencies for the transform.

`pipe_class` specifies the fully-qualified class name of the transform. The
module and class name of the transform should follow standard Python naming
conventions, i.e., the transform named `module.submodule.PipeClass` should
be defined as `class PipeClass` in the file `module/submodule.py` under
`pipe_root`.

`--os-type` specifies the base operating system to install. If not specified,
``dockerize`` defaults to a Python installation over Ubuntu.

`--os-version` specifies the base O/S version to install. If not specified,
``dockerize`` defaults to Python 2.7.14-slim.

Given a transform named `module.submodule.PipeClass`, ``dockerize`` will create
an image named `disdat-module-submodule[-submodule]`.

If successful, ``dockerize`` will create a Docker image named
`disdat-module-submodule`. Using the Docker (NOT Disdat) `run` command will
display the help message for the image entry point script.

Pushing images to AWS Elastic Container Registry
------------------------------------------------

Users can push Disdat Docker images to AWS Elastic Container Registry (ECR)
using the `--push` option to the ``dockerize`` command:

::

	dsdt dockerize --push pipe_root pipe_cls

Given a transform named ``module.submodule[.submodule].PipeClass``, ``dockerize`` will push
an image named `disdat-module-submodule[-submodule]:latest` to ECR.

To push images to Docker, the user needs to specify the registry prefix in
the Disdat configuration file. We assume that the user has first followed the
instructions in the AWS ECR documentation for setting up AWS credentials
and profiles required to access AWS services, as the dockerizer uses the
current AWS profile to determine the ECR URL and obtain Docker server
authentication tokens. The user then sets the following configuration options
in the Disdat configuration file in the ``[docker]`` stanza:

- `registry`: Set to `*ECR*`.
- `repository_prefix`: An optional prefix of the form `a/b/[...]` that
  ``dockerize`` will prepend to the image name. Given a transform named
  `module.submodule.PipeClass` and a prefix `a/b`, ``dockerize`` will push
  an image named `a/b/disdat-module-submodule[-submodule]:latest`.
