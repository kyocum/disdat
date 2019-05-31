``Dockerize``: The Disdat Container Compiler
============================================

Disdat provides a ``dockerize`` command for creating self-contained Docker
images of user-defined transforms. An image includes:

- A base operating system installation (e.g., Python 2.7.15 over Ubuntu or Python 3.6.8 over Ubuntu)
- A full Disdat Python environment
- Optional user-selected O/S-specific binary packages (e.g., `.deb` packages) and `pip`-managed Python packages
- The user-defined pipeline

Having created the image, the Disdat `run` command loads and executes the
transform within the image on an input bundle. The user should first install
the Docker platform before running the ``dockerize`` command.

The Dockerizer leverages Python's setuptools facilities to package user code into a source distribution or sdist.  
You should create a `setup.py` file above the Python package containing your Disdat task classes.   That `setup.py` file should contain a list of the required Python dependencies.   In addition, like any Python source distribution created by setuptools, you may include a `MANINFEST.in` file at the same level as the `setup.py` file.  As an example, let's look at the `disdat/examples/` directory.   It contains a `disdat/examples/setup.py` file that requires packages to run the pipelines in `disdat/examples/pipelines`

::

	$ dsdt dockerize path/to/users/setup.py user.module.PipeClass
	$ dsdt run path/to/users/setup.py user.module.PipeClass

Using the dockerizer to create images
-------------------------------------

Users create Docker images for their transforms using the ``dockerize`` command:

::

	dsdt dockerize [-h] [--config-dir CONFIG_DIR] [--os-type OS_TYPE]
                      [--os-version OS_VERSION] [--push] [--get-id]
                      [--sagemaker] [--no-build]
                      pipeline_root

`pipeline_root` specifies the root of the Python source tree containing the user's pipeline tasks. The root of the tree must have a `setuptools`-style `setup.py` that, at the minimum, declares any external Python packages
dependencies for the transform.

`--os-type` specifies the base operating system to install. If not specified,
``dockerize`` defaults to a Python installation over Ubuntu.

`--os-version` specifies the base O/S version to install. If not specified,
``dockerize`` defaults to Python 2.7.14-slim.

``dockerize`` will creates a Docker image with the name specified in the `setup.py` file, e.g., `name='disdat-example-pipelines` for `disdat/examples/setup.py`  Using the Docker (NOT Disdat) `run` command will
display the help message for the image entry point script.

Pushing images to AWS Elastic Container Registry
------------------------------------------------

Users can push Disdat Docker images to AWS Elastic Container Registry (ECR)
using the `--push` option to the ``dockerize`` command:

::

	dsdt dockerize --push pipeline_root

``dockerize`` will push the image to ECR.

To push images to Docker, the user needs to specify the registry prefix in
the Disdat configuration file. We assume that the user has first followed the
instructions in the AWS ECR documentation for setting up AWS credentials
and profiles required to access AWS services, as the dockerizer uses the
current AWS profile to determine the ECR URL and obtain Docker server
authentication tokens. The user then sets the following configuration options
in the Disdat configuration file in the ``[docker]`` stanza:

- `registry`: Set to `*ECR*`.
- `repository_prefix`: An optional prefix of the form `a/b/[...]` that
  ``dockerize`` will prepend to the image name.
