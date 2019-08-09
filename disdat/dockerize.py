#
# Copyright 2015, 2016, 2017  Human Longevity, Inc.
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
from __future__ import print_function

# Built-in imports
import inspect
import logging
import os
import tempfile
import shutil

# Third-party imports
import disdat.common
import disdat.resources
import disdat.utility.aws_s3 as aws
from disdat.infrastructure import dockerizer
from disdat.fs import determine_pipe_version

import docker

_MODULE_NAME = inspect.getmodulename(__file__)

_DOCKERIZER_ROOT = os.path.dirname(inspect.getsourcefile(dockerizer))

_logger = logging.getLogger(__name__)


def _copy_in_dot_file(disdat_config, docker_context, dot_file_name, option_name, cli):
    """
    Copy in a dot file to a file with name "dot_file_name" into the docker context.
    The user might have put the src path in the disdat config file under the "option_name"

    Args:
        disdat_config:  A disdat config object
        docker_context:  The place we are storing the docker context for build
        dot_file_name:  The name of the file in the docker context
        option_name: the name of the option in disdat.cfg
        cli (bool): whether we are called from cli

    Returns:
        None or if subprocess has output

    """
    retval = 0

    dot_file_path = os.path.join(docker_context, dot_file_name)
    if disdat_config.parser.has_option(_MODULE_NAME, option_name):
        dot_file = os.path.expanduser(disdat_config.parser.get(_MODULE_NAME, option_name))
        shutil.copy(dot_file, dot_file_path)
        print("Copying dot file {} into {}".format(dot_file, docker_context))
    else:
        touch_command = [
            'touch',
            dot_file_path
        ]
        retval = disdat.common.do_subprocess(touch_command, cli)

    return retval


def latest_container_id(pipeline_root, cli):
    """ Return the unique container image hash from the latest container made
    from this setup.py

    Args:
        pipeline_root (str): The path to the setup.py file that defines the container
        cli (bool): if called from cli

    Returns:
        (str): The full docker container image hash

    """
    setup_file = os.path.join(pipeline_root, 'setup.py')
    pipeline_image_name = disdat.common.make_project_image_name(setup_file)
    docker_client = docker.from_env()

    try:
        img_obj = docker_client.images.get(pipeline_image_name)
    except (docker.errors.APIError, docker.errors.ImageNotFound) as er:
        if cli:
            print("Disdat unable to find image with project name {}".format(pipeline_image_name))
            raise # exit with code > 0
        return None

    id = img_obj.id.replace('sha256:','')

    if cli:
        print("{}".format(id))

    return id


def dockerize(pipeline_root,
              config_dir=None,
              os_type=None,
              os_version=None,
              build=True,
              push=False,
              sagemaker=False,
              cli=False
              ):
    """ Create a Docker image for running a pipeline.

    Args:
        pipeline_root: Root of the Python source tree containing the
        setuptools-style setup.py file.
        config_dir (str):  Configuration of image (.deb, requires.txt, etc.)
        os_type (str): OS type string
        os_version (str): Version of OS
        build (bool): Build the image (default True)
        push (bool): Push to registry listed in Disdat config file
        sagemaker (bool): Build a container for 'train' or 'serve' in SageMaker
        cli (bool): Whether dockerize was called from the CLI (True) or an API (False -- default)

    Returns:
        (int): 0 equals success, >0 for error

    """

    disdat_config = disdat.common.DisdatConfig.instance()

    # Get configuration parameters
    image_os_type = os_type if os_type is not None else disdat_config.parser.get(_MODULE_NAME, 'os_type')
    image_os_version = os_version if os_version is not None else disdat_config.parser.get(_MODULE_NAME, 'os_version')
    docker_context = None

    if docker_context is None:
        docker_context = tempfile.mkdtemp(suffix=_MODULE_NAME)
    docker_makefile = os.path.join(_DOCKERIZER_ROOT, 'Makefile')
    _logger.debug('Using Docker context {}'.format(docker_context))

    # Populate the Docker context with the template containing dockerfiles,
    # entrypoints, etc.
    rsync_command = [
        'rsync',
        '-aq',  # Archive mode, no non-error messages
        '--exclude', '*.pyc',  # Don't copy any compiled Python files
        os.path.join(_DOCKERIZER_ROOT, 'context.template', ''),
        docker_context,
    ]

    retval = disdat.common.do_subprocess(rsync_command, cli)
    if retval: return retval

    # PIP: Overwrite pip.conf in the context.template in your repo if they have set the option,
    # else just create empty file.
    # At this time, the Dockerfile always sets the PIP_CONFIG_FILE ENV var to this file.
    retval = _copy_in_dot_file(disdat_config, docker_context, "pip.conf", "dot_pip_file", cli)
    if retval: return retval

    # ODBC: Overwrite the .odbc.ini in the context.template in your repo if the user set the option,
    # else just create empty file.
    # At this time, the Dockerfile always sets the ODBCINI var to this file.
    retval = _copy_in_dot_file(disdat_config, docker_context, "odbc.ini", "dot_odbc_ini_file", cli)
    if retval: return retval

    setup_file = os.path.join(pipeline_root, 'setup.py')

    if not disdat.common.setup_exists(setup_file):
        return 1

    pipeline_image_name = disdat.common.make_project_image_name(setup_file)

    DEFAULT_DISDAT_HOME = os.path.join('/', *os.path.dirname(disdat.dockerize.__file__).split('/')[:-1])
    DISDAT_HOME = os.getenv('DISDAT_HOME', DEFAULT_DISDAT_HOME)

    if build:
        pipe_version = determine_pipe_version(pipeline_root)
        build_command = [
            'make',  # XXX really need to check that this is GNU make
            '-f', docker_makefile,
            'PIPELINE_IMAGE_NAME={}'.format(pipeline_image_name),
            'DISDAT_DOCKER_CONTEXT={}'.format(docker_context),
            'DISDAT_ROOT={}'.format(os.path.join(DISDAT_HOME)),  # XXX YUCK
            'PIPELINE_ROOT={}'.format(pipeline_root),
            'OS_TYPE={}'.format(image_os_type),
            'OS_VERSION={}'.format(image_os_version),
            'GIT_HASH={}'.format(pipe_version.hash),
            'GIT_BRANCH={}'.format(pipe_version.branch),
            'GIT_FETCH_URL={}'.format(pipe_version.url),
            'GIT_TIMESTAMP={}'.format(pipe_version.tstamp),
            'GIT_DIRTY={}'.format(pipe_version.dirty),
        ]

        _logger.debug("pipeline root = {} build command = {}".format(pipeline_root, build_command))

        if config_dir is not None:
            build_command.append('CONFIG_ROOT={}'.format(config_dir))
        if sagemaker:
            build_command.append('SAGEMAKER_TRAIN_IMAGE_NAME={}'.format(disdat.common.make_sagemaker_project_image_name(setup_file)))
            build_command.append('sagemaker')
        retval = disdat.common.do_subprocess(build_command, cli)
        if retval: return retval

    if push:
        docker_client = docker.from_env()

        repository_name_prefix = None

        if disdat_config.parser.has_option('docker', 'repository_prefix'):
            repository_name_prefix = disdat_config.parser.get('docker', 'repository_prefix')
        if sagemaker:
            repository_name = disdat.common.make_sagemaker_project_repository_name(repository_name_prefix, setup_file)
            pipeline_image_name = disdat.common.make_sagemaker_project_image_name(setup_file)
        else:
            repository_name = disdat.common.make_project_repository_name(repository_name_prefix, setup_file)

        # Figure out the fully-qualified repository name, i.e., the name
        # including the registry.
        if disdat_config.parser.has_option('docker','registry'):
            registry_name = disdat_config.parser.get('docker', 'registry').strip('/')
            if registry_name == '*ECR*':
                policy_resource_name = None
                if disdat_config.parser.has_option('docker', 'ecr_policy'):
                    policy_resource_name = disdat_config.parser.get('docker', 'ecr_policy')
                fq_repository_name = aws.ecr_create_fq_respository_name(
                    repository_name,
                    policy_resource_package=disdat.resources,
                    policy_resource_name=policy_resource_name
                )
            else:
                fq_repository_name = '{}/{}'.format(registry_name, repository_name)
        else:
            if cli:
                raise RuntimeError("No registry present for push to succeed")
            else:
                return 1

        auth_config = None
        if disdat_config.parser.has_option('docker', 'ecr_login') or registry_name == '*ECR*':
            auth_config = aws.ecr_get_auth_config()
        docker_client.api.tag(pipeline_image_name, fq_repository_name)
        for line in docker_client.images.push(fq_repository_name, auth_config=auth_config, stream=True):
            if b'error' in line:
                if cli:
                    raise RuntimeError(line)
                else:
                    return 1
            else:
                if cli: print(line)

    return 0


def add_arg_parser(parsers):
    dockerize_p = parsers.add_parser('dockerize', description="Dockerizer a particular transform.")
    dockerize_p.add_argument(
        '--config-dir',
        type=str,
        default=None,
        help="A directory containing configuration files for the operating system within the Docker image",
    )
    dockerize_p.add_argument(
        '--os-type',
        type=str,
        default=None,
        help='The base operating system type for the Docker image',
    )
    dockerize_p.add_argument(
        '--os-version',
        type=str,
        default=None,
        help='The base operating system version for the Docker image',
    )
    dockerize_p.add_argument(
        '--push',
        action='store_true',
        help="Push the image to a remote Docker registry (default is to not push; must set 'docker_registry' in Disdat config)",
    )
    dockerize_p.add_argument(
        '--get-id',
        action='store_true',
        help="Do not build, only return latest container image ID",
    )
    dockerize_p.add_argument(
        '--sagemaker',
        action='store_true',
        default=False,
        help="Create a Docker image executable as a SageMaker container.",
    )
    dockerize_p.add_argument(
        '--no-build',
        action='store_false',
        help='Do not build an image (only copy files into the Docker build context)',
        dest='build',
    )
    dockerize_p.add_argument(
        "pipeline_root",
        type=str,
        help="Root of the Python source tree containing the user-defined transform; must have a setuptools-style setup.py file"
    )
    dockerize_p.set_defaults(func=lambda args: dockerize_entry(cli=True, **vars(args)))
    return parsers


def dockerize_entry(cli=False, **kwargs):
    """Run the dockerize command with parameters from the command line or the api.

    Parameters:
        cli (bool): Whether this was called from the command line or the api, default False
        **kwargs

    Returns:
        (int): 0 for success, 1 for failure
    """

    if 'get_id' in kwargs and kwargs['get_id']:
        return latest_container_id(kwargs['pipeline_root'], cli)
    else:
        return dockerize(kwargs['pipeline_root'],
                         config_dir=kwargs['config_dir'],
                         os_type=kwargs['os_type'],
                         os_version=kwargs['os_version'],
                         build=kwargs['build'],
                         push=kwargs['push'],
                         sagemaker=kwargs['sagemaker'],
                         cli=cli
                         )


if __name__ == "__main__":
    import api

    api.dockerize('/Users/kyocum/Code/anomaly-detection-service/anomaly', 'pipeline.pipeline.Train', push=True)