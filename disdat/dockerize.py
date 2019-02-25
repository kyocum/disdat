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
import subprocess
import tempfile
import shutil

# Third-party imports
import disdat.common
import disdat.resources
import disdat.utility.aws_s3 as aws
from disdat.infrastructure import dockerizer

import docker

_MODULE_NAME = inspect.getmodulename(__file__)

_DOCKERIZER_ROOT = os.path.dirname(inspect.getsourcefile(dockerizer))

_logger = logging.getLogger(__name__)


def _do_subprocess(cmd, cli):
    """ Standardize error processing

    Args:
        cmd (str): command to execute
        cli (bool): whether called from CLI (True) or API (False)

    Returns:
        (int): 0 if success, >0 if failure

    """
    output = 'No captured output from running CMD [{}]'.format(cmd)
    try:
        if not cli:
            output = subprocess.check_output(cmd)
        else:
            subprocess.check_call(cmd)
    except subprocess.CalledProcessError as cpe:
        if not cli:
            print (output)
            return cpe.returncode
        raise

    return 0


def dockerize(pipeline_root,
              pipeline_class_name,
              config_dir=None,
              os_type=None,
              os_version=None,
              build=True,
              push=False,
              sagemaker=False,
              cli=False):
    """ Create a Docker image for running a pipeline.

    Args:
        pipeline_root: Root of the Python source tree containing the
        user-defined transform; must have a setuptools-style setup.py file.
        pipeline_class_name: The fully-qualified name of the pipeline
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

    retval = _do_subprocess(rsync_command, cli)
    if retval: return retval

    # PIP: Overwrite pip.conf in the context.template in your repo if they have set the option,
    # else just create empty file.
    # At this time, the Dockerfile always sets the PIP_CONFIG_FILE ENV var to this file.
    pip_file_path = os.path.join(docker_context, "pip.conf")
    if disdat_config.parser.has_option(_MODULE_NAME, 'dot_pip_file'):
        dot_pip_file = os.path.expanduser(disdat_config.parser.get(_MODULE_NAME, 'dot_pip_file'))
        shutil.copy(dot_pip_file, pip_file_path)
        print("Copying dot pip file {} into {}".format(dot_pip_file, docker_context))
    else:
        touch_command = [
            'touch',
            pip_file_path
        ]
        retval = _do_subprocess(touch_command, cli)
        if retval: return retval

    # ODBC: Overwrite the .odbc.ini in the context.template in your repo if the user set the option,
    # else just create empty file.
    # At this time, the Dockerfile always sets the ODBCINI var to this file.
    odbc_file_path = os.path.join(docker_context,"odbc.ini")
    if disdat_config.parser.has_option(_MODULE_NAME, 'dot_odbc_ini_file'):
        dot_odbc_ini_file = os.path.expanduser(disdat_config.parser.get(_MODULE_NAME, 'dot_odbc_ini_file'))
        shutil.copy(dot_odbc_ini_file, odbc_file_path)
        print("Copying dot odbc.ini file {} into {}".format(dot_odbc_ini_file, odbc_file_path))
    else:
        touch_command = [
            'touch',
            odbc_file_path
        ]
        retval = _do_subprocess(touch_command, cli)
        if retval: return retval

    pipeline_image_name = disdat.common.make_pipeline_image_name(pipeline_class_name)
    DEFAULT_DISDAT_HOME = os.path.join('/', *os.path.dirname(disdat.dockerize.__file__).split('/')[:-1])
    DISDAT_HOME = os.getenv('DISDAT_HOME', DEFAULT_DISDAT_HOME)

    if build:
        build_command = [
            'make',  # XXX really need to check that this is GNU make
            '-f', docker_makefile,
            'PIPELINE_IMAGE_NAME={}'.format(pipeline_image_name),
            'DOCKER_CONTEXT={}'.format(docker_context),
            'DISDAT_ROOT={}'.format(os.path.join(DISDAT_HOME)),  # XXX YUCK
            'PIPELINE_ROOT={}'.format(pipeline_root),
            'PIPELINE_CLASS={}'.format(pipeline_class_name),
            'OS_TYPE={}'.format(image_os_type),
            'OS_VERSION={}'.format(image_os_version),
        ]
        if config_dir is not None:
            build_command.append('CONFIG_ROOT={}'.format(config_dir))
        if sagemaker:
            build_command.append('SAGEMAKER_TRAIN_IMAGE_NAME={}'.format(disdat.common.make_sagemaker_pipeline_image_name(pipeline_class_name)))
            build_command.append('sagemaker')
        retval = _do_subprocess(build_command, cli)
        if retval: return retval

    if push:
        docker_client = docker.from_env()
        repository_name_prefix = None
        if disdat_config.parser.has_option('docker', 'repository_prefix'):
            repository_name_prefix = disdat_config.parser.get('docker', 'repository_prefix')
        if sagemaker:
            repository_name = disdat.common.make_sagemaker_pipeline_repository_name(repository_name_prefix, pipeline_class_name)
            pipeline_image_name = disdat.common.make_sagemaker_pipeline_image_name(pipeline_class_name)
        else:
            repository_name = disdat.common.make_pipeline_repository_name(repository_name_prefix, pipeline_class_name)
        # Figure out the fully-qualified repository name, i.e., the name
        # including the registry.
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
        auth_config = None
        if disdat_config.parser.has_option('docker', 'ecr_login') or registry_name == '*ECR*':
            auth_config = aws.ecr_get_auth_config()
        docker_client.api.tag(pipeline_image_name, fq_repository_name)
        for line in docker_client.images.push(fq_repository_name, auth_config=auth_config, stream=True):
            if 'error' in line:
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
        "pipe_root",
        type=str,
        help="Root of the Python source tree containing the user-defined transform; must have a setuptools-style setup.py file"
    )
    dockerize_p.add_argument("pipe_cls", type=str, help="User-defined transform, e.g., module.PipeClass")
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

    return dockerize(kwargs['pipe_root'],
                     kwargs['pipe_cls'],
                     config_dir=kwargs['config_dir'],
                     os_type=kwargs['os_type'],
                     os_version=kwargs['os_version'],
                     build=kwargs['build'],
                     push=kwargs['push'],
                     sagemaker=kwargs['sagemaker'],
                     cli=cli
                     )
