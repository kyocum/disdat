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

"""
Run dockerized version of this pipe

Run differs from apply.  Apply will run the transform locally / natively.
Run executes the most recently built container.   By default it will
first run it locally.   Containers can be built for different backends.
Backends do things in different ways.

1.) Entrypoint arguments.  They might customize endpoints based on the arguments.  E.g, -d <train> or <test>

2.) Entrypoint long vs. short-lived.  They might run a web-server in the container, e.g., to serve a model.


author: Kenneth Yocum
"""
from __future__ import print_function

import argparse
import inspect
import os
import tempfile
import time
import json
from sys import platform

import docker
import six
import boto3 as b3
from enum import Enum

import disdat.fs as fs
import disdat.common as common
import disdat.utility.aws_s3 as aws
from disdat.common import DisdatConfig
from disdat import logger as _logger


_MODULE_NAME = inspect.getmodulename(__file__)


ENTRYPOINT_BIN = '/opt/bin/entrypoint.py'


class Backend(Enum):
    Local = 0
    AWSBatch = 1
    LocalSageMaker = 2
    SageMaker = 3

    @staticmethod
    def default():
        return Backend.Local.name

    @staticmethod
    def options():
        return [i.name for i in list(Backend)]


def _run_local(cli, pipeline_setup_file, arglist, backend):
    """
    Run container locally or run sagemaker container locally
    Args:
        cli (bool): Whether we were called from the CLI or API
        pipeline_setup_file (str): The FQ path to the setup.py used to dockerize the pipeline.
        arglist:
        backend:

    Returns:
        output (str): Returns None if there is a failure

    """

    on_macos = False
    if platform == "darwin":
        on_macos = True

    client = docker.from_env()

    environment = {}
    if 'AWS_PROFILE' in os.environ:
        environment['AWS_PROFILE'] = os.environ['AWS_PROFILE']

    environment[common.LOCAL_EXECUTION] = 'True'

    # Todo: Local runs do not yet set resource limits, but when they do, we'll have to set this
    #environment['DISDAT_CPU_COUNT'] = vcpus

    volumes = {}
    aws_config_dir = os.getenv('AWS_CONFIG_DIR', os.path.join(os.environ['HOME'], '.aws'))
    if aws_config_dir is not None and os.path.exists(aws_config_dir):
        volumes[aws_config_dir] = {'bind': '/root/.aws', 'mode': 'rw'}

    local_disdat_meta_dir = DisdatConfig.instance().get_meta_dir()
    volumes[local_disdat_meta_dir] = {'bind': '/root/.disdat', 'mode': 'rw'}

    try:
        if backend == Backend.LocalSageMaker:
            pipeline_image_name = common.make_sagemaker_project_image_name(pipeline_setup_file)
            tempdir = tempfile.mkdtemp()
            with open(os.path.join(tempdir, 'hyperparameters.json'), 'w') as of:
                json.dump(_sagemaker_hyperparameters_from_arglist(arglist), of)
                args = ['train']  # rewrite to just 'train'
                # On mac OS, tempdir returns /var, but is actually /private/var
                # Add /private since it that dir is shared (and not /var) with Docker.
                if on_macos:
                    localdir = os.path.join('/private', tempdir[1:])
                else:
                    localdir = tempdir
                volumes[localdir] = {'bind': '/opt/ml/input/config/', 'mode': 'rw'}
                _logger.info("VOLUMES: {}".format(volumes))
        else:
            # Add the actual command to the arglist (for non-sagemaker runs)
            arglist = [ENTRYPOINT_BIN] + arglist
            pipeline_image_name = common.make_project_image_name(pipeline_setup_file)

        _logger.debug('Running image {} with arguments {}'.format(pipeline_image_name, arglist))

        stdout = client.containers.run(pipeline_image_name, arglist, detach=False,
                                       environment=environment, init=True, stderr=True, volumes=volumes)
        stdout = six.ensure_str(stdout)
        if cli: print(stdout)
        return stdout
    except docker.errors.ContainerError as ce:
        _logger.error("Internal error running image {}".format(pipeline_image_name))
        _logger.error("Error: {}".format(six.ensure_str(ce.stderr)))
        return six.ensure_str(ce)
    except docker.errors.ImageNotFound:
        _logger.error("Unable to find the docker image {}".format(pipeline_image_name))
        return None


def get_fq_docker_repo_name(is_sagemaker, pipeline_setup_file):
    """
    Produce the fully qualified docker repo name.

    Args:
        is_sagemaker (bool): for sagemaker image
        pipeline_setup_file (str): the path to the setup.py file used to dockerize this pipeline

    Returns:
        (str): The fully qualified docker image repository name
    """
    disdat_config = DisdatConfig.instance()

    repository_prefix = None
    if disdat_config.parser.has_option('docker', 'repository_prefix'):
        repository_prefix = disdat_config.parser.get('docker', 'repository_prefix')
    if is_sagemaker:
        repository_name = common.make_sagemaker_project_repository_name(repository_prefix, pipeline_setup_file)
    else:
        repository_name = common.make_project_repository_name(repository_prefix, pipeline_setup_file)

    # Figure out the fully-qualified repository name, i.e., the name
    # including the registry.
    registry_name = disdat_config.parser.get('docker', 'registry').strip('/')
    if registry_name == '*ECR*':
        fq_repository_name = aws.ecr_get_fq_repository_name(repository_name)
    else:
        fq_repository_name = '{}/{}'.format(registry_name, repository_name)

    return fq_repository_name


def _run_aws_batch(arglist, fq_repository_name, job_name, pipeline_image_name,
                   aws_session_token_duration, vcpus, memory,
                   no_submit, job_role_arn):
    """
    Run job on AWS Batch.   Sends to queue configured in disdat.cfg.
    This assumes that you have already created a cluster that will run the jobs
    that have been assigned to that queue.

    Args:
        arglist:
        fq_repository_name (str): The fully qualified docker repository name
        job_name:
        pipeline_image_name:
        aws_session_token_duration:
        vcpus:
        memory:
        no_submit (bool): default False
        job_role_arn (str): Can be None

    Returns:

    """

    def check_role_arn(job_dict, jra):
        """ Check to see if the job desc dictionary contains the same job_role_arn (jra)
        """

        if jra is None:
            if 'jobRoleArn' not in job_dict['containerProperties']:
                return True
        else:
            if 'jobRoleArn' in job_dict['containerProperties']:
                if job_dict['containerProperties']['jobRoleArn'] == jra:
                    return True
        return False

    disdat_config = DisdatConfig.instance()

    # Get the parameter values required to kick off an AWS Batch job.
    # Every batch job must:
    # 1. Have a name
    # 2. Have a job definition that declares which ECR-hosted Docker
    #    image to use.
    # 3. Have a queue that feeds jobs into a compute cluster.
    # 4. The command to execute inside the Docker image; the command
    #    args are more-or-less the same as the ones used to execute
    #    locally using 'dsdt run'

    # Create a Job Definition and upload it.
    # We create per-user job definitions so multiple users do not clobber each other.
    # In addition, we never re-use a job definition, since the user may update
    # the vcpu or memory requirements and those are stuck in the job definition

    job_definition_name = aws.batch_get_job_definition_name(pipeline_image_name)

    if disdat_config.parser.has_option(_MODULE_NAME, 'aws_batch_job_definition'):
        job_definition_name = disdat_config.parser.get(_MODULE_NAME, 'aws_batch_job_definition')

    # TODO: Look through all of history to find one that matches?
    # TODO: Delete old jobs here or let user do it?
    job_definition_obj = aws.batch_get_latest_job_definition(job_definition_name)

    if (job_definition_obj is not None and
            job_definition_obj['containerProperties']['image'] == fq_repository_name and
            job_definition_obj['containerProperties']['vcpus'] == vcpus and
            job_definition_obj['containerProperties']['memory'] == memory and
            check_role_arn(job_definition_obj, job_role_arn)):

        job_definition_fqn = aws.batch_extract_job_definition_fqn(job_definition_obj)

        _logger.info("Re-using prior AWS Batch run job definition : {}".format(job_definition_obj))

    else:
        """ Whether None or doesn't match, make a new one """

        job_definition_obj = aws.batch_register_job_definition(job_definition_name, fq_repository_name,
                                          vcpus=vcpus, memory=memory, job_role_arn=job_role_arn)

        job_definition_fqn = aws.batch_get_job_definition(job_definition_name)

        _logger.info("New AWS Batch run job definition {}".format(job_definition_fqn))

    if no_submit:
        # Return the job description object
        return job_definition_obj

    job_queue = disdat_config.parser.get(_MODULE_NAME, 'aws_batch_queue')

    container_overrides = {'command': arglist}

    # Through the magic of boto3_session_cache, the client in our script
    # here can get at AWS profiles and temporary AWS tokens created in
    # part from MFA tokens generated through the user's shells; we don't
    # have to write special code of our own to deal with authenticating
    # with AWS.
    client = b3.client('batch', region_name=aws.profile_get_region())
    # A bigger problem might be that the IAM role executing the job on
    # a batch EC2 instance might not have access to the S3 remote. To
    # get around this, allow the user to create some temporary AWS
    # credentials.

    if aws_session_token_duration > 0 and job_role_arn is None:
        sts_client = b3.client('sts')
        try:
            token = sts_client.get_session_token(DurationSeconds=aws_session_token_duration)
            credentials = token['Credentials']
            container_overrides['environment'] = [
                {'name': 'AWS_ACCESS_KEY_ID', 'value': credentials['AccessKeyId']},
                {'name': 'AWS_SECRET_ACCESS_KEY', 'value': credentials['SecretAccessKey']},
                {'name': 'AWS_SESSION_TOKEN', 'value': credentials['SessionToken']}
            ]
        except Exception as e:
            _logger.debug("Unable to generate an STS token, instead trying users default credentials...")
            credentials = b3.session.Session().get_credentials()
            container_overrides['environment'] = [
                {'name': 'AWS_ACCESS_KEY_ID', 'value': credentials.access_key},
                {'name': 'AWS_SECRET_ACCESS_KEY', 'value': credentials.secret_key},
                {'name': 'AWS_SESSION_TOKEN', 'value': credentials.token}
            ]

    container_overrides['environment'].append({'name': 'DISDAT_CPU_COUNT', 'value': str(vcpus)})

    job = client.submit_job(jobName=job_name, jobDefinition=job_definition_fqn, jobQueue=job_queue,
                            containerOverrides=container_overrides)

    status = job['ResponseMetadata']['HTTPStatusCode']
    if status == 200:
        _logger.info('Job {} (ID {}) with definition {} submitted to AWS Batch queue {}'.format(job['jobName'], job['jobId'],
                                                                                         job_definition_fqn, job_queue))
        return job
    else:
        _logger.error('Job submission failed: HTTP Status {}'.format())
        return None


def _sagemaker_hyperparameters_from_arglist(arglist):
    """
    Return a dictionary of str:str that emulates
    what SageMaker will return when using boto3 interface.

    Args:
        arglist (list (str)): List of string arguments to entrypoint.py

    Returns:
        (dict (str:str)): Dictionary of string to string
    """

    return {'arglist': json.dumps(arglist)}


def _run_aws_sagemaker(arglist, fq_repository_name, job_name):
    """
    Runs a training job on AWS SageMaker.  This uses the default machine type
    in the disdat.cfg file.

    Args:
        arglist:
        fq_repository_name (str): fully qualified repository name
        job_name:  instance job name

    Returns:
        TrainingJobArn (str)
    """

    disdat_config = DisdatConfig.instance()

    job_name = job_name.replace('_', '-') # b/c SageMaker complains it must be ^[a-zA-Z0-9](-*[a-zA-Z0-9])*

    hyperparameter_dict = _sagemaker_hyperparameters_from_arglist(arglist)

    algorithm_specification = {'TrainingImage': fq_repository_name,
                               'TrainingInputMode': 'File'}

    role_arn = disdat_config.parser.get(_MODULE_NAME, 'aws_sagemaker_role_arn')

    input_channel_config = [
        {
            'ChannelName': 'disdat_sagemaker_input_blackhole',
            'DataSource': {
                'S3DataSource': {
                    'S3DataType': 'S3Prefix',
                    'S3Uri': disdat_config.parser.get(_MODULE_NAME, 'aws_sagemaker_s3_input_uri'),
                    'S3DataDistributionType': 'FullyReplicated'
                }
            },
            'ContentType': 'application/javascript',
            'CompressionType': 'None',  # | 'Gzip',
            'RecordWrapperType': 'None' # | 'RecordIO'
        },
    ]

    output_data_config = {'S3OutputPath': os.path.join(
        disdat_config.parser.get(_MODULE_NAME, 'aws_sagemaker_s3_output_uri'), job_name)}

    resource_config = {
            'InstanceType': disdat_config.parser.get(_MODULE_NAME, 'aws_sagemaker_instance_type'),
            'InstanceCount': int(disdat_config.parser.get(_MODULE_NAME, 'aws_sagemaker_instance_count')),
            'VolumeSizeInGB': int(disdat_config.parser.get(_MODULE_NAME, 'aws_sagemaker_volume_sizeGB'))
            # 'VolumeKmsKeyId': 'string'
        }

    vpc_config = None #'SecurityGroupIds': [], 'Subnets': []}

    stopping_condition = {'MaxRuntimeInSeconds': int(disdat_config.parser.get(_MODULE_NAME, 'aws_sagemaker_max_runtime_sec'))}

    tags = [{'Key': 'user', 'Value': 'disdat'},
            {'Key': 'job', 'Value': job_name}]

    if False:
        print("Disdat SageMaker configs")
        print("job name: {}".format(job_name))
        print("hparams: {}".format(hyperparameter_dict))
        print("algorithm: {}".format(algorithm_specification))
        print("Role ARN: {}".format(role_arn))
        print("Input data conf: {}".format(input_channel_config))
        print("Output data conf: {}".format(output_data_config))
        print("Resource conf: {}".format(resource_config))
        print("VPC conf: {}".format(vpc_config))
        print("Stopping condition seconds: {}".format(stopping_condition))
        print("Tags: {}".format(tags))

    client = b3.client('sagemaker', region_name=aws.profile_get_region())

    response = client.create_training_job(
        TrainingJobName=job_name,
        HyperParameters= hyperparameter_dict,
        AlgorithmSpecification=algorithm_specification,
        RoleArn=role_arn,
        InputDataConfig=input_channel_config,
        OutputDataConfig=output_data_config,
        ResourceConfig=resource_config,
        StoppingCondition=stopping_condition,
        Tags=tags
    )

    _logger.info("Disdat SageMaker create_training_job response {}".format(response))
    return response['TrainingJobArn']


def _run(
        output_bundle = '-',
        pipeline_root = '',
        pipeline_args = '',
        pipe_cls = None,
        backend = None,
        input_tags = {},
        output_tags = {},
        force = False,
        force_all = False,
        context = None,
        remote = None,
        no_pull = False,
        no_push = False,
        no_push_int = False,
        vcpus = 1,
        memory = 2000,
        workers = 1,
        no_submit = False,
        job_role_arn = None,
        aws_session_token_duration = 0,
        cli=False):
    """Run the dockerized version of a pipeline.

    Note these are named parameters so we avoid bugs related to argument order.

    Args:
        output_bundle (str): The human name of the output bundle
        pipeline_root (str): The path to the setup.py used to create the container
        pipeline_args: Optional arguments to pass to the pipeline class
        pipe_cls: Name of the pipeline class to run
        backend: The batch execution back-end to use (default
            `Backend.Local`)
        input_tags (list(str)): Find bundle with these tags ['key:value',...]
        output_tags (list(str)): Push result bundle with these tags ['key:value',...]
        force (bool): If `True` force recomputation of all upstream tasks (default `False`)
        force_all (bool): If `True` force recomputation of last task (default `False`)
        context (str): <remote context>/<local context> context string
        remote (str): The remote S3 URL.
        no_pull (bool): Do not pull before executing (start in empty local context)
        no_push (bool): Do not push any new bundles to remote (useful for testing locally)
        no_push_int (bool): Do not push new intermediate bundles to remote
        vcpus (int):  Number of AWS vCPUs the container requests
        memory (int):  Amount of memory container requests in MB
        workers (int): Number of Luigi workers to run tasks in DAG
        no_submit (bool): Produce the AWS job config (for AWS Batch), but do not submit the job
        job_role_arn (str):  The AWS role under which the job should execute
        aws_session_token_duration (int): the number of seconds our temporary credentials should last.
        cli (bool): Whether we called run from the API (buffer output) or the CLI

    Returns:
        job_result (json): A json blob that contains information about the run job.  Error with empty dict.  If backend
        is Sagemaker, return TrainingJobArn.   If backend is AWSBatch, return Batch Job description.   If local, return stdout.
    """
    def assert_or_log(cli, msg):
        if cli:
            _logger.error(msg)
        else:
            assert False, msg

    pfs = fs.DisdatFS()
    pipeline_setup_file = os.path.join(pipeline_root, 'setup.py')

    if not common.setup_exists(pipeline_setup_file):
        return assert_or_log(cli, "Disdat run: Unable to find setup.py file [{}].".format(pipeline_setup_file))

    # When run in a container, we create the uuid externally to look for a specific result
    output_bundle_uuid = pfs.disdat_uuid()

    # If the user did not specify a context, use the configuration of the current context
    if context is None:
        if not pfs.in_context():
            return assert_or_log(cli, "Disdat run: Not running in a local context. Switch or specify.")
        remote, context = common.get_run_command_parameters(pfs)

    if remote is None and (not no_push or not no_pull):  # if pulling or pushing, need a remote
        return assert_or_log(cli, "Pushing or pulling bundles with 'run' requires a remote.")

    arglist = common.make_run_command(output_bundle, output_bundle_uuid, pipe_cls, remote, context,
                                      input_tags, output_tags, force, force_all, no_pull, no_push,
                                      no_push_int, workers, pipeline_args)

    if backend == Backend.AWSBatch or backend == Backend.SageMaker:

        pipeline_image_name = common.make_project_image_name(pipeline_setup_file)

        job_name = '{}-{}'.format(pipeline_image_name, int(time.time()))

        fq_repository_name = get_fq_docker_repo_name(False, pipeline_setup_file)

        if backend == Backend.AWSBatch:

            # Add the actual command to the arglist
            arglist = [ENTRYPOINT_BIN] + arglist

            retval = _run_aws_batch(arglist,
                                    fq_repository_name,
                                    job_name,
                                    pipeline_image_name,
                                    aws_session_token_duration,
                                    vcpus,
                                    memory,
                                    no_submit,
                                    job_role_arn)
        else:

            fq_repository_name = get_fq_docker_repo_name(True, pipeline_root)

            retval = _run_aws_sagemaker(arglist,
                                        fq_repository_name,
                                        job_name)

    elif backend == Backend.Local or backend == Backend.LocalSageMaker:
        retval = _run_local(cli, pipeline_setup_file, arglist, backend)

    else:
        raise ValueError('Got unrecognized job backend \'{}\': Expected {}'.format(backend, Backend.options()))

    return retval


def add_arg_parser(parsers):
    run_p = parsers.add_parser('run', description="Run a containerized version of transform.")
    run_p.add_argument(
        '--backend',
        default=Backend.default(),
        type=str,
        choices=Backend.options(),
        help='An optional batch execution back-end to use',
    )
    run_p.add_argument(
        "-f",
        "--force",
        action='store_true',
        help="Force recomputation of the last task."
    )
    run_p.add_argument(
        '--force-all',
        action='store_true',
        help="Force re-computation of ALL upstream tasks.")
    run_p.add_argument(
        "--no-submit",
        action='store_true',
        help="For AWS Batch: Do not submit job -- only create the Batch job description."
    )
    run_p.add_argument(
        '--no-push-intermediates',
        action='store_true',
        help='Do not push the intermediate bundles to the remote repository (default is to push)',
        dest='no_push_int'
    )
    run_p.add_argument(
        '--pull',
        action='store_true',
        default=None,
        help="Synchronize local repo and remote before execution. Default False if 'local' backend, else default is True."
    )
    run_p.add_argument(
        '--push',
        action='store_true',
        default=None,
        help="Push bundles to remote context. Default False if 'local' backend, else default is True."
    )
    run_p.add_argument(
        '--use-aws-session-token',
        default=43200, # 12 hours of default time -- for long pipelines!
        type=int,
        help='For AWS Batch: Use temporary AWS session token, valid for AWS_SESSION_TOKEN_DURATION seconds. Default 43200. Set to zero to not use a token.',
        dest='aws_session_token_duration',
    )
    run_p.add_argument('--workers',
                       type=int,
                       default=2,
                       help="The number of Luigi workers to spawn.  Default is 2.")
    run_p.add_argument('--vcpus',
                       type=int,
                       default=2,
                       help="The vCPU count for an AWS Batch container.")
    run_p.add_argument('--memory',
                       type=int,
                       default=4000,
                       help="The memory (MiB) required by this AWS Batch container.")
    run_p.add_argument('--job-role-arn',
                       type=str,
                       default=None,
                       help="For AWS Batch: Use this ARN to indicate the role under which batch containers run.")
    run_p.add_argument('-c', '--context',
                       type=str,
                       default=None,
                       help="'<remote context>/<local context>', else use current context.")
    run_p.add_argument('-r', '--remote',
                       type=str,
                       default=None,
                       help="Remote URL, i.e, 's3://<bucket>/dsdt/', else use remote on current context")
    run_p.add_argument('-it', '--input-tag', nargs=1, type=str, action='append',
                       help="Input bundle tags: '-it authoritative:True -it version:0.7.1'",
                       dest='input_tags')
    run_p.add_argument('-ot', '--output-tag', nargs=1, type=str, action='append',
                       help="Output bundle tags: '-ot authoritative:True -ot version:0.7.1'",
                       dest='output_tags')
    run_p.add_argument('-o', '--output-bundle', type=str, default='-',
                       help="Name output bundle: '-o my.output.bundle'.  Default name is '<TaskName>_<param_hash>'")
    run_p.add_argument("pipeline_root", type=str, help="Root of the Python source tree containing the user-defined transform; must have a setuptools-style setup.py file")
    run_p.add_argument("pipe_cls", type=str, help="User-defined transform, e.g., module.PipeClass")
    run_p.add_argument("pipeline_args", type=str,  nargs=argparse.REMAINDER,
                       help="Optional set of parameters for this pipe '--parameter value'")
    run_p.set_defaults(func=lambda args: run_entry(cli=True, **vars(args)))
    return parsers


def run_entry(cli=False, **kwargs):
    """
    Handles parameter defaults for calling _run()

    From the CLI, the parseargs object has all the arguments
    From the API, the arguments are explicitly set

    Note: pipeline_args is an array of args: ['name',json.dumps(value),'name2',json.dumps(value2)]

    Args:
        kwargs (dict):

    Returns:

    """
    if kwargs['backend'] is not None:
        kwargs['backend'] = Backend[kwargs['backend']]
    else:
        kwargs['backend'] = Backend[Backend.default()]

    # CLI and API only set push or pull.  Translate to no-push no-pull in original run code.
    # If backend == 'Local' then don't pull or push by default
    # TODO: change run() semantics to push pull, not no_push, no_pull.
    if kwargs['push'] is not None:
        kwargs['no_push'] = not kwargs['push']
    else:
        kwargs['no_push'] = True if kwargs['backend'] == Backend.Local else False

    if kwargs['pull'] is not None:
        kwargs['no_pull'] = not kwargs['pull']
    else:
        kwargs['no_pull'] = True if kwargs['backend'] == Backend.Local else False

    # Ensure kwargs only contains the arguments we want when calling _run
    remove_keys = []
    for k in kwargs.keys():
        if k not in _run.__code__.co_varnames:
            remove_keys.append(k)

    for k in remove_keys:
        kwargs.pop(k)

    kwargs['input_tags'] = common.parse_args_tags(kwargs['input_tags'], to='list')
    kwargs['output_tags'] = common.parse_args_tags(kwargs['output_tags'], to='list')
    kwargs['cli'] = cli

    return _run(**kwargs)
