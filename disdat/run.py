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
Run

Run dockerized version of this pipe

pipes run input_bundle output_bundle pipes_cls

Run differs from apply.  Apply will run the transform locally / natively.
Run executes the most recently built container.   By default it will
first run it locally.   Containers can be built for different backends.
Backends do things in different ways.

1.) Entrypoint arguments.  They might customize endpoints based on the arguments.  E.g, -d <train> or <test>

2.) Entrypoint long vs. short-lived.  They might run a web-server in the container, e.g., to serve a model.


It will then need to run it remotely.   Thus we need to submit a job.


author: Kenneth Yocum
"""

# Built-in imports
import argparse

# Third-party imports
import boto3_session_cache as b3
import disdat.fs as fs
import disdat.common as common
import disdat.utility.aws_s3 as aws
import docker
import inspect
import logging
import luigi
import os
import tempfile
import time
import json
from disdat.common import DisdatConfig
from disdat.pipe_base import PipeBase
from enum import Enum
from luigi import build
from sys import platform


_MODULE_NAME = inspect.getmodulename(__file__)

_logger = logging.getLogger(__name__)


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


def _run_local(arglist, pipeline_class_name, backend):
    """
    Run container locally or run sagemaker container locally
    Args:
        arglist:
        pipeline_class_name:
        backend:

    Returns:

    """

    on_macos = False
    if platform == "darwin":
        on_macos = True

    client = docker.from_env()

    environment = {}
    if 'AWS_PROFILE' in os.environ:
        environment['AWS_PROFILE'] = os.environ['AWS_PROFILE']

    volumes = {}
    aws_config_dir = os.getenv('AWS_CONFIG_DIR', os.path.join(os.environ['HOME'], '.aws'))
    if aws_config_dir is not None and os.path.exists(aws_config_dir):
        volumes[aws_config_dir] = {'bind': '/root/.aws', 'mode': 'rw'}

    local_disdat_meta_dir = DisdatConfig.instance().get_meta_dir()
    volumes[local_disdat_meta_dir] = {'bind': '/root/.disdat', 'mode': 'rw'}

    try:
        if backend == Backend.LocalSageMaker:
            pipeline_image_name = common.make_sagemaker_pipeline_image_name(pipeline_class_name)
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
            pipeline_image_name = common.make_pipeline_image_name(pipeline_class_name)
            args = ' '.join(arglist)

        _logger.debug('Running image {} with arguments {}'.format(pipeline_image_name, args))

        print('Running image {} with arguments {}'.format(pipeline_image_name, args))

        stdout = client.containers.run(pipeline_image_name, args, detach=False,
                                       environment=environment, init=True, stderr=True, volumes=volumes)
        print stdout
    except docker.errors.ImageNotFound:
        _logger.error("Unable to find the docker image {}".format(pipeline_image_name))
        return


def get_fq_docker_repo_name(is_sagemaker, pipeline_class_name):
    """
    Produce the fully qualified docker repo name.

    Args:
        is_sagemaker (bool): for sagemaker image
        pipeline_class_name (str): the package.module.class string

    Returns:
        (str): The fully qualified docker image repository name
    """
    disdat_config = DisdatConfig.instance()

    repository_prefix = None
    if disdat_config.parser.has_option('docker', 'repository_prefix'):
        repository_prefix = disdat_config.parser.get('docker', 'repository_prefix')
    if is_sagemaker:
        repository_name = common.make_sagemaker_pipeline_repository_name(repository_prefix, pipeline_class_name)
    else:
        repository_name = common.make_pipeline_repository_name(repository_prefix, pipeline_class_name)

    # Figure out the fully-qualified repository name, i.e., the name
    # including the registry.
    registry_name = disdat_config.parser.get('docker', 'registry').strip('/')
    if registry_name == '*ECR*':
        fq_repository_name = aws.ecr_get_fq_respository_name(repository_name)
    else:
        fq_repository_name = '{}/{}'.format(registry_name, repository_name)

    return fq_repository_name


def _run_aws_batch(arglist, job_name, pipeline_class_name, aws_session_token_duration, vcpus, memory):
    """
    Run job on AWS Batch.   Sends to queue configured in disdat.cfg.
    This assumes that you have already created a cluster that will run the jobs
    that have been assigned to that queue.

    Args:
        arglist:
        pipeline_class_name:
        aws_session_token_duration:
        vcpus:
        memory:

    Returns:

    """

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

    # If the job definition does not exist, create it.
    job_definition_name = aws.batch_get_job_definition_name(pipeline_class_name)
    if disdat_config.parser.has_option(_MODULE_NAME, 'aws_batch_job_definition'):
        job_definition_name = disdat_config.parser.get(_MODULE_NAME, 'aws_batch_job_definition')
    job_definition = aws.batch_get_job_definition(job_definition_name)
    if job_definition is None:
        fq_repository_name = get_fq_docker_repo_name(False, pipeline_class_name)
        aws.batch_register_job_definition(job_definition_name, fq_repository_name, vcpus=vcpus, memory=memory)
        job_definition = aws.batch_get_job_definition(job_definition_name)
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
    if aws_session_token_duration > 0:
        sts_client = b3.client('sts')
        token = sts_client.get_session_token(DurationSeconds=aws_session_token_duration)
        credentials = token['Credentials']
        container_overrides['environment'] = [
            {'name': 'AWS_ACCESS_KEY_ID', 'value': credentials['AccessKeyId']},
            {'name': 'AWS_SECRET_ACCESS_KEY', 'value': credentials['SecretAccessKey']},
            {'name': 'AWS_SESSION_TOKEN', 'value': credentials['SessionToken']}
        ]
    job = client.submit_job(jobName=job_name, jobDefinition=job_definition, jobQueue=job_queue,
                            containerOverrides=container_overrides)
    status = job['ResponseMetadata']['HTTPStatusCode']
    if status == 200:
        print 'Job {} (ID {}) with definition {} submitted to AWS Batch queue {}'.format(job['jobName'], job['jobId'],
                                                                                         job_definition, job_queue)
    else:
        _logger.error('Job submission failed: HTTP Status {}'.format())


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


def _run_aws_sagemaker(arglist, job_name, pipeline_class_name):
    """
    Runs a training job on AWS SageMaker.  This uses the default machine type
    in the disdat.cfg file.

    Returns:

    """

    disdat_config = DisdatConfig.instance()

    job_name = job_name.replace('_', '-') # b/c SageMaker complains it must be ^[a-zA-Z0-9](-*[a-zA-Z0-9])*

    hyperparameter_dict = _sagemaker_hyperparameters_from_arglist(arglist)

    fq_repository_name = get_fq_docker_repo_name(True, pipeline_class_name)

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
        print "Disdat SageMaker configs"
        print "job name: {}".format(job_name)
        print "hparams: {}".format(hyperparameter_dict)
        print "algorithm: {}".format(algorithm_specification)
        print "Role ARN: {}".format(role_arn)
        print "Input data conf: {}".format(input_channel_config)
        print "Output data conf: {}".format(output_data_config)
        print "Resource conf: {}".format(resource_config)
        print "VPC conf: {}".format(vpc_config)
        print "Stopping condition seconds: {}".format(stopping_condition)
        print "Tags: {}".format(tags)

    client = b3.client('sagemaker', region_name=aws.profile_get_region())

    response = client.create_training_job(
        TrainingJobName=job_name,
        HyperParameters= hyperparameter_dict,
        AlgorithmSpecification=algorithm_specification,
        RoleArn=role_arn,
        InputDataConfig=input_channel_config,
        OutputDataConfig=output_data_config,
        ResourceConfig=resource_config,
        #VpcConfig=vpc_config,
        StoppingCondition=stopping_condition,
        Tags=tags
    )

    print "Disdat SageMaker create_training_job response {}".format(response)


def _run(
    input_bundle,
    output_bundle,
    pipeline_params,
    pipeline_class_name,
    backend=Backend.Local,
    force=False,  # @UnusedVariable
    push_input_bundle=True,
    aws_session_token_duration=0,
    input_tags=None,
    output_tags=None,
    fetch_list=None,
    context=None,
    remote=None,
    no_pull=False,
    vcpus=1,
    memory=2000
):
    """Run the dockerized version of a pipeline.

    Args:
        input_bundle: The human name of the input bundle
        output_bundle: The human name of the output bundle
        pipeline_class_name: Name of the pipeline class to run
        pipeline_params: Optional arguments to pass to the pipeline class
        backend: The batch execution back-end to use (default
            `Backend.Local`)
        force: If `True` force recomputation of all upstream pipe
            requirements (default `False`)
        input_tags (list(str)): Find bundle with these tags ['key:value',...]
        output_tags (list(str)): Push result bundle with these tags ['key:value',...]
        fetch_list (list(str)): Fetch these bundles before starting pipeline
        context (str): <remote context>/<local context> context string
        remote (str): The remote S3 URL.
        no_pull:
        vcpus:
        memory:

    Returns:
        `None`
    """

    pfs = fs.DisdatFS()

    try:
        output_bundle_uuid = pfs.disdat_uuid()
        if remote is None or context is None:
            # If either is none, then both are, b/c we checked in calling function
            remote, context = common.get_run_command_parameters(pfs)
    except ValueError:
        _logger.error("'run' requires a remote set with `dsdt remote <s3 url>`")
        return

    arglist = common.make_run_command(input_bundle, output_bundle, output_bundle_uuid, remote, context,
                                      input_tags, output_tags, fetch_list, no_pull, pipeline_params)

    if backend == Backend.AWSBatch or backend == Backend.SageMaker:

        # Make sure latest committed is sent to remote
        if push_input_bundle:
            result = pfs.push(human_name=input_bundle)
            if result is None:
                _logger.error("'run' failed to push input bundle {} to remote.".format(input_bundle))
                return

        pipeline_image_name = common.make_pipeline_image_name(pipeline_class_name)

        job_name = '{}-{}'.format(pipeline_image_name, int(time.time()))

        if backend == Backend.AWSBatch:
            _run_aws_batch(arglist, job_name, pipeline_class_name, aws_session_token_duration, vcpus, memory)
        else:
            _run_aws_sagemaker(arglist, job_name, pipeline_class_name)

    elif backend == Backend.Local or backend == Backend.LocalSageMaker:
        _run_local(arglist, pipeline_class_name, backend)

    else:
        raise ValueError('Got unrecognized job backend \'{}\': Expected {}'.format(backend, Backend.options()))


def add_arg_parser(parsers):
    run_p = parsers.add_parser('run', description="Run a containerized version of transform.")
    run_p.add_argument(
        '--backend',
        default=Backend.default(),
        type=str,
        choices=Backend.options(),
        help='An optional batch execution back-end to use',
    )
    run_p.add_argument("--force", action='store_true', help="If there are dependencies, force re-computation.")
    run_p.add_argument("--no-push-input", action='store_false',
                       help="Do not push the current committed input bundle before execution (default is to push)", dest='push_input_bundle')
    run_p.add_argument(
        '--no-pull',
        action='store_true',
        help='Do not pull (synchronize) remote repository with local repo.  This may cause entire pipeline to re-run.',
    )
    run_p.add_argument(
        '--use-aws-session-token',
        nargs=1,
        default=[0],
        type=int,
        help='Use a temporary AWS session token to access the remote, valid for AWS_SESSION_TOKEN_DURATION seconds',
        dest='aws_session_token_duration',
    )
    run_p.add_argument('--vcpus',
                       type=int,
                       default=1,
                       help="The vCPU count for an AWS Batch container.")
    run_p.add_argument('--memory',
                       type=int,
                       default=2000,
                       help="The memory (MiB) required by this AWS Batch container.")
    run_p.add_argument('-c', '--context',
                       type=str,
                       default=None,
                       help="'<remote context>/<local context>', else use current context.")
    run_p.add_argument('-r', '--remote',
                       type=str,
                       default=None,
                       help="Remote URL, i.e, 's3://<bucket>/dsdt/', else use remote on current context")
    run_p.add_argument('-f', '--fetch', nargs=1, type=str, action='append',
                       help="Fetch bundle into context: '-f some.input.bundle'")
    run_p.add_argument('-it', '--input-tag', nargs=1, type=str, action='append',
                       help="Input bundle tags: '-it authoritative:True -it version:0.7.1'")
    run_p.add_argument('-ot', '--output-tag', nargs=1, type=str, action='append',
                       help="Output bundle tags: '-ot authoritative:True -ot version:0.7.1'")
    run_p.add_argument("input_bundle", type=str, help="Name of source data bundle.  '-' means no input bundle.")
    run_p.add_argument("output_bundle", type=str, help="Name of destination bundle.  '-' means default output bundle.")
    run_p.add_argument("pipe_cls", type=str, help="User-defined transform, e.g., module.PipeClass")
    run_p.add_argument("pipeline_args", type=str,  nargs=argparse.REMAINDER,
                       help="Optional set of parameters for this pipe '--parameter value'")
    run_p.set_defaults(func=lambda args: main(args))
    return parsers


def main(args):
    """Convert cli args into luigi task and params and execute

    Args:
        args:

    Returns:
        `None`
    """

    pfs = fs.DisdatFS()

    # if any set, the other must be set.  if a or b, then a and b are set
    if args.context is not None or args.remote is not None:
        if args.context is None or args.remote is None:
            print "Must set both context [{}] and remote [{}] simultaneously.".format(args.context, args.remote)
            return
    else:
        if not pfs.in_context():
            print "'Must be in a context (or specify --context and --remote) to execute 'dsdt run'"
            return

    if args.backend is not None:
        backend = Backend[args.backend]
    else:
        backend = Backend.default()

    input_tags = common.parse_args_tags(args.input_tag, to='list')
    output_tags = common.parse_args_tags(args.output_tag, to='list')

    if args.fetch:
        fetch_list = ['{}'.format(kv[0]) for kv in args.fetch]
    else:
        fetch_list = []

    _run(input_bundle=args.input_bundle,
         output_bundle=args.output_bundle,
         pipeline_class_name=args.pipe_cls,
         pipeline_params=list(args.pipeline_args),  # for some reason ListParameter is creating a tuple
         backend=backend,
         force=bool(args.force),
         push_input_bundle=bool(args.push_input_bundle),
         aws_session_token_duration=args.aws_session_token_duration[0],
         input_tags=input_tags,
         output_tags=output_tags,
         fetch_list=fetch_list,
         context=args.context,
         remote=args.remote,
         no_pull=args.no_pull,
         vcpus=args.vcpus,
         memory=args.memory
         )
