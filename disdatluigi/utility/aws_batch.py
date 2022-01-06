#
# Copyright 2015, 2016, 2017 Human Longevity, Inc.
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
Utilities for accessing AWS using boto3

@author: twong / kyocum
@copyright: Human Longevity, Inc. 2017
@license: Apache 2.0
"""

import base64
import pkg_resources
from getpass import getuser
import six

from botocore.exceptions import ClientError
import boto3 as b3

from disdatluigi import logger as _logger
from disdat.utility.aws_s3 import profile_get_region

def batch_get_job_definition_name(pipeline_image_name):
    """Get the most recent active AWS Batch job definition for a dockerized
    pipeline.

    Note: The Python getpass docs do not specify the exception thrown when getting the user name fails.

    """

    try:
        return '{}-{}-job-definition'.format(getuser(), pipeline_image_name)
    except Exception as e:
        return '{}-{}-job-definition'.format('DEFAULT', pipeline_image_name)


def batch_get_latest_job_definition(job_definition_name):
    """Get the most recent active revision number for a AWS Batch job
    definition

    Args:
        job_definition_name: The name of the job definition
        remote_pipeline_image_name:
        vcpus:
        memory:

    Return:
        The latest job definition dictionary or `None` if the job definition does not exist
    """
    region = profile_get_region()
    client = b3.client('batch', region_name=region)
    response = client.describe_job_definitions(jobDefinitionName=job_definition_name, status='ACTIVE')
    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        raise RuntimeError(
            'Failed to get job definition revisions for {}: HTTP Status {}'.format(job_definition_name, response['ResponseMetadata']['HTTPStatusCode'])
        )
    job_definitions = response['jobDefinitions']
    revision = 0
    job_def = None
    for j in job_definitions:
        if j['jobDefinitionName'] != job_definition_name:
            continue
        if j['revision'] > revision:
            revision = j['revision']
            job_def = j

    return job_def


def batch_extract_job_definition_fqn(job_def):
    revision = job_def['revision']
    name = job_def['jobDefinitionName']
    return '{}:{}'.format(name, revision)


def batch_get_job_definition(job_definition_name):
    """Get the most recent active revision number for a AWS Batch job
    definition

    Args:
        job_definition_name: The name of the job definition

    Return:
        The fully-qualified job definition name with revision number, or
            `None` if the job definition does not exist
    """
    job_def = batch_get_latest_job_definition(job_definition_name)

    if job_def is None:
        return None
    else:
        return batch_extract_job_definition_fqn(job_def)


def batch_register_job_definition(job_definition_name, remote_pipeline_image_name,
                                  vcpus=1, memory=2000, job_role_arn=None):
    """Register a new AWS Batch job definition.

    Args:
        job_definition_name: The name of the job definition
        remote_pipeline_image_name: The ECR Docker image to load to run jobs
            using this definition
        vcpus: The number of vCPUs to use to run jobs using this definition
        memory: The amount of memory in MiB to use to run jobs using this
            definition
        job_role_arn (str): Can be None
    """

    container_properties = {
        'image': remote_pipeline_image_name,
        'vcpus': vcpus,
        'memory': memory,
    }

    if job_role_arn is not None:
        container_properties['jobRoleArn'] = job_role_arn

    region = profile_get_region()
    client = b3.client('batch', region_name=region)
    response = client.register_job_definition(
        jobDefinitionName=job_definition_name,
        type='container',
        containerProperties=container_properties
    )
    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        raise RuntimeError('Failed to create job definition {}: HTTP Status {}'.format(job_definition_name, response['ResponseMetadata']['HTTPStatusCode']))

    return response


def ecr_create_fq_respository_name(repository_name, policy_resource_package=None, policy_resource_name=None):
    ecr_client = b3.client('ecr', region_name=profile_get_region())
    # Create or fetch the repository in AWS (to store the image)
    try:
        response = ecr_client.create_repository(
            repositoryName=repository_name
        )
        repository_metadata = response['repository']
        # Set the policy on the repository
        if policy_resource_package is not None and policy_resource_name is not None:
            policy = pkg_resources.resource_string(policy_resource_package.__name__, policy_resource_name)
            _ = ecr_client.set_repository_policy(
                registryId=repository_metadata['registryId'],
                repositoryName=repository_name,
                policyText=policy,
                force=True
            )
    except ClientError as e:
        if e.response['Error']['Code'] == 'RepositoryAlreadyExistsException':
            response = ecr_client.describe_repositories(
                repositoryNames=[repository_name]
            )
            repository_metadata = response['repositories'][0]
        elif e.response['Error']['Code'] == 'AccessDeniedException':
            _logger.warn("Error [AccessDeniedException] when creating repo {}, trying to continue...".format(repository_name))
        else:
            raise e
    return repository_metadata['repositoryUri']


def ecr_get_fq_repository_name(repository_name):
    return ecr_create_fq_respository_name(repository_name)


def ecr_get_auth_config():
    ecr_client = b3.client('ecr', region_name=profile_get_region())
    # Authorize docker to push to ECR
    response = ecr_client.get_authorization_token()
    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        raise RuntimeError('Failed to get AWS ECR authorization token: HTTP Status {}'.format(response['ResponseMetadata']['HTTPStatusCode']))
    token = response['authorizationData'][0]['authorizationToken']

    token_bytes = six.b(token)

    token_decoded_bytes = base64.b64decode(token_bytes)

    token_decoded_str = token_decoded_bytes.decode('utf8')

    username, password = token_decoded_str.split(':')

    return {'username': username, 'password': password}
