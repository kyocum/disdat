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
import os
import pkg_resources
from getpass import getuser
import six
from multiprocessing import get_context

from botocore.exceptions import ClientError
import boto3 as b3
from six.moves import urllib

from disdat import logger as _logger

S3_LS_USE_MP_THRESH = 4000  # the threshold after which we should use MP to look up bundles on s3


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


def profile_get_region():
    """Gets the AWS region for the current AWS profile. If AWS_DEFAULT_REGION is set in env will just default to use
    that.
    """

    # ENV variables take precedence over the region in the ~/.aws/ folder
    if 'AWS_DEFAULT_REGION' in os.environ:
        region = os.environ['AWS_DEFAULT_REGION']
    else:
        session = b3.session.Session()
        profile = session.profile_name
        region = session.region_name
        if 'AWS_PROFILE' in os.environ:
            assert os.environ['AWS_PROFILE'] == profile, "Boto session profile != env AWS_PROFILE"

    return region


def s3_path_exists(s3_url):
    """
    Given an entire path, does the key exist?

    If you're checking for partial key, make sure to end with '/'

    This is how you make "folders" in s3, you use a key ending with '/'
    e.g., s3://mybucket/onelevel/anotherdir/
    bucket = mybucket
    key = onelevel/anotherdir/ -- it's a zero size object.

    If checking for full path, you can end with thing itself.

    Args:
        s3_url:

    Returns:

    """
    import botocore

    s3 = b3.resource('s3')
    bucket, key = split_s3_url(s3_url)
    if key is None:
        return s3_bucket_exists(bucket)
    exists = True
    try:
        s3.Object(bucket, key).load()
    except botocore.exceptions.ClientError as e:
        error_code = int(e.response['Error']['Code'])
        _logger.info("Error code {}".format(error_code))
        if error_code == 404:
            exists = False
        else:
            raise

    return exists


def s3_bucket_exists(bucket):
    """
    Code from Amazon docs for checking bucket existence.

    Args:
        bucket:

    Returns:
        booL: whether bucket exists

    """
    import botocore

    s3 = b3.resource('s3')
    exists = True
    try:
        s3.meta.client.head_bucket(Bucket=bucket)
    except botocore.exceptions.ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            exists = False
        elif error_code == 403:
            # for buckets you can get a forbidden instead of resource not found
            # if you have the s3:ListBucket permission on the bucket, Amazon S3 will return a
            # HTTP status code 404 ("no such key") error. If you don't have the s3:ListBucket permission,
            # Amazon S3 will return a HTTP status code 403 ("access denied") error.
            _logger.info("aws_s3: bucket {} raised a 403 (access forbidden), do you have ListBucket permission?".format(bucket))
            exists = False
        else:
            raise
    return exists


def s3_list_objects_at_prefix(bucket, prefix):
    """ List out the objects at this prefix.
    Returns a list of the keys found at this bucket.
    We do so because boto objects aren't serializable under multiprocessing

    Note: Do *not* use with multi-processing. This version uses boto Collections.
    That means all the filtering is done on the client side, which makes this a
    bad choice for multiprocessing as all the work is done for each call.

    Args:
        bucket(str): The s3 bucket
        prefix(str): The s3 key prefix you wish to search

    Returns:
        (list): List of item keys
    """
    s3 = b3.resource('s3')
    result = []
    try:
        s3_b = s3.Bucket(bucket)
        for i in s3_b.objects.filter(Prefix=prefix, MaxKeys=1024):
            result.append(i)
    except Exception as e:
        _logger.error("s3_list_objects_starting_hex: failed with exception {}".format(e))
        raise
    return result


def s3_list_objects_at_prefix_v2(bucket, prefix):
    """ List out the objects at this prefix
    Returns a list of the keys found at this bucket.
    We do so because boto objects aren't serializable under multiprocessing

    Note: Use v2 for multi-processing, since this filters on the server side!

    Args:
        bucket(str): The s3 bucket
        prefix(str): The s3 key prefix you wish to search

    Returns:
        (list): List of item keys
    """
    result = []
    client = b3.client('s3')

    #print(f"s3_list_objects_at_prefix_v2 the b3[{b3}] and client[{b3.client} and resource[{b3.resource}]")

    try:
        paginator = client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
        for page in page_iterator:
            if 'Contents' not in page:
                continue
            result += [obj['Key'] for obj in page['Contents']]
    except Exception as e:
        _logger.error("s3_list_objects_starting_hex: failed with exception {}".format(e))
        raise
    return result


def ls_s3_url_keys(s3_url, is_object_directory=False):
    """
    List all keys at this s3_url.  If `is_object_directory` is True, then
    treat this `s3_url` as a Disdat object directory, consisting of uuids that start
    with one of 16 ascii characters.  This allows the ls operation to run in parallel.

    Note: We convert ObjectSummary's of each object into simply a string of the key.

    Args:
        s3_url(str): The bucket and key of the "directory" under which to search
        is_object_directory(bool): Search a disdat object directory in parallel. Only faster for +1k objects

    Returns:
        list (str): list of keys under the bucket in the s3_path
    """
    if s3_url[-1] is not '/':
        s3_url += '/'

    bucket, s3_path = split_s3_url(s3_url)

    hexes = '0123456789abcdef'

    MAX_WAIT = 12 * 60
    multiple_results = []

    # MacOS X fails when we multi-process using fork and boto sessions.
    # One fix is to set export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
    mp_ctxt = get_context('forkserver')
    pool = mp_ctxt.Pool(processes=mp_ctxt.cpu_count()) # I/O bound, so let it use at least cpu_count()

    prefixes = ['']
    if is_object_directory:
        prefixes = [f'{c}{d}' for c in hexes for d in hexes]

    if is_object_directory:
        for i, prefix in enumerate(prefixes):
            prefix = os.path.join(s3_path, prefix)
            multiple_results.append(pool.apply_async(s3_list_objects_at_prefix_v2, (bucket, prefix)))
        pool.close()
        results = [item for res in multiple_results for item in res.get(timeout=MAX_WAIT)]
        pool.join()
    else:
        results = s3_list_objects_at_prefix_v2(bucket, s3_path)

    return results


def ls_s3_url_objects(s3_url):
    """
    List all objects under this s3_url.

    Note: If you want to get a specific boto s3 object for a key, use s3_list_objects_at_prefix directly.

    Note: the objects returned from this call cannot be subsequently passed into python multiprocessing
    because they cannot be serialized by default.

    Args:
        s3_url(str): The bucket and key of the "directory" under which to search

    Returns:
        list (str): list of s3 objects under the bucket in the s3_path
    """
    if s3_url[-1] is not '/':
        s3_url += '/'

    bucket, s3_path = split_s3_url(s3_url)

    return s3_list_objects_at_prefix(bucket, s3_path)


def ls_s3_url(s3_url):
    """

    Args:
        s3_url:

    Returns:
        list(dict)
    """

    bucket, s3_path = split_s3_url(s3_url)
    result = []
    client = b3.client('s3')
    paginator = client.get_paginator('list_objects_v2')
    # use delimiter to groupby, which means, list things only at this level.
    #page_iterator = paginator.paginate(Bucket=bucket, Delimiter='/', Prefix=s3_path)
    page_iterator = paginator.paginate(Bucket=bucket,Prefix=s3_path)
    for page in page_iterator:
        result += page['Contents']

    return result


def ls_s3_url_paths(s3_url):
    """
    Return path strings at this url

    Returns:
        (bool) : removed
    """

    return [os.path.join('s3://', r['Key']) for r in ls_s3_url(s3_url)]


def delete_s3_dir_many(s3_urls):
    """

    Args:
        s3_urls(list(str)): list of s3_urls we will remove

    Returns:
        number objects deleted (int)

    """
    MAX_WAIT = 12 * 60
    mp_ctxt = get_context('forkserver')  # Using forkserver here causes moto / pytest failures
    pool = mp_ctxt.Pool(processes=mp_ctxt.cpu_count())
    multiple_results = []
    for s3_url in s3_urls:
        multiple_results.append(pool.apply_async(delete_s3_dir, (s3_url,)))

    pool.close()
    results = [res.get(timeout=MAX_WAIT) for res in multiple_results]
    pool.join()
    return sum([1 if r > 0 else 0 for r in results])


def delete_s3_dir(s3_url):
    """

    Args:
        s3_url:

    Returns:
        number deleted (int)

    """
    s3 = b3.resource('s3')
    bucket, s3_path = split_s3_url(s3_url)

    bucket = s3.Bucket(bucket)
    objects_to_delete = []
    for obj in bucket.objects.filter(Prefix=s3_path):
        objects_to_delete.append({'Key': obj.key})
    if len(objects_to_delete) > 0:
        bucket.delete_objects(
            Delete={
                'Objects': objects_to_delete
            }
        )
    return len(objects_to_delete)


def cp_s3_file(s3_src_path, s3_root):
    """
    Copy an s3 file to an s3 location
    Keeps the original file name.
    Args:
        s3_src_path:
        s3_root:

    Returns:

    """
    s3 = b3.resource('s3')
    bucket, s3_path = split_s3_url(s3_root)
    filename = os.path.basename(s3_src_path)
    output_path = os.path.join(s3_path, filename)

    src_bucket, src_key = split_s3_url(s3_src_path)
    # print "Trying to copy from bucket {} key {} to bucket {} key {}".format(src_bucket, src_key, bucket, output_path)

    s3.Object(bucket, output_path).copy_from(CopySource={'Bucket': src_bucket, 'Key': src_key}, ServerSideEncryption="AES256")
    return os.path.join("s3://", bucket, output_path)


def put_s3_file(local_path, s3_root):
    """
    Put local file to location at s3_root.
    Keeps original file name.
    Args:
        local_path:
        s3_root:

    Returns:

    """
    s3 = b3.resource('s3')
    bucket, s3_path = split_s3_url(s3_root)
    filename = os.path.basename(local_path)
    s3.Object(bucket, os.path.join(s3_path, filename)).upload_file(local_path, ExtraArgs={"ServerSideEncryption": "AES256"})
    return filename


def get_s3_resource():
    return b3.resource('s3')


def get_s3_key(bucket, key, filename=None):
    """

    Args:
        bucket:
        key:
        file_name:
        s3: A boto3.resource('s3')

    Returns:

    """
    dl_retry = 3
    s3 = b3.resource('s3')
    #print(f"get_s3_key the b3[{b3}] and client[{b3.client} and resource[{b3.resource}]")

    if filename is None:
        filename = os.path.basename(key)
    else:
        path = os.path.dirname(filename)
        if not os.path.exists(path):
            try:
                os.makedirs(path)
            except os.error as ose:
                # swallow error -- likely directory already exists from other process
                _logger.debug("aws_s3.get_s3_key: Error code {}".format(os.strerror(ose.errno)))

    while dl_retry > 0:
        try:
            s3.Bucket(bucket).download_file(key, filename)
            dl_retry = -1
        except Exception as e:
            _logger.warn("aws_s3.get_s3_key Retry Count [{}] on download_file raised exception {}".format(dl_retry, e))
            dl_retry -= 1
            if dl_retry <= 0:
                _logger.warn("aws_s3.get_s3_key Fail on downloading file after 3 retries with exception {}".format(e))
                raise

    #print "PID({}) STOP bkt[] key[{}] file[{}]".format(multiprocessing.current_process(),key,filename)

    return filename


def get_s3_key_many(bucket_key_file_tuples):
    """ Retrieve many s3 keys in parallel and copy to the filename

    This was done primarily because when testing from an external module, moto
    fails to stub out calls to aws clients/resources if the multiprocessing occurs
    outside of the module doing the s3 calls.

    Args:
        bucket_key_file_tuples (tuple): (bucket:str, key:str, filename=None)

    Returns:
        filenames (list): list of filenames

    """
    MAX_WAIT = 12 * 60
    mp_ctxt = get_context('fork')  # Using forkserver here causes moto / pytest failures
    pool = mp_ctxt.Pool(processes=mp_ctxt.cpu_count())
    multiple_results = []
    for s3_bucket, s3_key, local_object_path in bucket_key_file_tuples:
        multiple_results.append(pool.apply_async(get_s3_key,
                                                 (s3_bucket, s3_key, local_object_path)))

    pool.close()
    results = [res.get(timeout=MAX_WAIT) for res in multiple_results]
    pool.join()
    return results


def get_s3_file(s3_url, filename=None):
    bucket, s3_path = split_s3_url(s3_url)

    return get_s3_key(bucket, s3_path, filename)


def split_s3_url(s3_url):
    """
    Return bucket, path

    Args:
        s3_url:

    Returns:
        (str, str)

    """
    s3_schemes = ["s3n", "s3"]
    url = urllib.parse.urlparse(s3_url)
    if url.scheme not in s3_schemes:
        raise ValueError('Got an invalid URL scheme: Expected {}, got "{}" from "{}"'.format(' or '.join(s3_schemes), url.scheme, url.geturl()))
    bucket = url.hostname
    if bucket is None:
        raise ValueError('Got an empty S3 bucket (too many "/"s starting "{}"?)'.format(url.geturl()))
    key = url.path.lstrip('/')
    if len(key) == 0:
        key = None
    return bucket, key
