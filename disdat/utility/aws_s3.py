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

import concurrent.futures as futures
import os
import urllib

import boto3 as b3

from disdat import log
from disdat import logger as _logger

S3_LS_USE_MP_THRESH = (
    2000  # the threshold after which we should use MP to look up bundles on s3
)

# Note: one client for all threads.
# You can use a client across multiple threads, but allocating the client in multiple threads
# races on the shared default session in which they are created.
# And you don't want to spend the time to create individual sessions for each thread.
# https://medium.com/@life-is-short-so-enjoy-it/aws-boto3-misunderstanding-about-thread-safe-a7261d7391fd


# Helper module-level access function to make sure resource is initialized
def get_s3_resource():
    if get_s3_resource._s3_resource is None:
        get_s3_resource._s3_resource = b3.resource("s3")
    return get_s3_resource._s3_resource


# Module global variable used to initialize and store process state for s3 resource
get_s3_resource._s3_resource = None


# Helper module-level access function to make sure client is initialized
def get_s3_client():
    if get_s3_client._s3_client is None:
        get_s3_client._s3_client = b3.client("s3")
    return get_s3_client._s3_client


# Module global variable used to initialize and store process state for s3 client
get_s3_client._s3_client = None


def disdat_cpu_count():
    """
    The current version of the aws_s3 module (here) is based on multi-threading, not multi-processing.
    Thus the concerns below do not apply.  However, I'm leaving the comment in the event that we wish to re-visit mp:
        Python retrieves the physical cpu count when running within a container (as of 7-2020).
        When we use multiprocessing (forkserver) this is memory intensive, possibly creating more workers than
        vCPUs.  To help avoid OOM errors, we support an environment variable called DISDAT_CPU_COUNT to be set.
        For example in `dsdt.run`, we set this to be equal to the requested vcpu count.
    Note that we still support the environment variable, which can be useful for tests (moto does not support multithreading).

    Finally, for multithreading, we scale up to 10 threads per available core.

    Returns:
        int: the number of available cpus

    """
    env_cpu_count = os.getenv("DISDAT_CPU_COUNT")
    if env_cpu_count is None:
        return os.cpu_count() * 2  # magic number of threads = 2 * core count
    else:
        return int(env_cpu_count)  # just use the number in the env variable.


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

    s3 = get_s3_resource()
    bucket, key = split_s3_url(s3_url)
    if key is None:
        return s3_bucket_exists(bucket)

    try:
        s3.Object(bucket, key).load()
    except botocore.exceptions.ClientError as e:
        error_code = int(e.response["Error"]["Code"])
        _logger.info("Error code {}".format(error_code))
        if error_code == 404:
            return False
        else:
            raise

    return True


def s3_bucket_exists(bucket):
    """
    Code from Amazon docs for checking bucket existence.

    Args:
        bucket:

    Returns:
        bool: whether bucket exists

    """
    import botocore

    s3 = get_s3_resource()
    exists = True
    try:
        s3.meta.client.head_bucket(Bucket=bucket)
    except botocore.exceptions.ClientError as e:
        error_code = int(e.response["Error"]["Code"])
        if error_code == 404:
            exists = False
        elif error_code == 403:
            # for buckets you can get a forbidden instead of resource not found
            # if you have the s3:ListBucket permission on the bucket, Amazon S3 will return a
            # HTTP status code 404 ("no such key") error. If you don't have the s3:ListBucket permission,
            # Amazon S3 will return a HTTP status code 403 ("access denied") error.
            _logger.info(
                "aws_s3: bucket {} raised a 403 (access forbidden), do you have ListBucket permission?".format(
                    bucket
                )
            )
            exists = False
        else:
            raise
    return exists


def s3_list_objects_at_prefix(bucket, prefix):
    """List out the objects at this prefix.
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
    s3 = get_s3_resource()
    result = []
    try:
        s3_b = s3.Bucket(bucket)
        for i in s3_b.objects.filter(Prefix=prefix, MaxKeys=1024):
            result.append(i)
    except Exception as e:
        _logger.error(
            "s3_list_objects_starting_hex: failed with exception {}".format(e)
        )
        raise
    return result


def _s3_list_objects_at_prefix_v2(client, bucket, prefix):
    """List out the objects at this prefix
    Returns a list of the keys found at this bucket.
    We do so because boto objects aren't serializable under multiprocessing

    Note: Use v2 for multi-processing, since this filters on the server side!

    Args:
        client(boto3.client): the boto3 s3 client
        bucket(str): The s3 bucket
        prefix(str): The s3 key prefix you wish to search

    Returns:
        (list): List of item keys
    """
    result = []
    try:
        paginator = client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
        for page in page_iterator:
            if "Contents" not in page:
                continue
            result += [obj["Key"] for obj in page["Contents"]]
    except Exception as e:
        _logger.error(
            "s3_list_objects_starting_hex: failed with exception {}".format(e)
        )
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
    if s3_url[-1] != "/":
        s3_url += "/"

    bucket, s3_path = split_s3_url(s3_url)
    if s3_path is None:
        s3_path = ""

    hexes = "0123456789abcdef"

    results = []

    client = get_s3_client()

    if is_object_directory:
        prefixes = [f"{c}{d}" for c in hexes for d in hexes]
        est_cpu_count = disdat_cpu_count()
        _logger.debug("ls_s3_url_keys using MP with cpu_count {}".format(est_cpu_count))

        with futures.ThreadPoolExecutor(max_workers=disdat_cpu_count()) as pool:
            result_futures = []
            for i, prefix in enumerate(prefixes):
                prefix = os.path.join(s3_path, prefix)
                result_futures.append(
                    pool.submit(_s3_list_objects_at_prefix_v2, client, bucket, prefix)
                )
            results = [
                r for f in futures.as_completed(result_futures) for r in f.result()
            ]

    else:
        results = _s3_list_objects_at_prefix_v2(client, bucket, s3_path)

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
    if s3_url[-1] != "/":
        s3_url += "/"

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
    client = get_s3_client()
    paginator = client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=s3_path)
    for page in page_iterator:
        result += page["Contents"]

    return result


def ls_s3_url_paths(s3_url):
    """
    Return path strings at this url

    Returns:
        (bool) : removed
    """

    return [os.path.join("s3://", r["Key"]) for r in ls_s3_url(s3_url)]


def delete_s3_dir_many(s3_urls):
    """

    Args:
        s3_urls(list(str)): list of s3_urls we will remove

    Returns:
        number objects deleted (int)

    """
    client = get_s3_resource()

    # The URLs may be "directories" or objects.
    # if s3_url in to_delete:

    with futures.ThreadPoolExecutor(max_workers=disdat_cpu_count()) as pool:
        result_futures = []
        for s3_url in s3_urls:
            result_futures.append(pool.submit(_delete_s3_dir, client, s3_url))
        results = [f.result() for f in futures.as_completed(result_futures)]

    return sum([1 if r > 0 else 0 for r in results])


def delete_s3_dir(s3_url):
    """

    Args:
        s3_url(str): the directory to delete

    Returns:
        number deleted (int)

    """
    return _delete_s3_dir(get_s3_resource(), s3_url)


def _delete_s3_dir(s3_resource, s3_url):
    """
    Args:
        resource(boto3.resource): The boto3 s3 resource
        s3_url(str): the directory to delete
    Returns:
        number deleted (int)
    """

    bucket, s3_path = split_s3_url(s3_url)

    bucket = s3_resource.Bucket(bucket)
    objects_to_delete = []
    for obj in bucket.objects.filter(Prefix=s3_path):
        objects_to_delete.append({"Key": obj.key})
    if len(objects_to_delete) > 0:
        try:
            bucket.delete_objects(Delete={"Objects": objects_to_delete})
        except Exception as e:
            print(e)
            print(f"FAILED DELETING OBJECTS: {objects_to_delete}")

    # print(f"Deleted {len(objects_to_delete)} objects at prefix {s3_path}")
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
    s3 = get_s3_resource()
    bucket, s3_path = split_s3_url(s3_root)
    filename = os.path.basename(s3_src_path)
    output_path = os.path.join(s3_path, filename)

    src_bucket, src_key = split_s3_url(s3_src_path)

    s3.Object(bucket, output_path).copy_from(
        CopySource={"Bucket": src_bucket, "Key": src_key},
        ACL="bucket-owner-full-control",
        ServerSideEncryption="AES256",
    )
    return os.path.join("s3://", bucket, output_path)


def cp_local_to_s3_file(local_file, s3_file):
    """
    Put fqp local file to fqp s3_file

    Args:
        local_file (str): Fully qualified path to local file
        s3_file (str): Fully qualified path to file on s3

    Returns:
        s3_file
    """
    s3 = get_s3_client()
    return _cp_local_to_s3_file(s3, local_file, s3_file)


def _cp_local_to_s3_file(s3_client, local_file, s3_file):
    bucket, s3_path = split_s3_url(s3_file)
    local_file = urllib.parse.urlparse(local_file).path
    s3_client.upload_file(
        local_file,
        bucket,
        s3_path,
        ExtraArgs={
            "ServerSideEncryption": "AES256",
            "ACL": "bucket-owner-full-control",
        },
    )
    return s3_file


def put_s3_file(local_path, s3_root):
    """
    Put local file to location at s3_root.
    Keeps original file name.
    Args:
        local_path:
        s3_root:

    Returns:

    """
    s3 = get_s3_resource()
    bucket, s3_path = split_s3_url(s3_root)
    if s3_path is None:
        s3_path = ""
    filename = os.path.basename(local_path)
    s3.Object(bucket, os.path.join(s3_path, filename)).upload_file(
        local_path,
        ExtraArgs={
            "ServerSideEncryption": "AES256",
            "ACL": "bucket-owner-full-control",
        },
    )
    return os.path.join(s3_root, filename)


def put_s3_key_many(bucket_key_file_tuples):
    """Push many s3 keys in parallel from filename

    Like get_s3_key_many, this was done primarily because when testing from an external module, moto
    fails to stub out calls to aws clients/resources if the multiprocessing occurs
    outside of the module doing the s3 calls.

    Args:
        bucket_key_file_tuples (list[tuple]): (filename, s3_path)

    Returns:
        list: list of destination s3 paths

    """
    s3_client = get_s3_client()

    with futures.ThreadPoolExecutor(max_workers=disdat_cpu_count()) as pool:
        result_futures = []
        for local_object_path, s3_path in bucket_key_file_tuples:
            result_futures.append(
                pool.submit(_cp_local_to_s3_file, s3_client, local_object_path, s3_path)
            )
        results = [f.result() for f in futures.as_completed(result_futures)]

    return results


def get_s3_key(bucket, key, filename=None):
    """

    Args:
        bucket:
        key:
        file_name:

    Returns:

    """
    s3 = get_s3_resource()
    return _get_s3_key(s3, bucket, key, filename)


def _get_s3_key(s3_resource, bucket, key, filename=None):
    dl_retry = 3
    if filename is None:
        filename = os.path.basename(key)
    else:
        path = os.path.dirname(filename)
        if not os.path.exists(path):
            try:
                os.makedirs(path)
            except os.error as ose:
                # swallow error -- likely directory already exists from other process
                _logger.debug(
                    "aws_s3.get_s3_key: Error code {}".format(os.strerror(ose.errno))
                )

    while dl_retry > 0:
        try:
            s3_resource.Bucket(bucket).download_file(key, filename)
            dl_retry = -1
        except Exception as e:
            _logger.warning(
                "aws_s3.get_s3_key Retry Count [{}] on download_file raised exception {}".format(
                    dl_retry, e
                )
            )
            dl_retry -= 1
            if dl_retry <= 0:
                _logger.warning(
                    "aws_s3.get_s3_key Fail on downloading file after 3 retries with exception {}".format(
                        e
                    )
                )
                raise

    return filename


def get_s3_key_many(bucket_key_file_tuples):
    """Retrieve many s3 keys in parallel and copy to the filename

    This was done primarily because when testing from an external module, moto
    fails to stub out calls to aws clients/resources if the multiprocessing occurs
    outside of the module doing the s3 calls.

    Args:
        bucket_key_file_tuples (tuple): (bucket:str, key:str, filename=None)

    Returns:
        filenames (list): list of filenames

    """
    s3_resource = get_s3_resource()

    with futures.ThreadPoolExecutor(max_workers=disdat_cpu_count()) as pool:
        result_futures = []
        for s3_bucket, s3_key, local_object_path in bucket_key_file_tuples:
            result_futures.append(
                pool.submit(
                    _get_s3_key, s3_resource, s3_bucket, s3_key, local_object_path
                )
            )
        results = [f.result() for f in futures.as_completed(result_futures)]

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
        raise ValueError(
            'Got an invalid URL scheme: Expected {}, got "{}" from "{}"'.format(
                " or ".join(s3_schemes), url.scheme, url.geturl()
            )
        )
    bucket = url.hostname
    if bucket is None:
        raise ValueError(
            'Got an empty S3 bucket (too many "/"s starting "{}"?)'.format(url.geturl())
        )
    key = url.path.lstrip("/")
    if len(key) == 0:
        key = None
    return bucket, key
