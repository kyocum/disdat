from __future__ import print_function
#
# Copyright 2017 Human Longevity, Inc.
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

from tests.functional.common import run_test, TEST_CONTEXT
import tempfile
import disdat.api as api
from disdat.utility import aws_s3
import pytest
import moto
import hashlib
import boto3
import os

TEST_BUNDLE = "test.bundle"
TEST_REMOTE = '__test_remote_context__'
TEST_BUCKET = 'test-bucket'
TEST_BUCKET_URL = "s3://{}".format(TEST_BUCKET)


# Setup moto s3 resources

# Make sure bucket is empty
#objects = s3_client.list_objects(Bucket=TEST_BUCKET)
#assert 'Contents' not in objects, 'Bucket should be empty'


def md5_file(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def test_local_file(run_test):
    """ Test copying in local file """

    local_fp = tempfile.NamedTemporaryFile()
    local_fp.write(b'an external local file')
    local_fp.flush()

    with api.Bundle(TEST_CONTEXT, name=TEST_BUNDLE) as b:
        b.add_data(local_fp.name)
        b.add_tags({'info':'added a local file'})

    saved_uuid = b.uuid
    saved_md5 = md5_file(local_fp.name)
    local_fp.close()

    b = api.get(TEST_CONTEXT, None, uuid=saved_uuid)
    assert md5_file(b.data) == saved_md5


def test_zero_copy_local_file(run_test):
    """ Test managed path of a local file """

    with api.Bundle(TEST_CONTEXT, name=TEST_BUNDLE) as b:
        f1 = b.get_file("file_1.txt")
        f2 = b.get_file("file_2.txt")
        with f1.open(mode='w') as f:
            f.write("This is our first file!")
        with f2.open(mode='w') as f:
            f.write("This is our second file!")
        b.add_data([f1,f2])
        b.add_params({'type':'file'})

    saved_uuid = b.uuid
    saved_f1_md5 = md5_file(f1.path)
    saved_f2_md5 = md5_file(f2.path)

    b = api.get(TEST_CONTEXT, None, uuid=saved_uuid)
    assert md5_file(b.data[0]) == saved_f1_md5
    assert md5_file(b.data[1]) == saved_f2_md5


@moto.mock_s3
def test_copy_in_s3_file(run_test):
    """ Test copying in s3 file
    The file should be copied into the local context
    """

    s3_resource = boto3.resource('s3')
    s3_resource.create_bucket(Bucket=TEST_BUCKET)

    # Copy a local file to moto s3 bucket
    saved_md5 = md5_file(__file__)
    aws_s3.put_s3_file(__file__, TEST_BUCKET_URL)

    s3_file = os.path.join(TEST_BUCKET_URL, os.path.basename(__file__))

    with api.Bundle(TEST_CONTEXT, name=TEST_BUNDLE) as b:
        b.add_data(s3_file)
        b.add_tags({'info': 'added an s3 file'})
    saved_uuid = b.uuid

    b = api.get(TEST_CONTEXT, None, uuid=saved_uuid)
    md5 = md5_file(b.data)
    print(md5)
    print(saved_md5)
    assert md5 == saved_md5


@moto.mock_s3
def test_copy_in_s3_file_with_remote(run_test):
    """ Test copying in s3 file
    The file should be copied into the remote context
    """

    s3_resource = boto3.resource('s3')
    s3_resource.create_bucket(Bucket=TEST_BUCKET)

    api.remote(TEST_CONTEXT, TEST_REMOTE, TEST_BUCKET_URL)

    # Copy a local file to moto s3 bucket
    saved_md5 = md5_file(__file__)
    aws_s3.put_s3_file(__file__, TEST_BUCKET_URL)

    s3_file = os.path.join(TEST_BUCKET_URL, os.path.basename(__file__))

    with api.Bundle(TEST_CONTEXT, name=TEST_BUNDLE) as b:
        b.add_data(s3_file)
        b.add_tags({'info': 'added an s3 file'})
    saved_uuid = b.uuid

    b = api.get(TEST_CONTEXT, None, uuid=saved_uuid)
    assert b.data.startswith("s3://")


@moto.mock_s3
def test_zero_copy_s3_file(run_test):
    """ Test managed path in local file """
    s3_resource = boto3.resource('s3')
    s3_resource.create_bucket(Bucket=TEST_BUCKET)

    api.remote(TEST_CONTEXT, TEST_REMOTE, TEST_BUCKET_URL)

    saved_md5 = md5_file(__file__)

    with api.Bundle(TEST_CONTEXT, name=TEST_BUNDLE) as b:
        s3_target = b.get_remote_file('test_s3_file.txt')
        aws_s3.cp_local_to_s3_file(__file__, s3_target.path)
        b.add_data(s3_target)
        b.add_tags({'info': 'added an s3 file'})
    saved_uuid = b.uuid

    b = api.get(TEST_CONTEXT, None, uuid=saved_uuid)
    b.pull(localize=True)
    md5 = md5_file(b.data)
    print(md5)
    print(saved_md5)
    assert md5 == saved_md5


if __name__ == "__main__":
    pytest.main([__file__])

