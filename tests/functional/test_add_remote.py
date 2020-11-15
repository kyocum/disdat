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
#
"""
Test add remote

Use the API to add a remote

"""
import boto3
import moto
import pytest

import disdat.api as api
from tests.functional.common import TEST_CONTEXT


TEST_REMOTE = '__test_remote_context__'
TEST_BUCKET = 'test-bucket'
TEST_BUCKET_URL = "s3://{}".format(TEST_BUCKET)
TEST_BUCKET_KEY_URL = "s3://{}/somekey".format(TEST_BUCKET)


@moto.mock_s3
def test_add_remote():
    api.delete_context(TEST_CONTEXT)
    api.context(context_name=TEST_CONTEXT)

    # Setup moto s3 resources
    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    s3_resource.create_bucket(Bucket=TEST_BUCKET)

    # Make sure bucket is empty
    objects = s3_client.list_objects(Bucket=TEST_BUCKET)
    assert 'Contents' not in objects, 'Bucket should be empty'

    # Bind remote context with just bucket
    api.remote(TEST_CONTEXT, TEST_REMOTE, TEST_BUCKET_URL)

    with api.Bundle(TEST_CONTEXT) as b:
        b.name = 'output'
        b.add_data([1, 3, 5])

    b.commit()
    b.push()

    # Bind remote to new context with bucket and key
    api.remote(TEST_CONTEXT, TEST_REMOTE, TEST_BUCKET_KEY_URL)

    with api.Bundle(TEST_CONTEXT) as b:
        b.name = 'output'
        b.add_data([1, 3, 5])

    b.commit()
    b.push()

    api.delete_context(TEST_CONTEXT)


@moto.mock_s3
def test_add_remote_fail():
    error = None
    api.delete_context(TEST_CONTEXT)
    api.context(context_name=TEST_CONTEXT)

    # Setup moto s3 resources
    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')

    # Bind remote context with just bucket
    try:
        api.remote(TEST_CONTEXT, TEST_REMOTE, TEST_BUCKET_URL)
    except Exception as e:
        error = e
    finally:
        assert(type(error) == RuntimeError)

    # Bind remote to new context with bucket and key
    try:
        api.remote(TEST_CONTEXT, TEST_REMOTE, TEST_BUCKET_KEY_URL)
    except Exception as e:
        error = e
    finally:
        assert(type(error) == RuntimeError)

    api.delete_context(TEST_CONTEXT)


if __name__ == '__main__':
    pytest.main([__file__])
