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
Test Incremental Push

Use API to create a bundle with some files
push to remote context

author: Sayantan Satpati
"""
import boto3
import moto
import pandas as pd
import pytest
import os

from disdat.pipe import PipeTask
import disdat.api as api
from tests.functional.common import run_test, TEST_CONTEXT

TEST_REMOTE = '__test_remote_context__'
TEST_BUCKET = 'test-bucket'
TEST_BUCKET_OTHER = 'test-bucket-other'
TEST_BUCKET_URL = "s3://{}".format(TEST_BUCKET)

# ================================================== #
# ================ Disdat Tasks ==================== #
# ================================================== #


class NonManagedLocal(PipeTask):
    def pipe_requires(self):
        self.set_bundle_name('b1')

    def pipe_run(self):
        # Task output was created on some local path
        d = {'col1': [1, 2], 'col2': [3, 4]}
        df = pd.DataFrame(data=d)
        local_file = '/tmp/test.parquet'
        df.to_parquet(local_file)
        return {'file': [local_file]}


class NonManagedS3(PipeTask):
    def pipe_requires(self):
        self.set_bundle_name('b2')

    def pipe_run(self):
        # Task output was created on some S3 path
        d = {'col1': [1, 2], 'col2': [3, 4]}
        df = pd.DataFrame(data=d)
        s3_file = 's3://{}/test.parquet'.format(TEST_BUCKET_OTHER)
        df.to_parquet(s3_file)

        return {'file': [s3_file]}


class ManagedLocal(PipeTask):
    def pipe_requires(self):
        self.set_bundle_name('b3')

    def pipe_run(self):
        target = self.create_output_file('test.parquet')

        # Write dataframe to S3 Managed Path
        d = {'col1': [1, 2], 'col2': [3, 4]}
        df = pd.DataFrame(data=d)
        with target.temporary_path() as temp_output_path:
            df.to_parquet(temp_output_path)

        return {'file': [target.path]}


class ManagedS3(PipeTask):
    def pipe_requires(self):
        self.set_bundle_name('b4')

    def pipe_run(self):
        target = self.create_remote_output_file('test.parquet')

        # Write dataframe to S3 Managed Path
        d = {'col1': [1, 2], 'col2': [3, 4]}
        df = pd.DataFrame(data=d)
        with target.temporary_path() as temp_output_path:
            df.to_parquet(temp_output_path)

        return {'file': [target.path]}


# ================================================== #
# ================== Tests ========================= #
# ================================================== #


def test_managed_local():
    api.delete_context(TEST_CONTEXT)
    api.context(context_name=TEST_CONTEXT)

    assert len(api.search(TEST_CONTEXT)) == 0, 'Context should be empty'

    api.apply(TEST_CONTEXT, ManagedLocal)
    assert len(api.search(TEST_CONTEXT)) == 1, 'Only one bundle should be present'
    print(api.cat(TEST_CONTEXT, 'b3'))

    assert os.path.exists(api.search(TEST_CONTEXT, human_name='b3')[0].data['file'][0]), \
        'Local file should be present in bundle'


def test_non_managed_local():
    api.delete_context(TEST_CONTEXT)
    api.context(context_name=TEST_CONTEXT)

    assert len(api.search(TEST_CONTEXT)) == 0, 'Context should be empty'

    api.apply(TEST_CONTEXT, NonManagedLocal)
    assert len(api.search(TEST_CONTEXT)) == 1, 'Only one bundle should be present'
    print(api.cat(TEST_CONTEXT, 'b1'))

    assert os.path.exists(api.search(TEST_CONTEXT, human_name='b1')[0].data['file'][0]), \
        'Local file should be present in bundle'


@moto.mock_s3
def test_remote_push_managed_s3():
    api.delete_context(TEST_CONTEXT)
    api.context(context_name=TEST_CONTEXT)

    # Setup moto s3 resources
    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    s3_resource.create_bucket(Bucket=TEST_BUCKET)

    # Make sure bucket is empty
    objects = s3_client.list_objects(Bucket=TEST_BUCKET)
    assert 'Contents' not in objects, 'Bucket should be empty'

    # Bind remote context
    api.remote(TEST_CONTEXT, TEST_REMOTE, TEST_BUCKET_URL)

    # Apply
    api.apply(TEST_CONTEXT, ManagedS3, incremental_push=True)

    assert not os.path.exists(api.search(TEST_CONTEXT, human_name='b4')[0].data['file'][0]), \
        'Managed S3 file should not be copied to local'

    # Get objects from remote
    objects = s3_client.list_objects(Bucket=TEST_BUCKET)
    keys = [o['Key'] for o in objects['Contents']]
    keys = [key.split('/')[-1] for key in keys]

    # Make sure files exist in S3
    for output_file in ['test.parquet']:
        assert output_file in keys, 'Pipeline should have pushed file'


@moto.mock_s3
def test_remote_push_non_managed_s3():
    api.delete_context(TEST_CONTEXT)
    api.context(context_name=TEST_CONTEXT)

    # Setup moto s3 resources
    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    s3_resource.create_bucket(Bucket=TEST_BUCKET)
    s3_resource.create_bucket(Bucket=TEST_BUCKET_OTHER)

    # Make sure bucket is empty
    objects = s3_client.list_objects(Bucket=TEST_BUCKET)
    assert 'Contents' not in objects, 'Bucket should be empty'
    objects = s3_client.list_objects(Bucket=TEST_BUCKET_OTHER)
    assert 'Contents' not in objects, 'Bucket should be empty'

    # Bind remote context
    api.remote(TEST_CONTEXT, TEST_REMOTE, TEST_BUCKET_URL)

    # Apply
    api.apply(TEST_CONTEXT, NonManagedS3, incremental_push=True)
    print(api.cat(TEST_CONTEXT, 'b2'))

    # Local context should not contain file if a remote exists.
    b = api.search(TEST_CONTEXT, human_name='b2')[0]
    assert not os.path.exists(b.data['file'][0]), 'Non Managed S3 file w/ remote should be copied to remote'
    b.pull(localize=True)
    assert os.path.exists(b.data['file'][0]), 'Non Managed S3 file after pull should be copied to local'

    # Get objects from remote
    objects = s3_client.list_objects(Bucket=TEST_BUCKET_OTHER)
    keys = [o['Key'] for o in objects['Contents']]
    keys = [key.split('/')[-1] for key in keys]

    # Make sure files exist in S3
    for output_file in ['test.parquet']:
        assert output_file in keys, 'Pipeline should have pushed file'


@moto.mock_s3
def test_remote_no_push_managed_s3():
    api.delete_context(TEST_CONTEXT)
    api.context(context_name=TEST_CONTEXT)

    # Setup moto s3 resources
    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    s3_resource.create_bucket(Bucket=TEST_BUCKET)

    # Make sure bucket is empty
    objects = s3_client.list_objects(Bucket=TEST_BUCKET)
    assert 'Contents' not in objects, 'Bucket should be empty'

    # Bind remote context
    api.remote(TEST_CONTEXT, TEST_REMOTE, TEST_BUCKET_URL)

    with pytest.raises(Exception) as e:
        api.apply(TEST_CONTEXT, ManagedS3)


@moto.mock_s3
def test_remote_no_push_non_managed_s3():
    api.delete_context(TEST_CONTEXT)
    api.context(context_name=TEST_CONTEXT)

    # Setup moto s3 resources
    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    s3_resource.create_bucket(Bucket=TEST_BUCKET)
    s3_resource.create_bucket(Bucket=TEST_BUCKET_OTHER)

    # Make sure bucket is empty
    objects = s3_client.list_objects(Bucket=TEST_BUCKET)
    assert 'Contents' not in objects, 'Bucket should be empty'
    objects = s3_client.list_objects(Bucket=TEST_BUCKET_OTHER)
    assert 'Contents' not in objects, 'Bucket should be empty'

    # Bind remote context
    api.remote(TEST_CONTEXT, TEST_REMOTE, TEST_BUCKET_URL)

    # Apply
    api.apply(TEST_CONTEXT, NonManagedS3)
    print(api.cat(TEST_CONTEXT, 'b2'))

    # Local context should not contain file if a remote exists.
    b = api.search(TEST_CONTEXT, human_name='b2')[0]
    assert not os.path.exists(b.data['file'][0]), 'Non Managed S3 file w/ remote should be copied to remote'
    assert b.data['file'][0].startswith("s3://")


def test_no_remote_push_managed_s3():
    api.delete_context(TEST_CONTEXT)
    api.context(context_name=TEST_CONTEXT)

    assert len(api.search(TEST_CONTEXT)) == 0, 'Context should be empty'

    with pytest.raises(Exception) as e:
        api.apply(TEST_CONTEXT, ManagedS3, incremental_push=True)


@moto.mock_s3
def test_no_remote_push_non_managed_s3():
    api.delete_context(TEST_CONTEXT)
    api.context(context_name=TEST_CONTEXT)

    # Setup moto s3 resources
    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    s3_resource.create_bucket(Bucket=TEST_BUCKET)
    s3_resource.create_bucket(Bucket=TEST_BUCKET_OTHER)

    # Make sure bucket is empty
    objects = s3_client.list_objects(Bucket=TEST_BUCKET)
    assert 'Contents' not in objects, 'Bucket should be empty'
    objects = s3_client.list_objects(Bucket=TEST_BUCKET_OTHER)
    assert 'Contents' not in objects, 'Bucket should be empty'

    api.apply(TEST_CONTEXT, NonManagedS3, incremental_push=True)
    print(api.cat(TEST_CONTEXT, 'b2'))
    assert len(api.search(TEST_CONTEXT)) == 1, 'One bundle should be present'

    assert os.path.exists(api.search(TEST_CONTEXT, human_name='b2')[0].data['file'][0]), \
        'Non Managed S3 file should be copied to local'


def test_no_remote_no_push_managed_s3():
    api.delete_context(TEST_CONTEXT)
    api.context(context_name=TEST_CONTEXT)

    assert len(api.search(TEST_CONTEXT)) == 0, 'Context should be empty'

    with pytest.raises(Exception) as e:
        api.apply(TEST_CONTEXT, ManagedS3)


@moto.mock_s3
def test_no_remote_no_push_non_managed_s3():
    api.delete_context(TEST_CONTEXT)
    api.context(context_name=TEST_CONTEXT)

    # Setup moto s3 resources
    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    s3_resource.create_bucket(Bucket=TEST_BUCKET)
    s3_resource.create_bucket(Bucket=TEST_BUCKET_OTHER)

    # Make sure bucket is empty
    objects = s3_client.list_objects(Bucket=TEST_BUCKET)
    assert 'Contents' not in objects, 'Bucket should be empty'
    objects = s3_client.list_objects(Bucket=TEST_BUCKET_OTHER)
    assert 'Contents' not in objects, 'Bucket should be empty'

    # Apply
    api.apply(TEST_CONTEXT, NonManagedS3)
    print(api.cat(TEST_CONTEXT, 'b2'))
    assert len(api.search(TEST_CONTEXT)) == 1, 'One bundle should be present'

    assert os.path.exists(api.search(TEST_CONTEXT, human_name='b2')[0].data['file'][0]), \
        'Non Managed S3 file should be copied to local'


if __name__ == '__main__':
    pytest.main([__file__])









