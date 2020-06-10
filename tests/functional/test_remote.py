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

import boto3
import moto
import pytest

from disdat.pipe import PipeTask
import disdat.api as api
from tests.functional.common import run_test, TEST_CONTEXT

TEST_REMOTE = '__test_remote_context__'
TEST_BUCKET = 'test-bucket'
TEST_BUCKET_URL = "s3://{}".format(TEST_BUCKET)


class RemoteTest(PipeTask):
    def pipe_requires(self, pipeline_input=None):
        self.set_bundle_name('remote_test')

    def pipe_run(self, pipeline_input=None):
        return 'Hello'


@moto.mock_s3
def test_push(run_test):
    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    s3_resource.create_bucket(Bucket=TEST_BUCKET)
    bucket = s3_resource.Bucket(TEST_BUCKET)

    objects = s3_client.list_objects(Bucket=TEST_BUCKET)
    assert 'Contents' not in objects, 'Bucket should be empty'

    assert len(api.search(TEST_CONTEXT)) == 0, 'Context should be empty'
    api.remote(TEST_CONTEXT, TEST_REMOTE, TEST_BUCKET_URL)

    api.apply(TEST_CONTEXT, RemoteTest)
    bundle = api.get(TEST_CONTEXT, 'remote_test')

    assert bundle.data == 'Hello'

    bundle.commit()
    bundle.push()

    objects = s3_client.list_objects(Bucket=TEST_BUCKET)
    assert 'Contents' in objects, 'Bucket should not be empty'
    assert len(objects['Contents']) > 0, 'Bucket should not be empty'

    bucket.objects.all().delete()
    bucket.delete()


@moto.mock_s3
def test_pull(run_test):
    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    s3_resource.create_bucket(Bucket=TEST_BUCKET)
    bucket = s3_resource.Bucket(TEST_BUCKET)

    objects = s3_client.list_objects(Bucket=TEST_BUCKET)
    assert 'Contents' not in objects, 'Bucket should be empty'

    assert len(api.search(TEST_CONTEXT)) == 0, 'Context should be empty'
    api.remote(TEST_CONTEXT, TEST_REMOTE, TEST_BUCKET_URL)

    api.apply(TEST_CONTEXT, RemoteTest)
    bundle = api.get(TEST_CONTEXT, 'remote_test')

    assert bundle.data == 'Hello'

    bundle.commit()
    bundle.push()

    objects = s3_client.list_objects(Bucket=TEST_BUCKET)
    assert 'Contents' in objects, 'Bucket should not be empty'
    assert len(objects['Contents']) > 0, 'Bucket should not be empty'

    api.delete_context(context_name=TEST_CONTEXT)
    api.context(context_name=TEST_CONTEXT)
    api.remote(TEST_CONTEXT, TEST_REMOTE, TEST_BUCKET_URL)
    api.pull(TEST_CONTEXT)

    pulled_bundles = api.search(TEST_CONTEXT)
    assert len(pulled_bundles) > 0, 'Pulled bundles down'
    assert pulled_bundles[0].data == 'Hello', 'Bundle contains correct data'

    bucket.objects.all().delete()
    bucket.delete()


if __name__ == '__main__':
    pytest.main([__file__])
