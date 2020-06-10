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
Test Incremental Pull

Use API to create a bundle with some files
push to remote context (test assumes that you have AWS credentials for an account.

author: Kenneth Yocum
"""
import boto3
import luigi
import moto
import pytest

from disdat.pipe import PipeTask
import disdat.api as api
from tests.functional.common import TEST_CONTEXT


TEST_REMOTE = '__test_remote_context__'
TEST_BUCKET = 'test-bucket'
TEST_BUCKET_URL = "s3://{}".format(TEST_BUCKET)


class AIP(PipeTask):
    def pipe_requires(self):
        self.set_bundle_name('a')

    def pipe_run(self):

        target = self.create_output_file('a.txt')
        with target.open('w') as output:
            output.write('Hi!')
        return {'file': [target]}


class BIP(PipeTask):

    n = luigi.IntParameter()

    def pipe_requires(self):
        self.set_bundle_name('b')
        self.add_dependency('a', AIP, params={})

    def pipe_run(self, a):
        target = self.create_output_file('b.txt')

        a_path = a['file'][0]
        with open(a_path) as f:
            print(f.read())

        with target.open('w') as output:
            output.write(str(self.n))
        return {'file': [target]}


class CIP(PipeTask):

    n = luigi.IntParameter(default=2)

    def pipe_requires(self):
        self.set_bundle_name('c')
        self.add_dependency('b', BIP, params={'n': self.n})

    def pipe_run(self, b=None):
        target = self.create_output_file('c.txt')
        with target.open('w') as output:
            output.write(str(self.n + 5))
        return {'file': [target]}


@moto.mock_s3
def test_add_with_treat_as_bundle():
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

    # Run test pipeline
    api.apply(TEST_CONTEXT, CIP)

    # Push bundles to remote
    for bundle_name in ['a', 'b', 'c']:
        assert api.get(TEST_CONTEXT, bundle_name) is not None, 'Bundle should exist'

        api.commit(TEST_CONTEXT, bundle_name)
        api.push(TEST_CONTEXT, bundle_name)

    # Blow away context and recreate
    api.delete_context(TEST_CONTEXT)
    assert TEST_CONTEXT not in api.ls_contexts()

    api.context(context_name=TEST_CONTEXT)
    api.remote(TEST_CONTEXT, TEST_REMOTE, TEST_BUCKET_URL)

    assert api.search(TEST_CONTEXT) == [], 'Context should be empty'

    # Pull bundles from remote
    api.pull(TEST_CONTEXT)

    # Make sure all bundle meta data comes down but data remains in S3
    for bundle_name in ['a', 'b', 'c']:
        bundle = api.get(TEST_CONTEXT, bundle_name)
        assert bundle is not None, 'Bundle should exist'

        data_path = bundle.data['file'][0]
        assert data_path.startswith('s3://'), 'Data should be in S3'

    # Rerun pipeline
    api.apply(TEST_CONTEXT, BIP, params={'n': 100}, incremental_pull=True)

    # Make sure all bundles exist. Bundles a and b should have local paths
    for bundle_name in ['a', 'b', 'c']:
        bundle = api.get(TEST_CONTEXT, bundle_name)
        assert bundle is not None, 'Bundle should exist'

        data_path = bundle.data['file'][0]
        if bundle_name in ['a', 'b']:
            assert not data_path.startswith('s3://'), 'Data should be local'
        else:
            assert data_path.startswith('s3://'), 'Data should be in S3'

    api.delete_context(TEST_CONTEXT)


if __name__ == '__main__':
    pytest.main([__file__])
