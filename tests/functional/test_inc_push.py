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

author: Kenneth Yocum
"""
import boto3
import luigi
import moto

from disdat.pipe import PipeTask
import disdat.api as api
from tests.functional.common import TEST_CONTEXT

TEST_REMOTE = '__test_remote_context__'
TEST_BUCKET = 'test-bucket'
TEST_BUCKET_URL = "s3://{}".format(TEST_BUCKET)


class APush(PipeTask):
    def pipe_requires(self):
        self.set_bundle_name('a')

    def pipe_run(self):

        target = self.create_output_file('a.txt')
        with target.open('w') as output:
            output.write('Hi!')
        return {'file': [target]}


class BPush(PipeTask):

    n = luigi.IntParameter()

    def pipe_requires(self):
        self.set_bundle_name('b')
        self.add_dependency('a', APush, params={})

    def pipe_run(self, a):
        target = self.create_output_file('b.txt')

        a_path = a['file'][0]
        with open(a_path) as f:
            print(f.read())

        with target.open('w') as output:
            output.write(str(self.n))
        return {'file': [target]}


class CPush(PipeTask):

    n = luigi.IntParameter(default=2)

    def pipe_requires(self):
        self.set_bundle_name('c')
        self.add_dependency('b', BPush, params={'n': self.n})

    def pipe_run(self, b=None):
        # Barf!
        raise Exception


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

    # Try to run the pipeline - should fail
    try:
        # Run test pipeline
        api.apply(TEST_CONTEXT, CPush, incremental_push=True)
    except Exception as e:
        pass

    # Get objects from remote
    objects = s3_client.list_objects(Bucket=TEST_BUCKET)
    keys = [o['Key'] for o in objects['Contents']]
    keys = [key.split('/')[-1] for key in keys]

    # Make sure files exist in S3
    for output_file in ['a.txt', 'b.txt']:
        assert output_file in keys, 'Pipeline should have pushed file'

    api.delete_context(TEST_CONTEXT)
