import boto3
import moto

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
def test_push():
    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    s3_resource.create_bucket(Bucket=TEST_BUCKET)
    bucket = s3_resource.Bucket(TEST_BUCKET)

    objects = s3_client.list_objects(Bucket=TEST_BUCKET)
    assert 'Contents' not in objects, 'Bucket should be empty'

    assert len(api.search(TEST_CONTEXT)) == 0, 'Context should be empty'
    api.remote(TEST_CONTEXT, TEST_REMOTE, TEST_BUCKET_URL, force=True)

    api.apply(TEST_CONTEXT, '-', 'RemoteTest')
    bundle = api.get(TEST_CONTEXT, 'remote_test')

    assert bundle.data == 'Hello'

    bundle.commit()
    bundle.push()

    objects = s3_client.list_objects(Bucket=TEST_BUCKET)
    assert 'Contents' in objects, 'Bucket should not be empty'
    assert len(objects['Contents']) > 0, 'Bucket should not be empty'

    bucket.objects.all().delete()
    bucket.delete()

if __name__ == '__main__':
    test_push()