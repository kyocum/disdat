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

import os

import boto3
import moto
import docker
import pytest


from tests.functional.common import run_test, TEST_CONTEXT
from tests.functional.common_tasks import COMMON_DEFAULT_ARGS
import disdat.api as api


TEST_NAME    = 'test_bundle'
TEST_BUCKET  = 'test-bucket'
TEST_BUCKET_URL = "s3://{}".format(TEST_BUCKET)
PWD          = os.path.dirname(__file__)
SETUP_DIR    = os.path.join(PWD,'..')
PIPELINE_CLS = 'functional.common_tasks.A'


@pytest.fixture(scope="module")
def build_container_setup_only():
    """ Create a docker image locally.  At the moment we are only
    testing whether the basic python setup.py builds correctly.

    TODO: Need to test using the configure directory with
        a.) Using a MANIFEST file
        b.) Installing custom python packages
        c.) Installing rpms
        d.) Installing R packages
    """

    retval = api.dockerize(SETUP_DIR)
    id = api.dockerize_get_id(SETUP_DIR)
    yield id
    docker_client = docker.from_env()
    docker_client.images.remove(id, force=True)


def test_run_local_container(run_test, build_container_setup_only):
    """ Run the local container.
    Test if it runs, test if it re-runs all steps, test if it re-runs last step.
    """

    retval = api.run(SETUP_DIR,
                     TEST_CONTEXT,
                     PIPELINE_CLS
                     )

    b_b = api.get(TEST_CONTEXT, 'B')
    assert b_b is not None

    b_a = api.get(TEST_CONTEXT, 'A')
    assert b_a is not None
    assert b_a.data == sum(COMMON_DEFAULT_ARGS)

    # Re-run with force all

    retval = api.run(SETUP_DIR,
                     TEST_CONTEXT,
                     PIPELINE_CLS,
                     {'int_array': [1, 2, 3]},
                     force_all=True
                     )

    b_b_f = api.get(TEST_CONTEXT, 'B')
    assert b_b_f is not None
    assert b_b.uuid != b_b_f.uuid

    b_a_f = api.get(TEST_CONTEXT, 'A')
    assert b_a_f is not None
    assert b_a.uuid != b_a_f.uuid
    assert b_a_f.data == sum([1, 2, 3])

    # Re-run with force last one

    retval = api.run(SETUP_DIR,
                     TEST_CONTEXT,
                     PIPELINE_CLS,
                     {'int_array': [1, 2, 3]},
                     force=True
                     )

    b_b_f2 = api.get(TEST_CONTEXT, 'B')
    assert b_b_f2 is not None
    assert b_b_f.uuid == b_b_f2.uuid

    b_a_f2 = api.get(TEST_CONTEXT, 'A')
    assert b_a_f2 is not None
    assert b_a_f.uuid != b_a_f2.uuid


#@moto.mock_s3
def manual_test_run_aws_batch(run_test, build_container_setup_only):
    """ Incomplete test.   The container code itself needs to have
    its S3 access mocked out.  Here we are testing manually
    """

    # Setup moto s3 resources
    #s3_resource = boto3.resource('s3')
    #s3_resource.create_bucket(Bucket=TEST_BUCKET)

    # Add a remote.   Pull and Push!
    manual_s3_url = 's3://'
    api.remote(TEST_CONTEXT, TEST_CONTEXT, manual_s3_url)

    retval = api.run(SETUP_DIR,
                     TEST_CONTEXT,
                     PIPELINE_CLS,
                     remote_context=TEST_CONTEXT,
                     remote_s3_url=manual_s3_url,
                     pull=True,
                     push=True
                     )

    # Blow away everything and pull
    api.rm(TEST_CONTEXT, bundle_name='.*', rm_all=True)
    api.pull(TEST_CONTEXT)
    b = api.get(TEST_CONTEXT, 'A')
    assert b.data == sum(COMMON_DEFAULT_ARGS)


if __name__ == '__main__':
    pytest.main([__file__])
