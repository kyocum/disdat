import os
import tempfile
from typing import List

import boto3
import moto
import pytest

import disdat.api as api

TEST_CONTEXT = "___test_context___"
TEST_BUCKET = "test-bucket"
PUT_TEST_BUCKET = "del-test-bucket"
TEST_BUCKET_URL = "s3://{}".format(TEST_BUCKET)
MAX_KEYS = 10000


@pytest.fixture(scope="module")
def my_temp_path():
    with tempfile.TemporaryDirectory() as newpath:
        yield newpath


@pytest.fixture(autouse=True, scope="function")
def run_test():
    # Remove test context before running test
    setup()
    yield
    api.delete_context(context_name=TEST_CONTEXT)


@pytest.fixture(autouse=False, scope="module")
def run_module_test():
    # Remove test context before running test
    setup()
    yield
    pass
    api.delete_context(context_name=TEST_CONTEXT)


def setup():
    os.environ["DISDAT_CPU_COUNT"] = "1"
    if TEST_CONTEXT in api.ls_contexts():
        api.delete_context(context_name=TEST_CONTEXT)
    api.context(context_name=TEST_CONTEXT)


@pytest.fixture(autouse=False, scope="module")
def moto_boto():
    # When you need to have moto mocked across tests.
    with moto.mock_s3():
        print(">>>>>>>>>>>>>>>>>>>>  MOTO UP  <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
        yield
    print(">>>>>>>>>>>>>>>>>>>> MOTO DOWN <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")


@pytest.fixture(scope="module")
def count() -> int:
    return 100


@pytest.fixture(scope="function")
@moto.mock_s3
def populate_objects(run_module_test, moto_boto, count: int) -> dict:
    """Create a disdat context with count bundles.
    Return the list of UUIDs and the list of objects
    (list[uuid], list[paths])"""
    s3_client = boto3.client("s3")
    s3_resource = boto3.resource("s3", region_name="us-east-1")
    s3_resource.create_bucket(Bucket=TEST_BUCKET)
    bucket = s3_resource.Bucket(TEST_BUCKET)
    api.remote(TEST_CONTEXT, TEST_CONTEXT, TEST_BUCKET_URL)
    uuids = []
    for i in range(count):
        with api.Bundle(TEST_CONTEXT, name="remote_test") as b:
            b.add_data(f"Hello World {i}")
        b.commit().push()
        uuids.append(b.uuid)
    objects = s3_client.list_objects(Bucket=TEST_BUCKET, MaxKeys=MAX_KEYS)
    assert "Contents" in objects, "Bucket should not be empty"
    if len(objects["Contents"]) != 2 * count:
        print(f"Bucket should have {count*2} ojects, not {len(objects['Contents'])}")
    return {"uuids": uuids, "paths": [d["Key"] for d in objects["Contents"]]}


@pytest.fixture(scope="module")
def populate_local_files(run_module_test, moto_boto, my_temp_path, count: int) -> List:
    """Create a local temp directory, fill it with files.
    Note that we use PUT_TEST_BUCKET, b/c we don't want to delete the files made in populate_objects
    """
    s3_resource = boto3.resource("s3", region_name="us-east-1")
    s3_resource.create_bucket(Bucket=PUT_TEST_BUCKET)
    files = []
    for i in range(count):
        dir = os.path.join(my_temp_path, str(i % 10))
        try:
            os.mkdir(dir)
        except FileExistsError as fee:
            pass
        files.append(os.path.join(dir, f"file_{i}.txt"))
        with open(files[i], mode="w") as file:
            file.write(f"this is file {i}")
    return files
