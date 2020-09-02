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
import moto
import pytest
import hashlib
import tempfile
import os

import disdat.api as api
import disdat.utility.aws_s3 as aws_s3
from tests.functional.common import TEST_CONTEXT

TEST_REMOTE = '__test_remote_context__'
TEST_BUCKET = 'test-bucket'
TEST_BUCKET_URL = "s3://{}".format(TEST_BUCKET)


def md5_file(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def create_local_file_bundle(name):
    """
    Create a local file bundle.  It has an external file,
    a managed file, and a managed dir file.

    Args:
        name:

    Returns:

    """
    local_fp = tempfile.NamedTemporaryFile()
    local_fp.write(b'an external local file in bundle')
    local_fp.flush()

    with api.Bundle(TEST_CONTEXT, name=name) as b:
        f1 = b.get_file("file_1.txt")
        f2 = b.get_file("file_2.txt")
        f3 = os.path.join(b.get_directory("vince/klartho"), 'file_3.txt')
        with open(f1, mode='w') as f:
            f.write("This is our first file! {}".format(name))
        with open(f2, mode='w') as f:
            f.write("This is our second file! {}".format(name))
        with open(f3, mode='w') as f:
            f.write("This is our third file! {}".format(name))
        b.add_data([local_fp.name, f1, f2, f3])
        hashes = {"f{}".format(i): md5_file(f) for i, f in enumerate([local_fp.name, f1, f2, f3])}
        b.add_tags(hashes)

    local_fp.close()

    saved_uuid = b.uuid
    b = api.get(TEST_CONTEXT, None, uuid=saved_uuid)
    b.commit()
    for i, f in enumerate(b.data):
        assert md5_file(f) == hashes["f{}".format(i)]

    return b


def create_remote_file_bundle(name):
    """ Create a bundle with
     a.) an unmanaged s3 path
     b.) a managed s3 path
     c.) a managed s3 path with a directory
     """
    s3_resource = boto3.resource('s3')
    s3_resource.create_bucket(Bucket=TEST_BUCKET)

    # Copy a local file to moto s3 bucket
    saved_md5 = md5_file(__file__)
    aws_s3.put_s3_file(__file__, TEST_BUCKET_URL)

    s3_path_1 = os.path.join(TEST_BUCKET_URL, os.path.basename(__file__))

    with api.Bundle(TEST_CONTEXT, name=name) as b:
        s3_path_2 = b.get_remote_file('test_s3_file.txt')
        aws_s3.cp_local_to_s3_file(__file__, s3_path_2)
        s3_path_3 = os.path.join(b.get_remote_directory('vince/klartho'), 'test_s3_file.txt')
        aws_s3.cp_local_to_s3_file(__file__, s3_path_3)

        b.add_data([s3_path_1, s3_path_2, s3_path_3])
        b.add_tags({'info': 'added an s3 file'})

    saved_uuid = b.uuid

    b = api.get(TEST_CONTEXT, None, uuid=saved_uuid)
    b.commit()
    md5 = md5_file(b.data[0])
    print(md5)
    print(saved_md5)
    assert md5 == saved_md5


def _setup(remote=True):
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
    if remote:
        api.remote(TEST_CONTEXT, TEST_REMOTE, TEST_BUCKET_URL)

    return s3_client


@moto.mock_s3
def test_fast_push():

    s3_client = _setup()

    bundles = {}
    for i in range(3):
        name = "shark{}".format(i)
        bundles[name] = create_local_file_bundle(name)
        bundles[name].push()   # need this to put the bundles in the remote b/c of moto mp issues

    # Moto keeps s3 xfers in memory, so multiprocessing will succeed
    # But when the subprocess exits, the files will disappear
    # This is basically useless in a test.
    api.push(TEST_CONTEXT)  # push and remote all data, then pull and localize.

    objects = s3_client.list_objects(Bucket=TEST_BUCKET)
    assert 'Contents' in objects, 'Bucket should not be empty'
    assert len(objects['Contents']) > 0, 'Bucket should not be empty'

    api.rm(TEST_CONTEXT, rm_all=True)
    api.pull(TEST_CONTEXT, localize=True)
    found_bundles = {b.name: b for b in api.search(TEST_CONTEXT)}
    for n, b in found_bundles.items():
        for i, f in enumerate(b.data):
            assert md5_file(f) == b.tags["f{}".format(i)]

    api.delete_context(TEST_CONTEXT)


@moto.mock_s3
def test_bundle_push_delocalize():
    """ Test Bundle.push(delocalize)
    Test if we can push individually, and see that the files actualize to s3 paths.
    """
    s3_client = _setup()

    bundles = {}
    for i in range(3):
        name = "shark{}".format(i)
        bundles[name] = create_local_file_bundle(name)
        bundles[name].push(delocalize=True)

    objects = s3_client.list_objects(Bucket=TEST_BUCKET)
    assert 'Contents' in objects, 'Bucket should not be empty'
    assert len(objects['Contents']) > 0, 'Bucket should not be empty'

    # Check for delocalization
    for b in bundles.values():
        for i, f in enumerate(b.data):
            assert f.startswith("s3://")

    # Might as well test the push and pull worked.
    api.rm(TEST_CONTEXT, rm_all=True)
    api.pull(TEST_CONTEXT, localize=True)
    found_bundles = {b.name: b for b in api.search(TEST_CONTEXT)}
    for n, b in found_bundles.items():
        for i, f in enumerate(b.data):
            assert md5_file(f) == b.tags["f{}".format(i)]

    api.delete_context(TEST_CONTEXT)

@moto.mock_s3
def X_test_api_push_delocalize():
    """ Test api.push(delocalize)

    Unfortunately, moto fails on testing this with MP, so removing test. 

    """
    _ = _setup()

    bundles = {}
    for i in range(3):
        name = "shark{}".format(i)
        bundles[name] = create_local_file_bundle(name)

    # Moto keeps s3 xfers in memory, so multiprocessing will succeed
    # But when the subprocess exits, the files will disappear
    # So we cannot see the result of the push (b/c of moto in memory s3),
    # but the delocalize should remove the data.
    api.push(TEST_CONTEXT, delocalize=True)  # push and remote all data, then pull and localize.

    for b in bundles.values():
        for i, f in enumerate(b.data):
            assert f.startswith("s3://")

    api.delete_context(TEST_CONTEXT)


@moto.mock_s3
def test_bundle_link_localization_no_remote():
    """ Test the ability to localize and delocalize individual links
    Note: you can only localize / de-localize a closed bundle.

    create bundles.

    test w/o a remote

    """
    _ = _setup(remote=False)

    b = create_local_file_bundle("sharkies")

    # all local files.  then delocalize each in turn.
    errors = 0
    for i, f in enumerate(b.data):
        assert f.startswith("/")
        try:
            b.delocalize(f)
        except AssertionError as ae:
            print ("Caught an expected assertion error -- {}".format(ae))
            errors += 1
        assert b.data[i].startswith("/") # should have failed

    assert errors == len(b.data)
    api.delete_context(TEST_CONTEXT)


@moto.mock_s3
def test_bundle_link_localization_with_remote():
    """ Test the ability to localize and delocalize individual links
    Note: you can only localize / de-localize a closed bundle.

    create bundles.

    test w/o a remote

    """
    _ = _setup()

    b = create_local_file_bundle("sharkies")

    b.push()

    # all local files.  then delocalize each in turn.
    for i, f in enumerate(b.data):
        assert f.startswith("/")
        b.delocalize(f)
        assert b.data[i].startswith("s3://")

    # bring it all back
    for i, f in enumerate(b.data):
        assert b.data[i].startswith("s3://")  # testing if we can both s3 and local paths in a link frame
        b.localize(f)
        assert b.data[i].startswith("/")

    api.delete_context(TEST_CONTEXT)


if __name__ == "__main__":
    pytest.main([__file__])
