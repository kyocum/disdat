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
import pathlib
from time import time
from typing import List

import boto3
import pytest

import disdat.api as api
from disdat.common import create_uuid
from disdat.utility.aws_s3 import (
    delete_s3_dir_many,
    get_s3_key_many,
    ls_s3_url_keys,
    put_s3_key_many,
)
from tests.functional.common import (
    MAX_KEYS,
    PUT_TEST_BUCKET,
    TEST_BUCKET,
    TEST_CONTEXT,
    count,
    moto_boto,
    my_temp_path,
    populate_local_files,
    populate_objects,
    run_module_test,
)

"""  Tests for s3_utility 
Test the parallel upload, download, file listing, and deletion
The parallel listing only happens when we look at keys in a disdat context. 
"""


def list_files(dir: str) -> List[str]:
    path = pathlib.Path(dir)
    return [str(f) for f in path.iterdir()]


def test_ls_s3_url_keys(populate_objects):
    start = time()
    disdat_objects_url = f"s3://{TEST_BUCKET}/context/{TEST_CONTEXT}/objects"
    ls_result = ls_s3_url_keys(disdat_objects_url, is_object_directory=True)
    end = time()
    # print(f"ls_s3_url_keys: {ls_result}")
    print(f"Elapsed: {end-start}")
    assert set(ls_result) == set(populate_objects["paths"])


def test_get_s3_keys_many(populate_objects, tmp_path):
    gets = [
        (TEST_BUCKET, f, os.path.join(tmp_path, os.path.basename(f)))
        for f in populate_objects["paths"]
    ]
    start = time()
    get_result = get_s3_key_many(gets)
    end = time()
    # print(f"get_s3_key_many: {get_result}")
    print(f"Elapsed: {end-start}")
    found_set = [os.path.basename(f) for f in list_files(tmp_path)]
    assert set(found_set) == set(
        [os.path.basename(f) for f in populate_objects["paths"]]
    )


def test_put_s3_key_many(populate_local_files):
    """populate_local_files, creates a bunch of local files in a temp dir, but it also
    creates a bucket at the module fixture level.  So we're just writing straight into s3://bucket/thing.txt
    """
    start = time()
    # bucket_key_file_tuples (list[tuple]): (filename, s3_path)
    puts = [
        (
            f,
            os.path.join(
                f"s3://{PUT_TEST_BUCKET}",
                os.path.split(os.path.dirname(f))[1],
                os.path.basename(f),
            ),
        )
        for f in populate_local_files
    ]
    put_results = put_s3_key_many(puts)
    end = time()
    print(f"Elapsed: {end-start}")
    s3_client = boto3.client("s3")
    objects = s3_client.list_objects(Bucket=PUT_TEST_BUCKET, MaxKeys=MAX_KEYS)
    found_s3_paths = [d["Key"] for d in objects["Contents"]]
    assert set([os.path.basename(f) for f in put_results]) == set(
        [os.path.basename(f) for f in found_s3_paths]
    )


def test_delete_s3_dir_many(populate_objects):
    """Delete bundles in a remote context. This passes in s3 keys for
    each of the bundles, not individual files.
    Note: because moto is not thread safe, we only test with a single thread. see common.py:setup

    Args:
        populate_objects (_type_): A fixture creating the files on s3 to delete.
    """
    s3_client = boto3.client("s3")
    objects = s3_client.list_objects(Bucket=TEST_BUCKET, MaxKeys=MAX_KEYS)
    s3_paths = set([f["Key"] for f in objects["Contents"]])
    assert s3_paths == set(
        populate_objects["paths"]
    ), f"s3 list object != fixture s3 object list"
    head_cache = set()
    for s in s3_paths:
        head, _ = os.path.split(s)
        if head not in head_cache:
            head_cache.add(head)
    _delete_s3_paths(s3_client, head_cache)


def test_delete_s3_dir_many_individual(populate_objects):
    """Delete bundles in a remote context. This passes in s3 keys for individual
    files on s3.
    Note: because moto is not thread safe, we only test with a single thread. see common.py:setup
    Args:
        populate_objects (_type_): A fixture creating the files on s3 to delete.
    """
    s3_client = boto3.client("s3")
    objects = s3_client.list_objects(Bucket=TEST_BUCKET, MaxKeys=MAX_KEYS)
    s3_paths = set([f["Key"] for f in objects["Contents"]])
    assert s3_paths == set(
        populate_objects["paths"]
    ), f"s3 list object != fixture s3 object list"
    _delete_s3_paths(s3_client, s3_paths)


def _delete_s3_paths(s3_client, s3_paths):
    start = time()
    del_results = delete_s3_dir_many(
        [os.path.join(f"s3://{TEST_BUCKET}", p) for p in s3_paths]
    )
    end = time()
    print(f"delete_s3_key_many_individual: {del_results}")
    print(f"Elapsed: {end-start}")
    objects = s3_client.list_objects(Bucket=TEST_BUCKET, MaxKeys=MAX_KEYS)
    assert "Contents" not in objects


if __name__ == "__main__":
    # setup()
    # uuids, paths = populate_objects(2)
    # x = create_testdir()
    # print(x)
    # print(uuids)
    # print(paths)

    pytest.main([__file__, "-s"])
