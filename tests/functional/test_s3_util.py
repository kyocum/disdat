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

import logging
import os
import pathlib
from time import time
from types import SimpleNamespace
from typing import List

import boto3
import botocore
import pytest

import disdat.api as api
import disdat.utility.aws_s3 as aws_s3
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


# Tests for issue #223: AWS Error.Code must not be cast to int.
#
# `head_bucket` and `Object.load` report a missing object with the numeric-string
# code '404', but service-level failures use symbolic codes ('ServiceUnavailable',
# 'AccessDenied', ...). Casting the code to int raised ValueError on those,
# replacing the real ClientError and leaving it only on __context__.


def _client_error(code, operation):
    """Build a botocore ClientError carrying `code` as its Error.Code."""
    return botocore.exceptions.ClientError(
        {"Error": {"Code": code, "Message": "synthetic {}".format(code)}},
        operation,
    )


@pytest.mark.parametrize(
    "code", ["ServiceUnavailable", "SlowDown", "InternalError", "InvalidAccessKeyId"]
)
def test_bucket_exists_propagates_non_numeric_error_code(monkeypatch, code):
    """A non-numeric Error.Code must surface as the original ClientError."""

    class _Stub:
        def head_bucket(self, Bucket):
            raise _client_error(code, "HeadBucket")

    monkeypatch.setattr(
        aws_s3, "get_s3_resource", lambda: SimpleNamespace(meta=SimpleNamespace(client=_Stub()))
    )

    with pytest.raises(botocore.exceptions.ClientError) as excinfo:
        aws_s3.s3_bucket_exists("any-bucket")

    assert excinfo.value.response["Error"]["Code"] == code


@pytest.mark.parametrize("code", ["ServiceUnavailable", "SlowDown", "InternalError"])
def test_path_exists_propagates_non_numeric_error_code(monkeypatch, code):
    """s3_path_exists has the same cast; it must also propagate."""

    class _Obj:
        def load(self):
            raise _client_error(code, "HeadObject")

    monkeypatch.setattr(
        aws_s3, "get_s3_resource", lambda: SimpleNamespace(Object=lambda b, k: _Obj())
    )

    with pytest.raises(botocore.exceptions.ClientError) as excinfo:
        aws_s3.s3_path_exists("s3://some-bucket/some/key")

    assert excinfo.value.response["Error"]["Code"] == code


@pytest.mark.parametrize("code", ["NoSuchBucket", "404"])
def test_bucket_exists_false_for_missing_bucket_codes(monkeypatch, code):
    """Both the numeric and symbolic 'missing' codes mean False, not an error."""

    class _Stub:
        def head_bucket(self, Bucket):
            raise _client_error(code, "HeadBucket")

    monkeypatch.setattr(
        aws_s3, "get_s3_resource", lambda: SimpleNamespace(meta=SimpleNamespace(client=_Stub()))
    )

    assert aws_s3.s3_bucket_exists("missing-bucket") is False


@pytest.mark.parametrize("code", ["AccessDenied", "403"])
def test_bucket_exists_false_for_forbidden_codes(monkeypatch, code):
    """403/AccessDenied is treated as 'not visible to us', not an exception."""

    class _Stub:
        def head_bucket(self, Bucket):
            raise _client_error(code, "HeadBucket")

    monkeypatch.setattr(
        aws_s3, "get_s3_resource", lambda: SimpleNamespace(meta=SimpleNamespace(client=_Stub()))
    )

    assert aws_s3.s3_bucket_exists("forbidden-bucket") is False


def test_bucket_exists_raises_for_suspended_account(monkeypatch):
    """AllAccessDisabled means the account is suspended, not that the bucket is
    absent. It must raise rather than be reported as a missing bucket, so the
    operator sees the real cause."""

    class _Stub:
        def head_bucket(self, Bucket):
            raise _client_error("AllAccessDisabled", "HeadBucket")

    monkeypatch.setattr(
        aws_s3,
        "get_s3_resource",
        lambda: SimpleNamespace(meta=SimpleNamespace(client=_Stub())),
    )

    with pytest.raises(botocore.exceptions.ClientError) as excinfo:
        aws_s3.s3_bucket_exists("suspended-bucket")

    assert excinfo.value.response["Error"]["Code"] == "AllAccessDisabled"


def test_bucket_exists_logs_url_before_propagating(monkeypatch, caplog):
    """The propagating branch must name the bucket it was checking."""

    class _Stub:
        def head_bucket(self, Bucket):
            raise _client_error("ServiceUnavailable", "HeadBucket")

    monkeypatch.setattr(
        aws_s3,
        "get_s3_resource",
        lambda: SimpleNamespace(meta=SimpleNamespace(client=_Stub())),
    )

    with caplog.at_level(logging.WARNING):
        with pytest.raises(botocore.exceptions.ClientError):
            aws_s3.s3_bucket_exists("noisy-bucket")

    assert "noisy-bucket" in caplog.text
    assert "ServiceUnavailable" in caplog.text


def test_path_exists_logs_url_before_propagating(monkeypatch, caplog):
    """Same for s3_path_exists: the failing URL must appear in the log."""

    class _Obj:
        def load(self):
            raise _client_error("ServiceUnavailable", "HeadObject")

    monkeypatch.setattr(
        aws_s3, "get_s3_resource", lambda: SimpleNamespace(Object=lambda b, k: _Obj())
    )

    with caplog.at_level(logging.WARNING):
        with pytest.raises(botocore.exceptions.ClientError):
            aws_s3.s3_path_exists("s3://noisy-bucket/some/key")

    assert "s3://noisy-bucket/some/key" in caplog.text


@pytest.mark.parametrize("code", ["NoSuchKey", "404"])
def test_path_exists_false_for_missing_key_codes(monkeypatch, code):
    """A missing key is False for both the numeric and symbolic codes."""

    class _Obj:
        def load(self):
            raise _client_error(code, "HeadObject")

    monkeypatch.setattr(
        aws_s3, "get_s3_resource", lambda: SimpleNamespace(Object=lambda b, k: _Obj())
    )

    assert aws_s3.s3_path_exists("s3://some-bucket/some/key") is False


if __name__ == "__main__":
    # setup()
    # uuids, paths = populate_objects(2)
    # x = create_testdir()
    # print(x)
    # print(uuids)
    # print(paths)

    pytest.main([__file__, "-s"])
