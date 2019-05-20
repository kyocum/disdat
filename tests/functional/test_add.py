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
import hashlib

import boto3
import moto
import pandas as pd
import pytest

import disdat.api as api
from tests.functional.common import TEST_CONTEXT


TEST_REMOTE = '__test_remote_context__'
TEST_BUCKET = 'test-bucket'
TEST_BUCKET_URL = "s3://{}".format(TEST_BUCKET)


def get_hash(path):
    return hashlib.md5(open(path, 'rb').read()).hexdigest()


def test_add_bad_path(tmpdir):
        # Create Context
        api.context(TEST_CONTEXT)

        # Create path to csv file but don't create file
        test_csv_path = os.path.join(str(tmpdir), 'test.csv')

        # Assert csv file does not exist
        assert not os.path.exists(test_csv_path)

        # Try to add file to the bundle
        with pytest.raises(AssertionError) as ex:
            api.add(TEST_CONTEXT, 'bad_path', test_csv_path)

        # Assert Exited with error code of 1
        assert ex.type == AssertionError

        # Make sure bundle does not exist
        assert api.get(TEST_CONTEXT, 'test_file_as_bundle_txt_file') is None, 'Bundle should not exist'

        api.delete_context(TEST_CONTEXT)


def test_single_file(tmpdir):

    # Create Context
    api.context(TEST_CONTEXT)

    # Create test .csv file
    test_csv_path = os.path.join(str(tmpdir), 'test.csv')
    df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
    df.to_csv(test_csv_path)

    # Assert csv_file_exits
    assert os.path.exists(test_csv_path)

    # Add the file to the bundle
    api.add(TEST_CONTEXT, 'test_single_file', test_csv_path)

    # Retrieve the bundle
    b = api.get(TEST_CONTEXT, 'test_single_file')

    # Assert the bundles contain the same data
    bundle_hash, file_hash = get_hash(b.data[0]), get_hash(test_csv_path)
    assert bundle_hash == file_hash, 'Hashes do not match'

    # Test with tags
    tag = {'test': 'tag'}
    api.add(TEST_CONTEXT, 'test_single_file', test_csv_path, tags=tag)

    # Retrieve the bundle
    b = api.get(TEST_CONTEXT, 'test_single_file')

    # Assert the bundles contain the same data
    bundle_hash, file_hash = get_hash(b.data[0]), get_hash(test_csv_path)
    assert bundle_hash == file_hash, 'Hashes do not match'
    assert b.tags == tag, 'Tags do not match'

    # Remove test .csv
    os.remove(test_csv_path)

    # Assert that data still remains in the bundle
    assert api.get(TEST_CONTEXT, 'test_single_file') is not None, 'Bundle should exist'

    api.delete_context(TEST_CONTEXT)


def test_add_directory(tmpdir):

    # Create Context
    api.context(TEST_CONTEXT)

    # Create test .csv file
    test_csv_path = os.path.join(str(tmpdir), 'test.csv')
    df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
    df.to_csv(test_csv_path)

    deeper_directory = os.path.join(str(tmpdir), 'deep_dir')
    os.mkdir(deeper_directory)

    # Create test .txt file
    test_text_path = os.path.join(deeper_directory, 'test.txt')
    with open(test_text_path, 'w') as f:
        f.write('Hello!')

    # Assert files exist
    assert os.path.exists(test_csv_path)
    assert os.path.exists(test_text_path)

    local_paths = [test_csv_path, test_text_path]

    # Add the directory to the bundle
    api.add(TEST_CONTEXT, 'test_directory', str(tmpdir))

    # Assert check sums are the same
    b = api.get(TEST_CONTEXT, 'test_directory')
    files = zip(b.data, local_paths)
    for f in files:
        bundle_file, local_file = f
        assert get_hash(bundle_file) == get_hash(local_file), 'Hashes do not match'

    # Add the directory to the bundle with tags
    tag = {'test': 'tag'}
    api.add(TEST_CONTEXT, 'test_directory', str(tmpdir), tags=tag)

    # Assert check sums are the same
    b = api.get(TEST_CONTEXT, 'test_directory')
    files = zip(b.data, local_paths)
    for f in files:
        bundle_file, local_file = f
        assert get_hash(bundle_file) == get_hash(local_file), 'Hashes do not match'

    assert b.tags == tag, 'Tags do not match'
    api.delete_context(TEST_CONTEXT)


@moto.mock_s3
def test_add_with_treat_as_bundle(tmpdir):
    api.context(context_name=TEST_CONTEXT)

    # Setup moto s3 resources
    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    s3_resource.create_bucket(Bucket=TEST_BUCKET)

    # Make sure bucket is empty
    objects = s3_client.list_objects(Bucket=TEST_BUCKET)
    assert 'Contents' not in objects, 'Bucket should be empty'

    local_paths = []
    s3_paths = []

    # Create and upload test.csv file
    key = 'test.csv'
    test_csv_path = os.path.join(str(tmpdir), key)
    df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
    df.to_csv(test_csv_path)

    s3_resource.meta.client.upload_file(test_csv_path, TEST_BUCKET, key)
    s3_path = "s3://{}/{}".format(TEST_BUCKET, key)

    local_paths.append(test_csv_path)
    s3_paths.append(s3_path)

    # Create and uploadt test.txt file
    key = 'text.txt'
    test_txt_path = os.path.join(str(tmpdir), key)
    with open(test_txt_path, 'w') as f:
        f.write('Test')

    s3_resource.meta.client.upload_file(test_txt_path, TEST_BUCKET, key)
    s3_path = "s3://{}/{}".format(TEST_BUCKET, key)

    local_paths.append(test_txt_path)
    s3_paths.append(s3_path)

    bool_values = [True, False]
    string_values = ['a', 'b']
    float_values = [1.3, 3.5]
    int_values = [4, 5]

    # Build bundle dataframe
    bundle_df = pd.DataFrame({
        'local_paths': local_paths,
        's3_paths': s3_paths,
        'bools': bool_values,
        'strings': string_values,
        'floats': float_values,
        'ints': int_values
    })

    bundle_df_path = os.path.join(str(tmpdir), 'bundle.csv')
    bundle_df.to_csv(bundle_df_path)

    # Add bundle dataframe
    api.add(TEST_CONTEXT, 'test_add_bundle', bundle_df_path, treat_file_as_bundle=True)

    # Assert that data in bundle is a dataframe
    b = api.get(TEST_CONTEXT, 'test_add_bundle')
    assert(isinstance(b.data, pd.DataFrame))

    # Add bundle dataframe with tags
    tag = {'test': 'tag'}
    api.add(TEST_CONTEXT, 'test_add_bundle', bundle_df_path, treat_file_as_bundle=True, tags=tag)

    # Assert that data in bundle is a dataframe
    b = api.get(TEST_CONTEXT, 'test_add_bundle')
    assert(isinstance(b.data, pd.DataFrame))
    assert b.tags == tag, 'Tags do not match'

    api.delete_context(TEST_CONTEXT)


def test_data_as_bundle_not_csv(tmpdir):

    # Create Context
    api.context(TEST_CONTEXT)

    # Create test .txt file
    test_txt_path = os.path.join(str(tmpdir), 'test.txt')
    with open(test_txt_path, 'w') as f:
        f.write('this should not create a bundle')

    # Assert the txt file exists
    assert os.path.exists(test_txt_path)

    # Try to add file to the bundle
    with pytest.raises(AssertionError) as ex:
        api.add(TEST_CONTEXT, 'bad_path', test_txt_path, treat_file_as_bundle=True)

    # Assert Exited with error code of 1
    assert ex.type == AssertionError

    # Make sure bundle does not exist
    assert api.get(TEST_CONTEXT, 'test_file_as_bundle_txt_file') is None, 'Bundle should not exist'

    api.delete_context(TEST_CONTEXT)
