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
    bundle_hash, file_hash = get_hash(b.data), get_hash(test_csv_path)
    assert bundle_hash == file_hash, 'Hashes do not match'

    # Test with tags
    tag = {'test': 'tag'}
    api.add(TEST_CONTEXT, 'test_single_file', test_csv_path, tags=tag)

    # Retrieve the bundle
    b = api.get(TEST_CONTEXT, 'test_single_file')

    # Assert the bundles contain the same data
    bundle_hash, file_hash = get_hash(b.data), get_hash(test_csv_path)
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

    # Directory Structure
    # - test.csv
    # - second/test_1.txt
    # - second/test_2.txt
    # - second/third/test_3.txt
    # - second/third/test_4.txt
    level_1 = ''

    level_2 = os.path.join(level_1, 'second')
    os.mkdir(os.path.join(str(tmpdir), level_2))

    level_3 = os.path.join(level_2, 'third')
    os.mkdir(os.path.join(str(tmpdir), level_3))

    # Dictionary to hold paths
    path_dict = {}

    # Create files and save paths
    test_csv_name = 'test.csv'
    test_csv_path = os.path.join(level_1, test_csv_name)
    test_csv_abs_path = os.path.join(str(tmpdir), test_csv_path)
    df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
    df.to_csv(test_csv_abs_path)

    path_dict[test_csv_name] = (test_csv_abs_path, test_csv_path.split('/'))

    test_text_1_name = 'test_1.txt'
    test_text_1_path = os.path.join(level_2, test_text_1_name)
    test_text_name_1_abs_path = os.path.join(str(tmpdir), test_text_1_path)
    with open(test_text_name_1_abs_path, 'w') as f:
        f.write('Hello!')

    path_dict[test_text_1_name] = (test_text_name_1_abs_path, test_text_1_path.split('/'))

    test_text_2_name = 'test_2.txt'
    test_text_2_path = os.path.join(level_2, test_text_2_name)
    test_text_name_2_abs_path = os.path.join(str(tmpdir), test_text_2_path)
    with open(test_text_name_2_abs_path, 'w') as f:
        f.write('Hello!')

    path_dict[test_text_2_name] = (test_text_name_2_abs_path, test_text_2_path.split('/'))

    test_text_3_name = 'test_3.txt'
    test_text_3_path = os.path.join(level_3, test_text_3_name)
    test_text_name_3_abs_path = os.path.join(str(tmpdir), test_text_3_path)
    with open(test_text_name_3_abs_path, 'w') as f:
        f.write('Third Hello!')

    path_dict[test_text_3_name] = (test_text_name_3_abs_path, test_text_3_path.split('/'))

    test_text_4_name = 'test_4.txt'
    test_text_4_path = os.path.join(level_3, test_text_4_name)
    test_text_name_4_abs_path = os.path.join(str(tmpdir), test_text_4_path)
    with open(test_text_name_4_abs_path, 'w') as f:
        f.write('Third World!')

    path_dict[test_text_4_name] = (test_text_name_4_abs_path, test_text_4_path.split('/'))

    # Assert files exist
    assert os.path.exists(test_csv_abs_path)
    assert os.path.exists(test_text_name_1_abs_path)
    assert os.path.exists(test_text_name_2_abs_path)
    assert os.path.exists(test_text_name_3_abs_path)
    assert os.path.exists(test_text_name_4_abs_path)

    # Add the directory to the bundle
    api.add(TEST_CONTEXT, 'test_directory', str(tmpdir))

    # Assert check sums are the same
    b = api.get(TEST_CONTEXT, 'test_directory')
    for f in b.data:
        bundle_file_name = f.split('/')[-1]
        local_abs_path, local_split_path = path_dict[bundle_file_name]

        # Make sure paths match
        assert get_hash(f) == get_hash(local_abs_path), 'Hashes do not match'

        bundle_path = os.path.join(*f.split('/')[-len(local_split_path):])
        local_path = os.path.join(*local_split_path)

        assert local_path == bundle_path, 'Bundle should have the same directory structure'

    # Add the directory to the bundle with tags
    tag = {'test': 'tag'}
    api.add(TEST_CONTEXT, 'test_directory', str(tmpdir), tags=tag)

    # Assert check sums are the same
    b = api.get(TEST_CONTEXT, 'test_directory')
    for f in b.data:
        bundle_file_name = f.split('/')[-1]
        local_abs_path, local_split_path = path_dict[bundle_file_name]

        # Make sure paths match
        assert get_hash(f) == get_hash(local_abs_path), 'Hashes do not match'

        # Make sure directory structure stays the same
        local_path = os.path.join(*local_split_path)
        bundle_path = os.path.join(*f.split('/')[-len(local_split_path):])

        assert local_path == bundle_path, 'Bundle should have the same directory structure'

    # Make sure tags exist
    assert b.tags == tag, 'Tags do not match'

    api.delete_context(TEST_CONTEXT)


@moto.mock_s3
def deprecated_add_with_treat_as_bundle(tmpdir):
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

    # These are now deprecated
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


def deprecated_data_as_bundle_not_csv(tmpdir):

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


if __name__ == '__main__':
    import tempfile
    test_single_file(tempfile.gettempdir())