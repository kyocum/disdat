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
import pytest
import pandas as pd
from numpy import random
import shutil

import disdat.api as api
from tests.functional.common import run_test, TEST_CONTEXT


TEST_REMOTE = '__test_remote_context__'
TEST_BUCKET = 'test-bucket'
TEST_BUCKET_URL = "s3://{}".format(TEST_BUCKET)
TEST_BUNDLE_NAME = 'test_bundle_cat'


def get_hash(path):
    return hashlib.md5(open(path, 'rb').read()).hexdigest()


def test_cat(run_test):

    import tempfile

    # P3 only with tempfile.TemporaryDirectory() as tmpdir:
    tmpdir = tempfile.mkdtemp()
    try:
        # Create a couple of files to throw in the bundle .csv file
        for i in range(3):
            test_csv_path = os.path.join(str(tmpdir), '{}_test.csv'.format(i))
            df = pd.DataFrame({'a': random.randint(0,10,10), 'b': random.randint(10)})
            df.to_csv(test_csv_path)
            assert os.path.exists(test_csv_path)

        # Add the file to the bundle.  Data is list[filepath,...]
        api.add(TEST_CONTEXT, TEST_BUNDLE_NAME, tmpdir)

        # Retrieve the bundle
        bundle_data = api.cat(TEST_CONTEXT, TEST_BUNDLE_NAME)

        # Assert the bundles contain the same data
        for f in bundle_data:
            i = os.path.basename(f).split('_')[0]
            bundle_hash, file_hash = get_hash(f), get_hash(os.path.join(tmpdir, '{}_test.csv'.format(i)))
            assert bundle_hash == file_hash, 'Hashes do not match'
    finally:
        shutil.rmtree(tmpdir)

if __name__ == '__main__':
    pytest.main([__file__])
