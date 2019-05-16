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

import disdat.api as api
from tests.functional.common import TEST_CONTEXT
import os
import hashlib
import pandas as pd

TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../data')


def gen_checksums(path):

    file_hashes = {}

    for root, dirs, files in os.walk(path, topdown=True):
        dst_basepath = root.replace(path, '')

        for f in files:
            fqp = os.path.join(root, f)
            file_hashes[os.path.join(dst_basepath, f).lstrip('/')] = hashlib.md5(open(fqp,'rb').read()).hexdigest()

    return file_hashes


def check_array(b_data, file_hashes):
    """

    Args:
        b_data:

    Returns:

    """

    if len(b_data) == 1:
        common_prefix = os.path.dirname(os.path.commonprefix(b_data))
    else:
        common_prefix = os.path.commonprefix(list(b_data))

    for fqp in b_data:
        hash = hashlib.md5(open(fqp,'rb').read()).hexdigest()
        local_path = fqp.replace(common_prefix, '').lstrip('/')
        assert(hash == file_hashes[local_path])


def test():
    """ Use add to
    a.) add a single file [file]
    b.) add a directory of files (with a subdirectory with files)
    c.) add a csv file that is a bundle
    """

    file_hashes = gen_checksums(TEST_DATA_DIR)

    api.context(TEST_CONTEXT)

    api.add(TEST_CONTEXT, 'test_add_file', os.path.join(TEST_DATA_DIR, 'US_chronic_disease_indicators_CDI_2012.csv'))
    api.add(TEST_CONTEXT, 'test_add_dir', TEST_DATA_DIR)
    api.add(TEST_CONTEXT, 'test_add_bundle', os.path.join(TEST_DATA_DIR, 'test_bundle_file.csv'), treat_file_as_bundle=True)

    b = api.get(TEST_CONTEXT, 'test_add_file')
    check_array(b.data, file_hashes)

    b = api.get(TEST_CONTEXT, 'test_add_dir')
    check_array(b.data, file_hashes)

    b = api.get(TEST_CONTEXT, 'test_add_bundle')
    assert(isinstance(b.data, pd.DataFrame))

    api.delete_context(TEST_CONTEXT)


if __name__ == "__main__":
    test()


