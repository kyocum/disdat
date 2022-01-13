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

import numpy as np
import pandas as pd
import pytest
import six

import disdat.api as api
from tests.functional.common import TEST_CONTEXT


@pytest.fixture(autouse=True)
def run_test():
    # Remove test context before running test
    setup()
    yield


def setup():
    if TEST_CONTEXT in api.ls_contexts():
        api.delete_context(context_name=TEST_CONTEXT)

    api.context(context_name=TEST_CONTEXT)


def test_int_task():
    assert len(api.search(TEST_CONTEXT)) == 0, 'Context should be empty'

    _ = api.Bundle(TEST_CONTEXT, name='int_task', data=1)
    data = api.get(TEST_CONTEXT, 'int_task').data

    assert data == 1, 'Data did not match output'
    assert type(data) == int, 'Data is not int'
    assert len(api.search(TEST_CONTEXT)) == 1, 'Only one bundle should be present'


def test_string_task():
    assert len(api.search(TEST_CONTEXT)) == 0, 'Context should be empty'

    _ = api.Bundle(TEST_CONTEXT, name='string_task', data='output')
    data = api.get(TEST_CONTEXT, 'string_task').data

    assert data == 'output', 'Data did not match output'
    assert type(data) == six.text_type, 'Data is not string'
    assert len(api.search(TEST_CONTEXT)) == 1, 'Only one bundle should be present'


def test_float_task():
    assert len(api.search(TEST_CONTEXT)) == 0, 'Context should be empty'

    _ = api.Bundle(TEST_CONTEXT, name='float_task', data=2.5)
    data = api.get(TEST_CONTEXT, 'float_task').data

    assert data == 2.5, 'Data did not match output'
    assert type(data) == float, 'Data is not float'
    assert len(api.search(TEST_CONTEXT)) == 1, 'Only one bundle should be present'


def test_list_task():
    assert len(api.search(TEST_CONTEXT)) == 0, 'Context should be empty'

    _ = api.Bundle(TEST_CONTEXT, name='list_task', data=[1, 2, 3])
    data = api.get(TEST_CONTEXT, 'list_task').data

    assert np.array_equal(data, [1, 2, 3]), 'Data did not match output'
    assert type(data) == np.ndarray, 'Data is not list'
    assert len(api.search(TEST_CONTEXT)) == 1, 'Only one bundle should be present'


def test_df_task():
    assert len(api.search(TEST_CONTEXT)) == 0, 'Context should be empty'

    df = pd.DataFrame()
    df['a'] = [1, 2, 3]

    _ = api.Bundle(TEST_CONTEXT, name='df_task', data=df)
    data = api.get(TEST_CONTEXT, 'df_task').data

    assert df.equals(data), 'Data did not match output'
    assert type(data) == pd.DataFrame, 'Data is not df'
    assert len(api.search(TEST_CONTEXT)) == 1, 'Only one bundle should be present'


def test_file_task():
    assert len(api.search(TEST_CONTEXT)) == 0, 'Context should be empty'

    with api.Bundle(TEST_CONTEXT, name='file_task') as b:
        f1 = b.get_file("test.txt")
        with open(f1, mode='w') as f:
            f.write('5')
        b.add_data(f1)

    output_path = api.get(TEST_CONTEXT, 'file_task').data

    with open(output_path) as f:
        output = f.read()

    assert output == '5', 'Data did not match output'
    assert type(output_path )== str, 'Data is not path'
    assert len(api.search(TEST_CONTEXT)) == 1, 'Only one bundle should be present'


def test_dict_task():
    setup()
    assert len(api.search(TEST_CONTEXT)) == 0, 'Context should be empty'

    d = {'hello': ['world']}
    _ = api.Bundle(TEST_CONTEXT, name='dict_task', data=d)
    d = api.get(TEST_CONTEXT, 'dict_task').data

    assert d == {
    'hello': ['world']
    }, 'Data did not match output'
    assert type(d) == dict, 'Data is not dict'
    assert len(api.search(TEST_CONTEXT)) == 1, 'Only one bundle should be present'


if __name__ == '__main__':
    pytest.main([__file__])
