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

from disdat.pipe import PipeTask
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


# Test Return Types
class IntTask(PipeTask):
    def pipe_requires(self, pipeline_input=None):
        self.set_bundle_name('int_task')

    def pipe_run(self, pipeline_input=None):
        return 1


def test_int_task():
    assert len(api.search(TEST_CONTEXT)) == 0, 'Context should be empty'

    api.apply(TEST_CONTEXT, IntTask)
    data = api.get(TEST_CONTEXT, 'int_task').data

    assert data == 1, 'Data did not match output'
    assert type(data) == int, 'Data is not int'
    assert len(api.search(TEST_CONTEXT)) == 1, 'Only one bundle should be present'


class StringTask(PipeTask):
    def pipe_requires(self, pipeline_input=None):
        self.set_bundle_name('string_task')

    def pipe_run(self, pipeline_input=None):
        return 'output'


def test_string_task():
    assert len(api.search(TEST_CONTEXT)) == 0, 'Context should be empty'

    api.apply(TEST_CONTEXT, StringTask)
    data = api.get(TEST_CONTEXT, 'string_task').data

    assert data == 'output', 'Data did not match output'
    assert type(data) == six.text_type, 'Data is not string'
    assert len(api.search(TEST_CONTEXT)) == 1, 'Only one bundle should be present'


class FloatTask(PipeTask):
    def pipe_requires(self, pipeline_input=None):
        self.set_bundle_name('float_task')

    def pipe_run(self, pipeline_input=None):
        return 2.5


def test_float_task():
    assert len(api.search(TEST_CONTEXT)) == 0, 'Context should be empty'

    api.apply(TEST_CONTEXT, FloatTask)
    data = api.get(TEST_CONTEXT, 'float_task').data

    assert data == 2.5, 'Data did not match output'
    assert type(data) == float, 'Data is not float'
    assert len(api.search(TEST_CONTEXT)) == 1, 'Only one bundle should be present'


class ListTask(PipeTask):
    def pipe_requires(self, pipeline_input=None):
        self.set_bundle_name('list_task')

    def pipe_run(self, pipeline_input=None):
        return [1, 2, 3]


def test_list_task():
    assert len(api.search(TEST_CONTEXT)) == 0, 'Context should be empty'

    api.apply(TEST_CONTEXT, ListTask)
    data = api.get(TEST_CONTEXT, 'list_task').data

    assert np.array_equal(data, [1, 2, 3]), 'Data did not match output'
    assert type(data) == np.ndarray, 'Data is not list'
    assert len(api.search(TEST_CONTEXT)) == 1, 'Only one bundle should be present'


class DataFrameTask(PipeTask):
    def pipe_requires(self, pipeline_input=None):
        self.set_bundle_name('df_task')

    def pipe_run(self, pipeline_input=None):
        df = pd.DataFrame()
        df['a'] = [1, 2, 3]
        return df


def test_df_task():
    assert len(api.search(TEST_CONTEXT)) == 0, 'Context should be empty'

    api.apply(TEST_CONTEXT, DataFrameTask)
    data = api.get(TEST_CONTEXT, 'df_task').data

    df = pd.DataFrame()
    df['a'] = [1, 2, 3]

    assert df.equals(data), 'Data did not match output'
    assert type(data) == pd.DataFrame, 'Data is not df'
    assert len(api.search(TEST_CONTEXT)) == 1, 'Only one bundle should be present'


class FileTask(PipeTask):
    def pipe_requires(self, pipeline_input=None):
        self.set_bundle_name('file_task')

    def pipe_run(self, pipeline_input=None):
        target = self.create_output_file('test.txt')
        with target.open('w') as of:
            of.write('5')

        return target


def test_file_task():
    assert len(api.search(TEST_CONTEXT)) == 0, 'Context should be empty'

    api.apply(TEST_CONTEXT, FileTask)
    output_path = api.get(TEST_CONTEXT, 'file_task').data

    with open(output_path) as f:
        output = f.read()

    assert output == '5', 'Data did not match output'
    assert type(output_path )== str, 'Data is not path'
    assert len(api.search(TEST_CONTEXT)) == 1, 'Only one bundle should be present'


class DictTask(PipeTask):
    def pipe_requires(self, pipeline_input=None):
        self.set_bundle_name('dict_task')

    def pipe_run(self, pipeline_input=None):
        return {
            'hello': ['world']
        }


def test_dict_task():
    setup()
    assert len(api.search(TEST_CONTEXT)) == 0, 'Context should be empty'

    api.apply(TEST_CONTEXT, DictTask)
    data = api.get(TEST_CONTEXT, 'dict_task').data

    assert data == {
    'hello': ['world']
    }, 'Data did not match output'
    assert type(data) == dict, 'Data is not dict'
    assert len(api.search(TEST_CONTEXT)) == 1, 'Only one bundle should be present'


if __name__ == '__main__':
    pytest.main([__file__])
