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

import luigi

from disdat.pipe import PipeTask
import disdat.api as api
from tests.functional.common import run_test, TEST_CONTEXT


class A(PipeTask):
    def pipe_requires(self, pipeline_input=None):
        self.set_bundle_name('a')

    def pipe_run(self, pipeline_input=None):
        return 2


class B(PipeTask):

    n = luigi.IntParameter()

    def pipe_requires(self, pipeline_input=None):
        self.set_bundle_name('b')

    def pipe_run(self, pipeline_input=None):
        return 2 * self.n


class C(PipeTask):

    n = luigi.IntParameter(default=2)

    def pipe_requires(self, pipeline_input=None):
        self.set_bundle_name('c')
        self.add_dependency('a', A, params={})
        self.add_dependency('b', B, params={'n': self.n})

    def pipe_run(self, pipeline_input=None, a=None, b=None):
        return a + b


def test_single_task():
    assert len(api.search(TEST_CONTEXT)) == 0, 'Context should be empty'

    api.apply(TEST_CONTEXT, A)
    data = api.get(TEST_CONTEXT, 'a').data

    assert data == 2, 'Data did not match output'
    assert type(data) == int, 'Data is not path'
    assert len(api.search(TEST_CONTEXT)) == 1, 'Only one bundle should be present'

    api.apply(TEST_CONTEXT, A)
    assert len(api.search(TEST_CONTEXT)) == 1, 'Only one bundle should be present'


def test_dependant_tasks():
    assert len(api.search(TEST_CONTEXT)) == 0, 'Context should be empty'

    api.apply(TEST_CONTEXT,  C)
    data = api.get(TEST_CONTEXT, 'c').data

    assert data == 6, 'Data did not match output'
    assert type(data) == int, 'Data is not path'
    assert len(api.search(TEST_CONTEXT)) == 3, 'Three bundles should be present'


def test_task_with_parameter():
    assert len(api.search(TEST_CONTEXT)) == 0, 'Context should be empty'

    api.apply(TEST_CONTEXT,  B, params={'n': 10})
    data = api.get(TEST_CONTEXT, 'b').data

    assert data == 20, 'Data did not match output'
    assert type(data) == int, 'Data is not path'
    assert len(api.search(TEST_CONTEXT)) == 1, 'One bundle should be present'

    api.apply(TEST_CONTEXT,  B, params={'n': 20})
    data = api.get(TEST_CONTEXT, 'b').data

    assert data == 40, 'Data did not match output'
    assert type(data) == int, 'Data is not path'
    assert len(api.search(TEST_CONTEXT)) == 2, 'Two bundles should be present'


def test_child_task_with_parameter():
    assert len(api.search(TEST_CONTEXT)) == 0, 'Context should be empty'

    api.apply(TEST_CONTEXT,  C, params={'n': 10})
    data = api.get(TEST_CONTEXT, 'c').data

    assert data == 22, 'Data did not match output'
    assert type(data) == int, 'Data is not path'
    assert len(api.search(TEST_CONTEXT)) == 3, 'Three bundles should be present'

    api.apply(TEST_CONTEXT,  C, params={'n': 20})
    data = api.get(TEST_CONTEXT, 'c').data

    assert data == 42, 'Data did not match output'
    assert type(data) == int, 'Data is not path'
    assert len(api.search(TEST_CONTEXT)) == 5, 'Five bundles should be present'

