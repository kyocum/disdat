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
import pytest
from disdat.pipe import PipeTask
import disdat.api as api


class ContextTest(PipeTask):
    def pipe_requires(self, pipeline_input=None):
        self.set_bundle_name('context_test')

    def pipe_run(self, pipeline_input=None):
        return 2


def test_create_context():
    context_name = '__test__'
    assert context_name not in api.ls_contexts(), 'Context exists'

    api.context(context_name)
    assert context_name in api.ls_contexts(), 'Test context does exists'
    api.delete_context(context_name=context_name)
    assert context_name not in api.ls_contexts(), 'Test context exists'


def test_independent_context():
    context_1_name = '__test_context_1__'
    context_2_name = '__test_context_2__'

    api.context(context_1_name)
    api.context(context_2_name)

    api.apply(context_1_name, ContextTest)

    assert len(api.search(context_1_name)) == 1, 'Only one bundle should be in context one'
    assert len(api.search(context_2_name)) == 0, 'Context two should be empty'

    api.delete_context(context_name=context_1_name)
    api.delete_context(context_name=context_2_name)

    assert context_1_name not in api.ls_contexts(), 'Contexts should be removed'
    assert context_2_name not in api.ls_contexts(), 'Contexts should be removed'


if __name__ == '__main__':
    pytest.main([__file__])