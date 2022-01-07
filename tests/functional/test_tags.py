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

import uuid
import pytest

from disdat import api
from disdatluigi.pipe import PipeTask
from disdatluigi import common

TAGS = {'tag1': 'omg', 'tag2': 'it works'}


class Source(PipeTask):

    def pipe_requires(self):
        self.set_bundle_name('tagged')

    def pipe_run(self):
        self.add_tags(TAGS)
        return 0


class Destination(PipeTask):

    def pipe_requires(self):
        self.set_bundle_name('output')
        self.add_dependency('tagged', Source, params={})

    def pipe_run(self, tagged):
        tags = self.get_tags('tagged')
        assert tags is not TAGS
        assert tags == TAGS
        return 1


@pytest.fixture
def context():

    try:
        print('ensuring disdat is initialized')
        common.DisdatLuigiConfig.init()
    except:
        print('disdat already initialized, no worries...')

    print('creating temporary local context')
    context = uuid.uuid1().hex
    api.context(context)

    yield context

    print('deleting temporary local context')
    api.delete_context(context)


class TestContext:

    def test_tags(self, context):
        api.apply(context, Destination)


if __name__ == '__main__':
    pytest.main([__file__])
