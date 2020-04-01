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
import pytest

from disdat.pipe import PipeTask
import disdat.api as api
from tests.functional.common import run_test, TEST_CONTEXT # autouse fixture to setup / tear down context

EXT_BUNDLE_NAME='ext_bundle_human_name'
BUNDLE_CONTENTS=list(range(9))
EXT_TASK_PARAM_VAL='this is a test value'


class ExternalPipeline(PipeTask):
    test_param = luigi.Parameter()

    """ External Pipeline """
    def pipe_requires(self):
        self.set_bundle_name('external_pipeline')

    def pipe_run(self):
        print ("ExternalPipeline called with parameter [{}]".format(self.test_param))
        return BUNDLE_CONTENTS


class PipelineA(PipeTask):

    def pipe_requires(self):
        self.set_bundle_name('pipeline_a')
        b = self.add_external_dependency('ext_input', ExternalPipeline, {'test_param': EXT_TASK_PARAM_VAL})
        assert list(b.data) == BUNDLE_CONTENTS

    def pipe_run(self, ext_input=None):
        assert list(ext_input) == BUNDLE_CONTENTS
        return True


class PipelineB(PipeTask):
    ext_uuid = luigi.Parameter()

    def pipe_requires(self):
        self.set_bundle_name('pipeline_b')
        b = self.add_external_dependency('ext_input',
                                         ExternalPipeline,
                                         {},
                                         uuid=self.ext_uuid)
        assert list(b.data) == BUNDLE_CONTENTS

    def pipe_run(self, ext_input=None):
        assert list(ext_input) == BUNDLE_CONTENTS
        return True


class PipelineC(PipeTask):
    ext_name = luigi.Parameter()

    def pipe_requires(self):
        self.set_bundle_name('pipeline_b')
        b = self.add_external_dependency('ext_input',
                                         ExternalPipeline,
                                         {},
                                         human_name=self.ext_name)
        assert list(b.data) == BUNDLE_CONTENTS

    def pipe_run(self, ext_input=None):
        assert list(ext_input) == BUNDLE_CONTENTS
        return True


def create_bundle_from_pipeline():
    """ Run the internal pipeline, create a bundle, return the uuid
    """

    api.apply(TEST_CONTEXT,
              ExternalPipeline,
              params={'test_param': EXT_TASK_PARAM_VAL},
              output_bundle=EXT_BUNDLE_NAME)
    b = api.get(TEST_CONTEXT, EXT_BUNDLE_NAME)
    return b.uuid


def test_ord_external_dependency(run_test):

    uuid = create_bundle_from_pipeline()

    print ("UUID of created bundle is {}".format(uuid))

    # Ordinary ext dep
    api.apply(TEST_CONTEXT, PipelineA)


def test_uuid_external_dependency(run_test):

    uuid = create_bundle_from_pipeline()

    print ("UUID of created bundle is {}".format(uuid))

    # Ext dep by specific UUID
    api.apply(TEST_CONTEXT, PipelineB, params={'ext_uuid': uuid})


def test_name_external_dependency(run_test):

    uuid = create_bundle_from_pipeline()

    print ("UUID of created bundle is {}".format(uuid))

    # Ext dep by human name
    api.apply(TEST_CONTEXT, PipelineC, params={'ext_name': EXT_BUNDLE_NAME})


if __name__ == '__main__':
    #api.context(context_name=TEST_CONTEXT)
    #try:
    #    test_external_dependency()
    #finally:
    #    api.delete_context(context_name=TEST_CONTEXT)
    pytest.main([__file__])
