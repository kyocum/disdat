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
    test_param = luigi.Parameter(default=EXT_TASK_PARAM_VAL)
    throw_assert = luigi.BoolParameter(default=True)

    def pipe_requires(self):
        self.set_bundle_name('pipeline_a')
        b = self.add_external_dependency('ext_input', ExternalPipeline, {'test_param': self.test_param})
        if self.throw_assert:
            assert b is not None
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
        assert b is not None
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
        assert b is not None
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

    api.apply(TEST_CONTEXT, PipelineA)

    result = api.apply(TEST_CONTEXT, PipelineA)
    assert result['success'] is True
    assert result['did_work'] is False


def test_uuid_external_dependency(run_test):

    uuid = create_bundle_from_pipeline()

    api.apply(TEST_CONTEXT, PipelineB, params={'ext_uuid': uuid})

    result = api.apply(TEST_CONTEXT, PipelineB, params={'ext_uuid': uuid})
    assert result['success'] is True
    assert result['did_work'] is False


def test_name_external_dependency(run_test):

    uuid = create_bundle_from_pipeline()

    api.apply(TEST_CONTEXT, PipelineC, params={'ext_name': EXT_BUNDLE_NAME})

    result = api.apply(TEST_CONTEXT, PipelineC, params={'ext_name': EXT_BUNDLE_NAME})
    assert result['success'] is True
    assert result['did_work'] is False


def test_ord_external_dependency_fail(run_test):
    """ Test ability to handle a failed lookup.
    Note: Disdat/Luigi swallows exceptions in tasks.  Here our tasks
    assert that they get back a bundle on their lookup.  If we catch it, then the
    test succeeds.

    Args:
        run_test:

    Returns:

    """

    uuid = create_bundle_from_pipeline()

    result = api.apply(TEST_CONTEXT, PipelineA, params={'test_param': 'never run before',
                                                        'throw_assert': False})

    assert result['success'] is True

    try:
        result = api.apply(TEST_CONTEXT, PipelineA, params={'test_param': 'never run before'})
    except AssertionError as ae:
        print("ERROR: {}".format(ae))
        return


def test_uuid_external_dependency_fail(run_test):
    """ Test ability to handle a failed lookup.
    Note: Disdat/Luigi swallows exceptions in tasks.  Here our tasks
    assert that they get back a bundle on their lookup.  If we catch it, then the
    test succeeds.

    Args:
        run_test:

    Returns:

    """

    uuid = create_bundle_from_pipeline()
    try:
        result = api.apply(TEST_CONTEXT, PipelineB, params={'ext_uuid': 'not a valid uuid'})
    except AssertionError as ae:
        print("ERROR: {}".format(ae))
        return


def test_name_external_dependency_fail(run_test):
    """ Test ability to handle a failed lookup.
    Note: Disdat/Luigi swallows exceptions in tasks.  Here our tasks
    assert that they get back a bundle on their lookup.  If we catch it, then the
    test succeeds.

    Args:
        run_test:

    Returns:

    """

    uuid = create_bundle_from_pipeline()
    try:
        result = api.apply(TEST_CONTEXT, PipelineC, params={'ext_name': 'not a bundle name'})
    except AssertionError as ae:
        print("ERROR: {}".format(ae))
        return


if __name__ == '__main__':
    if False:
        api.delete_context(context_name=TEST_CONTEXT)
        api.context(context_name=TEST_CONTEXT)

        test_ord_external_dependency_fail(run_test)

        api.delete_context(context_name=TEST_CONTEXT)
        api.context(context_name=TEST_CONTEXT)

        test_uuid_external_dependency_fail(run_test)

        api.delete_context(context_name=TEST_CONTEXT)
        api.context(context_name=TEST_CONTEXT)

        test_name_external_dependency_fail(run_test)
    else:
        pytest.main([__file__])
