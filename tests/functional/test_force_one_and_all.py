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

TEST_NAME    = 'test_bundle'


def test(run_test):
    """ This tests if apply force=True and force_all=True re-run everything.
    We have two tasks. One depends on the other.
    force_all should re-run both, force should re-run only the last.
    """

    # first run there should be no bundles
    #assert len(api.search(TEST_CONTEXT)) == 0, 'Context should be empty'
    api.apply(TEST_CONTEXT, A, params={})
    first_B_uuid = api.get(TEST_CONTEXT, 'B').uuid
    first_A_uuid = api.get(TEST_CONTEXT, 'A').uuid

    # second, force re-run last task
    api.apply(TEST_CONTEXT, A, force=True, params={})
    one_B_uuid = api.get(TEST_CONTEXT, 'B').uuid
    one_A_uuid = api.get(TEST_CONTEXT, 'A').uuid
    assert(first_B_uuid == one_B_uuid)
    assert(first_A_uuid != one_A_uuid)

    # second, force all to re-run.
    api.apply(TEST_CONTEXT, A, force_all=True, params={})
    all_B_uuid = api.get(TEST_CONTEXT, 'B').uuid
    all_A_uuid = api.get(TEST_CONTEXT, 'A').uuid
    assert(all_B_uuid != one_B_uuid)
    assert(all_A_uuid != one_A_uuid)

    # third, make sure a force_all doesn't crash if there is an external bundle.
    api.apply(TEST_CONTEXT, A, force_all=True, params={'set_ext_dep': True})
    final_B_uuid = api.get(TEST_CONTEXT, 'B').uuid
    final_A_uuid = api.get(TEST_CONTEXT, 'A').uuid
    assert(final_B_uuid == all_B_uuid)
    assert(final_A_uuid != all_A_uuid)


class B(PipeTask):

    def pipe_requires(self):
        self.set_bundle_name("B")
        return

    def pipe_run(self):
        print ("Task B finished.")

        return True


class A(PipeTask):
    set_ext_dep = luigi.BoolParameter(default=False)

    def pipe_requires(self):
        self.set_bundle_name("A")
        if self.set_ext_dep:
            self.add_external_dependency('B', B, params={})
        else:
            self.add_dependency('B', B, {})

    def pipe_run(self, B=None):
        print ("Task A finished.")
        return


if __name__ == '__main__':
    test()
