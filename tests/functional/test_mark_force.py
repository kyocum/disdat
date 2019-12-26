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
    """ This tests if mark_force works for tasks.
    We have two tasks. One depends on the other.  The upstream is marked
    mark_force and should always run.
    """

    def run_and_get(name, do_ext=False):
        api.apply(TEST_CONTEXT, A_2, params={'set_ext_dep': do_ext})
        b = api.get(TEST_CONTEXT, 'B')
        print ("Run {}: b.creation_date {} b.uuid {}".format(name, b.creation_date, b.uuid))
        return b

    b = run_and_get("One")
    first_uuid = b.uuid

    b = run_and_get("Two")
    assert(first_uuid != b.uuid)
    second_uuid = b.uuid

    b = run_and_get("Three", do_ext=True)
    assert(second_uuid == b.uuid)


class B_2(PipeTask):

    def pipe_requires(self):
        self.set_bundle_name("B")
        self.mark_force()
        return

    def pipe_run(self):
        print ("Task B finished.")

        return True


class A_2(PipeTask):
    set_ext_dep = luigi.BoolParameter(default=False)

    def pipe_requires(self):
        if self.set_ext_dep:
            self.add_external_dependency('B', B_2, params={})
        else:
            self.add_dependency('B', B_2, {})

    def pipe_run(self, B=None):
        print ("Task A finished.")
        return


if __name__ == '__main__':
    test()
