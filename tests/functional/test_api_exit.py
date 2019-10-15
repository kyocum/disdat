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

from disdat.pipe import PipeTask
import disdat.api as api
import luigi

from tests.functional.common import TEST_CONTEXT

TEST_NAME    = 'test_bundle'


def test():
    """ Purpose of this test is to have one task that produces a bundle.
    And another task that requires it.

    1.) Create external dep -- also creates PreMaker_auf_datamaker
    dsdt apply - - test_external_bundle.DataMaker --int_array '[1000,2000,3000]'

    2.) Remove Premaker_auf_datamaker
    dsdt rm PreMaker_auf_datamaker

    3.) Try to run Root -- it should find DataMaker but not re-create it or PreMaker_auf_datamaker

    """

    api.context(TEST_CONTEXT)

    result = None
    try:
        result = api.apply(TEST_CONTEXT, Root2, output_bundle='test_api_exit', params={}, force=True, workers=2)
    except Exception as e:
        print ("Got exception {} result {} ".format(e, e.result))
        assert(e.result['did_work'])
        assert(not e.result['success'])
    finally:
        print("API apply returned {}".format(result))


class FailBate(PipeTask):
    """
    Generate a small data set of possible basketball scores
    """
    unique = luigi.Parameter()

    def pipe_requires(self):
        self.set_bundle_name("GenData")

    def pipe_run(self):

        if self.unique == 1:
            print("Task about to fail . . . ")
            _ = 100 / 0
        elif self.unique == 0:
            pass

        return


class Root2(PipeTask):
    """
    Average scores of an upstream task
    """

    def pipe_requires(self):
        """ Depend on GenData """
        self.add_dependency('task_succeeds', FailBate, {'unique': 0})
        self.add_dependency('task_fails', FailBate, {'unique': 1})

    def pipe_run(self, **kwargs):
        """ Compute average and return as a dictionary """
        return True


if __name__ == "__main__":
    test()
