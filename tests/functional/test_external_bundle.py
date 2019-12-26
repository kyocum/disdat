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
import pandas as pd
import numpy as np
from disdat.pipe import PipeTask
import disdat.api as api


TEST_CONTEXT = '_test_context_'
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

    api.apply(TEST_CONTEXT, DataMaker, params={'int_array': [1000, 2000, 3000]})

    b = api.get(TEST_CONTEXT, 'PreMaker_auf_datamaker')

    assert(b is not None)

    b.rm()

    api.apply(TEST_CONTEXT, Root_1)

    b = api.get(TEST_CONTEXT, 'PreMaker_auf_root')

    assert(b is not None)

    api.delete_context(TEST_CONTEXT)


class DataMaker(PipeTask):
    """ Run this by itself.
    Then B requires DataMaker as external, and A. """

    int_array = luigi.ListParameter(default=[1, 2, 3, 5, 8])

    def pipe_requires(self):
        self.set_bundle_name("DataMaker")
        self.add_dependency('premaker', PreMaker, {'printme': "auf_datamaker"})
        return

    def pipe_run(self, premaker=None):

        return np.array(self.int_array)


class PreMaker(PipeTask):

    printme = luigi.Parameter(default="snarky")

    def pipe_requires(self):
        self.set_bundle_name("PreMaker_{}".format(self.printme))
        return

    def pipe_run(self):

        print("Task premaker says {}".format(self.printme))

        return pd.DataFrame({'fark': np.random.randint(100, size=10), 'bark': np.random.randint(10, size=10)})


class Root_1(PipeTask):

    def pipe_requires(self):
        self.add_dependency('premaker', PreMaker, {'printme': "auf_root"})
        self.add_external_dependency('datamaker', DataMaker, {'int_array': [1000, 2000, 3000]})

    def pipe_run(self, premaker=None, datamaker=None):

        print ("Root received a datamaker {}".format(datamaker))

        return


if __name__ == '__main__':
    test()
