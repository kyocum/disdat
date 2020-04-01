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
import pytest

from tests.functional.common import run_test, TEST_CONTEXT # autouse fixture to setup / tear down context


def test(run_test):
    """ Purpose of this test is to have one task that produces a bundle.
    And another task that requires it.

    1.) Run DataMaker which runs PreMaker
    2.) Assert that those ran, and remove PreMaker
    3.) run Root_1 which needs DataMaker (external dep) and PreMaker
    4.) assert that premaker re-ran and root ran successfully (getting external dependency)

    """

    api.context(TEST_CONTEXT)

    api.apply(TEST_CONTEXT, DataMaker, params={'int_array': [1000, 2000, 3000]})

    b = api.get(TEST_CONTEXT, 'PreMaker')
    assert(b is not None)
    pm_uuid = b.uuid
    b.rm()

    api.apply(TEST_CONTEXT, Root_1)

    b = api.get(TEST_CONTEXT, 'PreMaker')
    assert(b is not None)
    assert(b.uuid != pm_uuid)

    b = api.get(TEST_CONTEXT, 'Root_1')
    assert(b is not None)

    api.delete_context(TEST_CONTEXT)


class DataMaker(PipeTask):
    """ Run this by itself.
    Then B requires DataMaker as external, and A. """

    int_array = luigi.ListParameter(default=[1, 2, 3, 5, 8])

    def pipe_requires(self):
        self.set_bundle_name("DataMaker")
        self.add_dependency('premaker', PreMaker, params={})
        return

    def pipe_run(self, premaker=None):

        return np.array(self.int_array)


class PreMaker(PipeTask):

    printme = luigi.Parameter(default="snarky")

    def pipe_requires(self):
        return

    def pipe_run(self):

        print("Task premaker says {}".format(self.printme))

        return pd.DataFrame({'fark': np.random.randint(100, size=10), 'bark': np.random.randint(10, size=10)})


class Root_1(PipeTask):

    def pipe_requires(self):
        self.add_dependency('premaker', PreMaker, params={})
        self.add_external_dependency('datamaker', DataMaker, {'int_array': [1000, 2000, 3000]})

    def pipe_run(self, premaker=None, datamaker=None):
        print ("Root received a datamaker {}".format(datamaker))
        return


if __name__ == '__main__':
    pytest.main([__file__])
    #test()
