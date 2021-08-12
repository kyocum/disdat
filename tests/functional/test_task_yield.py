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


""" Tests for dynamic dependencies using Disdat-Luigi

Run tests with 
A.) 1 or more yields
B.) yield of lists
C.) 1 or more workers 
D.) Mixed with other dependencies
E.) Mixed with other upstream yield dependencies 

Need to check lineage as well


"""


class Static(PipeTask):
    n = luigi.IntParameter()
    dyn_yield = luigi.BoolParameter(default=False)

    def pipe_requires(self):
        self.set_bundle_name("Static[{}]".format(self.n))

    def pipe_run(self):
        if self.dyn_yield:
            result = self.yield_dependency(Yielded, params={'n': self.n, 'source': "Static{}Source".format(self.n)})
            yield result
        return self.n * 2


class Yielded(PipeTask):
    n = luigi.IntParameter()
    source = luigi.Parameter(default="RootSource")
    dyn_yield = luigi.BoolParameter(default=False)

    def pipe_requires(self, pipeline_input=None):
        self.set_bundle_name('Yielded{}{}'.format(self.source, self.n))

    def pipe_run(self, pipeline_input=None):
        if self.dyn_yield:
            result = self.yield_dependency(Yielded, params={'n': self.n, 'source': "Yielded{}Source".format(self.n)})
            yield result
        return self.n


class Root(PipeTask):
    """
    Dynamically yield N tasks
    """
    n = luigi.IntParameter(default=0)
    static_deps = luigi.BoolParameter(default=False)
    static_dynamic_deps = luigi.BoolParameter(default=False)

    def pipe_requires(self):
        self.set_bundle_name('root')
        if self.static_deps:
            self.add_dependency('a', Static, params={'n': 0, 'dyn_yield': self.static_dynamic_deps})
            self.add_dependency('b', Static, params={'n': 1, 'dyn_yield': self.static_dynamic_deps})

    def pipe_run(self, a=None, b=None):
        results = []
        for i in range(self.n):
            results.append(self.yield_dependency(Yielded, params={'n': i}))
            yield results[-1]

        print (sum(d.pipe_output for d in results))
        return sum(d.pipe_output for d in results)


class RootYieldList(Root):
    def pipe_run(self, a=None, b=None):
        results = []
        for i in range(self.n):
            results.append(self.yield_dependency(Yielded, params={'n': i}))
        yield results
        print (sum(d.pipe_output for d in results))
        return sum(d.pipe_output for d in results)


def _yield_pipeline(count, workers, roottask, static_deps=False, static_dynamic=False):
    assert len(api.search(TEST_CONTEXT)) == 0, 'Context should be empty'
    api.apply(TEST_CONTEXT, roottask, output_bundle="test_root", params={'n': count,
                                                                         'static_deps': static_deps,
                                                                         'static_dynamic_deps': static_dynamic},
              workers=workers)
    b = api.get(TEST_CONTEXT, "test_root")
    assert b.data == sum(i for i in range(count))


"""  One-by-one yield, with many yields, with many workers """

def test_simple_yield_one():
    _yield_pipeline(1, 1, Root, False)

def test_simple_yield_one_mp():
    _yield_pipeline(1, 3, Root, False)

def test_simple_yield_many():
    _yield_pipeline(10, 1, Root, False)

def test_simple_yield_many_mp():
    _yield_pipeline(10, 3, Root, False)


"""  One-by-one yield, with many yields, with many workers, with static dependencies """

def test_simple_yield_one_statics():
    _yield_pipeline(1, 1, Root, True)

def test_simple_yield_one_mp_statics():
    _yield_pipeline(1, 3, Root, True)

def test_simple_yield_many_statics():
    _yield_pipeline(10, 1, Root, True)

def test_simple_yield_many_mp_statics():
    _yield_pipeline(10, 3, Root, True)

def test_simple_yield_many_mp_statics_dynamic():
    _yield_pipeline(4, 3, Root, True, True)


"""  One-by-one yield, with many yields, with many workers, with static dependencies """

def test_list_yield_one():
    _yield_pipeline(1, 1, RootYieldList, False)

def test_list_yield_one_mp():
    _yield_pipeline(1, 3, RootYieldList, False)

def test_list_yield_many():
    _yield_pipeline(10, 1, RootYieldList, False)

def test_list_yield_many_mp():
    _yield_pipeline(10, 3, RootYieldList, False)


"""  One-by-one yield, with many yields, with many workers, with static dependencies """

def test_list_yield_one_statics():
    _yield_pipeline(1, 1, RootYieldList, True)

def test_list_yield_one_mp_statics():
    _yield_pipeline(1, 3, RootYieldList, True)

def test_list_yield_many_statics():
    _yield_pipeline(10, 1, RootYieldList, True)

def test_list_yield_many_mp_statics():
    _yield_pipeline(10, 3, RootYieldList, True)

def test_list_yield_many_mp_statics_dynamic():
    _yield_pipeline(4, 3, RootYieldList, True, True)


if __name__ == '__main__':
    api.delete_context(TEST_CONTEXT)
    api.context(TEST_CONTEXT)
    #test_simple_yield_one()
    test_simple_yield_many_mp_statics_dynamic()


    api.delete_context(TEST_CONTEXT)
    api.context(TEST_CONTEXT)
    #test_list_yield_many()
    test_list_yield_many_mp_statics_dynamic()
