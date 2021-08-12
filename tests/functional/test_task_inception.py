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


""" Test ability to call pipelines in pipelines 
Test with workers=1 and workers>1
"""


class A(PipeTask):
    n = luigi.IntParameter()

    def pipe_requires(self):
        self.set_bundle_name("A[{}]".format(self.n))
        self.add_dependency("shark", B, params={'n': self.n+1})

    def pipe_run(self, shark=None):
        return self.n * 2


class B(A):
    def pipe_requires(self):
        self.set_bundle_name("B[{}]".format(self.n))


class Root(PipeTask):
    """
    Call another pipeline internally.
    """
    n = luigi.IntParameter(default=0)
    workers = luigi.IntParameter(default=1)
    inception = luigi.BoolParameter(default=False)

    def pipe_requires(self):
        self.set_bundle_name('root')
        self.add_dependency('a', A, params={'n': self.n+1})
        self.add_dependency('b', B, params={'n': self.n+1})

    def pipe_run(self, a=None, b=None):
        if self.inception:
            api.apply(TEST_CONTEXT, Root, output_bundle="inception_result", params={'n': 100}, workers=self.workers)
            b = api.get(TEST_CONTEXT, "inception_result" )
            return b.data
        else:
            return a + b


def _inception_pipeline(inner_workers, outer_workers):
    assert len(api.search(TEST_CONTEXT)) == 0, 'Context should be empty'
    api.apply(TEST_CONTEXT, Root, output_bundle="test_root", params={'inception': True,
                                                                     'workers': inner_workers},
              workers=outer_workers)

    b = api.get(TEST_CONTEXT, "test_root")

    print("FINAL DATA {}".format(b.data))

def test_inception_1_1():
    _inception_pipeline(1, 1)

def test_inception_3_1():
    _inception_pipeline(3, 1)

def test_inception_1_3():
    _inception_pipeline(1, 3)

def test_inception_3_3():
    _inception_pipeline(3, 3)


if __name__ == '__main__':
    api.delete_context(TEST_CONTEXT)
    api.context(TEST_CONTEXT)
    test_inception_3_3()
    #api.apply(TEST_CONTEXT, A, params={'n': 1})