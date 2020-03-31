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

import pytest
import luigi
from disdat.pipe import PipeTask
import disdat.api as api
from tests.functional.common import run_test, TEST_CONTEXT, setup

""" This set of tests verifies the behavior of Disdat's re-execution / data re-use logic.
 
 Luigi has a simple re-run logic: do not re-run tasks when the targets returned by the output function exist.
 
 Note that because task parameters should define up-stream dependencies, a given task should only re-use its result
 when its upstream required tasks have also re-used their results.  If a tasks parameters have not changed, its dependencies
 should not change, those cached results may be used, and so on.  
 
 But tasks with the same parameters may be re-executed for a variety of real-world reasons.  First, the task code may have 
 changed.  Or the task may be reading from an external data source that has changed (a database table).  While this 
 second case should be handled by the programmer (parameterizing by the time of db access), the programmer may forget or 
 the time unit may not be of sufficient granularity.   E.g., parametrize by day read, but the database changes more frequently.
 Or an external up-stream input, produced by another pipeline, has been updated.  
 
 In addition, it is possible for a layer above Disdat (or Luigi), could dynamically create tasks, and parameterize them
 outside of the normal pattern of "right-to-left" or "last task passes parameters to first task" pattern. 
 
 Finally, Disdat de-couples absolute file paths (used in Luigi) from the logical name of a task's output.  What this 
 means is that absolute file paths alone do not dictate whether a task should re-use that output.  Instead, whether any 
 *version* of a parameterized task's output exists determines if it should re-run.  
 
 In all of these cases, the downstream task should be re-run when new versions of upstream outputs exist.  Disdat builds 
 upon Luigi's straightforward logic to determine whether to re-execute or re-use existing outputs in these conditions. 
 
 In particular, Disdat uses the processing_name as the notion of versioning "sameness" for re-use.  The processing name
 is, a summary of the current task's parameters and a summary of the parameters of its upstream tasks.  In essence, it is 
 is analogous to a Merkle Tree, summarizing the state of the world used to run a single task.   
 
 Note that UUIDs are "lineage" names.  They tell us exactly the data used to produce an output.      
  
  Tests: * means new params
  1.) Force and Force all are handled by test_force_one_and_all.py
  2.) Run A, Run A, should re-use
  3.) Run A, Run A*, should re-run
  4.) Run A->B, Re-run A*.  Run A->B, nothing should run. 
  5.) Run A->B, re-run A (force), Run A->B, B should re-run. 
  6.) Run A->B, Re-run A*.  Run A*->B, B should re-run. 
  7.) Run A->B->C,  Run A*->B.   Run A->B->C, nothing should run
  8.) Run A->B->C,  Run A*->B.   Run A*->B->C, C should re-run
  9.) Run A,B -> C(a=A,b=B) Run A,B -> C(a=B,b=A), C should run both times.  
   
 """


def test_A2_A3(run_test):
    """
    2.) Run A, Run A, should re-use
    3.) Run A, Run A*, should re-run
    """

    result = api.apply(TEST_CONTEXT, A)
    assert result['did_work'] is True
    first_A_uuid = api.get(TEST_CONTEXT, 'A').uuid
    result = api.apply(TEST_CONTEXT, A)
    assert result['did_work'] is False
    second_A_uuid = api.get(TEST_CONTEXT, 'A').uuid
    assert first_A_uuid == second_A_uuid
    assert len(api.search(TEST_CONTEXT, 'A')) is 1

    # Mod args, should re-run
    result = api.apply(TEST_CONTEXT, A, params={'a': 2,'b': 3})
    assert result['did_work'] is True
    next_A_uuid = api.get(TEST_CONTEXT, 'A').uuid
    assert next_A_uuid != second_A_uuid
    assert len(api.search(TEST_CONTEXT, 'A')) is 2


def test_AB4(run_test):
    """
    4.) Run A->B, Re-run A*.  Run A->B, nothing should run.
    """

    result = api.apply(TEST_CONTEXT, B)
    assert result['success'] is True
    assert result['did_work'] is True

    result = api.apply(TEST_CONTEXT, A, params={'a':2, 'b':3})
    assert result['success'] is True
    assert result['did_work'] is True

    result = api.apply(TEST_CONTEXT, B)
    assert result['success'] is True
    assert result['did_work'] is False


def test_AB5(run_test):
    """
    5.) Run A->B, re-run A (force), Run A->B, B should re-run.
    """

    result = api.apply(TEST_CONTEXT, B)
    assert result['success'] is True
    assert result['did_work'] is True

    result = api.apply(TEST_CONTEXT, A, force=True)
    assert result['success'] is True
    assert result['did_work'] is True

    result = api.apply(TEST_CONTEXT, B)
    assert result['success'] is True
    assert result['did_work'] is True


def test_AB6(run_test):
    """
    6.) Run A->B, Re-run A*.  Run A*->B, B should re-run.

    Args:
        run_test:

    Returns:

    """

    result = api.apply(TEST_CONTEXT, B)
    assert result['success'] is True
    assert result['did_work'] is True
    B_uuid = api.get(TEST_CONTEXT, 'B').uuid

    result = api.apply(TEST_CONTEXT, APrime)
    assert result['success'] is True
    assert result['did_work'] is True
    APrime_uuid = api.get(TEST_CONTEXT, 'APrime').uuid

    def custom_B_requires(self):
        self.add_dependency('a', APrime, params={})

    old_requires = B.pipe_requires
    B.pipe_requires = custom_B_requires

    result = api.apply(TEST_CONTEXT, B)
    assert result['success'] is True
    assert result['did_work'] is True
    assert APrime_uuid == api.get(TEST_CONTEXT, 'APrime').uuid
    assert B_uuid != api.get(TEST_CONTEXT, 'B').uuid

    B.pipe_requires = old_requires


def test_ABC7(run_test):
    """
    7.) Run A->B->C,  Run A*->B.   Run A->B->C, nothing should run

    Args:
        run_test:

    Returns:

    """

    result = api.apply(TEST_CONTEXT, C)
    assert result['success'] is True
    assert result['did_work'] is True
    B_uuid = api.get(TEST_CONTEXT, 'B').uuid

    def custom_B_requires(self):
        self.add_dependency('a', APrime, params={})

    old_requires = B.pipe_requires
    B.pipe_requires = custom_B_requires

    result = api.apply(TEST_CONTEXT, B)
    assert result['success'] is True
    assert result['did_work'] is True
    assert B_uuid != api.get(TEST_CONTEXT, 'B').uuid  # should have a new B

    B.pipe_requires = old_requires

    result = api.apply(TEST_CONTEXT, C)
    assert result['success'] is True
    assert result['did_work'] is False


def test_ABC8(run_test):
    """
    8.) Run A->B->C,  Run A*->B.   Run A*->B->C, C should re-run

    Args:
        run_test:

    Returns:

    """

    result = api.apply(TEST_CONTEXT, C)
    assert result['success'] is True
    assert result['did_work'] is True
    B_uuid = api.get(TEST_CONTEXT, 'B').uuid

    def custom_B_requires(self):
        self.add_dependency('a', APrime, params={})

    old_requires = B.pipe_requires
    B.pipe_requires = custom_B_requires

    result = api.apply(TEST_CONTEXT, B)
    assert result['success'] is True
    assert result['did_work'] is True
    assert B_uuid != api.get(TEST_CONTEXT, 'B').uuid  # should have a new B
    B_uuid = api.get(TEST_CONTEXT, 'B').uuid
    APrime_uuid = api.get(TEST_CONTEXT, 'APrime').uuid

    result = api.apply(TEST_CONTEXT, C)
    assert result['success'] is True
    assert result['did_work'] is True
    assert B_uuid == api.get(TEST_CONTEXT, 'B').uuid
    assert APrime_uuid == api.get(TEST_CONTEXT, 'APrime').uuid

    B.pipe_requires = old_requires


def test_bundle_depsABC9(run_test):
    """
    10.) Run A,B -> C(a=A,b=B) Run A,B -> C(a=B,b=A), C should run both times.

    Args:
        run_test:

    Returns:

    """

    def custom_C_requires(self):
        self.add_dependency('a', A, params={})
        self.add_dependency('b', B, params={})

    old_requires = C.pipe_requires
    C.pipe_requires = custom_C_requires

    result = api.apply(TEST_CONTEXT, C)
    assert result['success'] is True
    assert result['did_work'] is True
    A_uuid = api.get(TEST_CONTEXT, 'A').uuid
    B_uuid = api.get(TEST_CONTEXT, 'B').uuid
    C_uuid = api.get(TEST_CONTEXT, 'C').uuid

    def custom_C_requires_swap(self):
        self.add_dependency('a', B, params={})
        self.add_dependency('b', A, params={})

    C.pipe_requires = custom_C_requires_swap

    result = api.apply(TEST_CONTEXT, C)
    assert result['success'] is True
    assert result['did_work'] is True
    assert A_uuid == api.get(TEST_CONTEXT, 'A').uuid
    assert B_uuid == api.get(TEST_CONTEXT, 'B').uuid
    assert C_uuid != api.get(TEST_CONTEXT, 'C').uuid

    C.pipe_requires = old_requires


class A(PipeTask):

    a = luigi.IntParameter(default=1)
    b = luigi.IntParameter(default=2)

    def pipe_requires(self):
        pass

    def pipe_run(self):
        return self.a+self.b


class APrime(PipeTask):

    a = luigi.IntParameter(default=2)
    b = luigi.IntParameter(default=1)

    def pipe_requires(self):
        pass

    def pipe_run(self):
        return self.a+self.b


class B(PipeTask):

    a = luigi.IntParameter(default=3)
    b = luigi.IntParameter(default=4)

    def pipe_requires(self):
        self.add_dependency('a', A, params={})

    def pipe_run(self, a):
        return a + self.a + self.b


class C(PipeTask):

    a = luigi.IntParameter(default=5)
    b = luigi.IntParameter(default=6)

    def pipe_requires(self):
        self.add_dependency('b', B, params={})

    def pipe_run(self, **kwargs):
        input_sum = sum([v for v in kwargs.values()])
        return input_sum + self.b + self.a


if __name__ is '__main__':
    pytest.main([__file__])


