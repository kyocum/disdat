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
import luigi

""" These are simple tasks used for test_api_run """

COMMON_DEFAULT_ARGS=[10, 100, 1000]


class B(PipeTask):
    """ B required by A """
    int_array = luigi.ListParameter(default=None)

    def pipe_run(self):
        print ("B saving type [{}]".format(type(self.int_array)))
        return self.int_array


class A(PipeTask):
    """ A is the root task"""
    int_array = luigi.ListParameter(default=COMMON_DEFAULT_ARGS)

    def pipe_requires(self):
        self.add_dependency('b', B, {'int_array': self.int_array})

    def pipe_run(self, b=None):
        print ("Saving the sum of B {}".format(b))
        print ("A got type [{}]".format(type(b)))
        return sum(list(b))
