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

""" Purpose of this test is to show that if you return nothing, you 
still need to get the input in the downstream task.  See git issue 
 https://github.com/kyocum/disdat/issues/31
 """


class Bizarre(PipeTask):

    def pipe_requires(self):
        return

    def pipe_run(self):
        return


class a(Bizarre):
    def pipe_requires(self):
        return


class b(Bizarre):
    def pipe_requires(self):
        self.add_dependency('something', a, {})

