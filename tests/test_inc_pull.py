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
"""
Test Incremental Pull

Use API to create a bundle with some files
push to remote context (test assumes that you have AWS credentials for an account.

author: Kenneth Yocum
"""

import getpass
from disdat.pipe import PipeTask
import disdat.api as api


TEST_CONTEXT = '_test_context_'
TEST_NAME    = 'test_bundle'
REMOTE_URL   = 's3://disdat-prod-486026647292-us-west-2/beta/'


def test():
    """

    Returns:

    """

    api.context(TEST_CONTEXT)
    api.remote(TEST_CONTEXT, TEST_CONTEXT, REMOTE_URL, force=True)

    with api.Bundle(TEST_CONTEXT, TEST_NAME, owner=getpass.getuser()) as b:
        for i in range(3):
            with b.add_file('output_{}'.format(i)).open('w') as of:
                of.write("some text for the {} file".format(i))

    b.commit().push()

    b.rm()

    b.pull(localize=False)

    api.apply(TEST_CONTEXT, '-', 'test_output', 'ConsumeExtDep', incremental_pull=True)

    api.delete_context(TEST_CONTEXT, remote=True)


class ConsumeExtDep(PipeTask):
    """ Consume files from an external dependency
    """

    def pipe_requires(self, pipeline_input=None):
        """ We depend on a manually created bundle that
        is parameterized by its name and its owner
        """
        self.add_external_dependency("input_files",
                                     api.BundleWrapperTask,
                                     {'name': TEST_NAME,
                                      'owner': getpass.getuser()})

    def pipe_run(self, pipeline_input=None, input_files=None):
        """ For each file, print out its name and contents.
        """
        max_len = 0
        nfiles = 0
        for v in input_files:
            with open(v, 'r') as of:
                s = of.read()
                if len(s) > max_len:
                    max_len = len(s)
                print "Reading file: {} length:{}".format(v, len(s))
                nfiles += 1

        return {'num categories': [len(input_files)], 'num files': [nfiles], 'max string': [max_len]}


if __name__ == "__main__":
    test()
