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

from disdat.pipe import PipeTask
import disdat.api as api
import luigi


TEST_CONTEXT = '_test_context_'
TEST_NAME    = 'example_bundle'
REMOTE_URL   = 's3://disdat-prod-486026647292-us-west-2/beta/'


"""
Bundles have
human_name
processing_name
set of tags
date


"""


def test():
    """

    Returns:

    """

    contexts = api.ls_contexts()

    if TEST_CONTEXT in contexts:
        print "Test incremental pull failed as test context [{}] already exists.".format(TEST_CONTEXT)

    api.context(TEST_CONTEXT)
    api.remote(TEST_CONTEXT, TEST_CONTEXT, REMOTE_URL, force=True)

    # Make Bundle, add files, close
    with api.Bundle(TEST_CONTEXT, TEST_NAME) as b:
        output = {'file':[],'name':[]}
        for i in range(2):
            with b.make_file('output_{}'.format(i)).open('w') as of:
                of.write("some text for the {} file".format(i))
                output['name'].append('output_{}'.format(i))
                output['file'].append(of.name)
        b.add_data(output)

    b.push()

    b.rm()

    b = api.Bundle(TEST_CONTEXT, TEST_NAME).pull(localize=False)



class ExternalDepWrapper(PipeTask):
    """ Only to generate the processing name """
    def pipe_requires(self, pipeline_input=None):
        self.set_bundle_name()


class ConsumeExtDep(PipeTask):
    """ Consume files from an external dependency
    """
    num_luigi_files  = luigi.IntParameter(default=2)
    num_dir_files    = luigi.IntParameter(default=2)

    def pipe_requires(self, pipeline_input=None):
        """ No new tasks
        Returns:
            None
        """
        self.add_external_dependency("input_files",
                                     ExternalDepWrapper,
                                     {})
        return None

    def pipe_run(self, pipeline_input=None, input_files=None):
        """ For each file, print out its name and contents.
        """
        max_len = 0
        nfiles = 0
        for k, v in input_files.iteritems():
            for f in v:
                with open(f,'r') as of:
                    s = of.read()
                    if len(s) > max_len:
                        max_len = len(s)
                    print "Reading file: {} length:{}".format(f, len(s))
                    nfiles += 1

        return {'num categories': [len(input_files)], 'num files': [nfiles], 'max string': [max_len]}


if __name__ == "__main__":
    test()
    #api.apply('examples', '-', '-', 'ReadFiles', params={'num_luigi_files':5})
