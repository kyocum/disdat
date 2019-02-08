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
import numpy as np
import os
from disdat.pipe import PipeTask
import disdat.api as api


TEST_CONTEXT = '_test_context_'
TEST_NAME    = 'test_bundle'
TEST_REMOTE  = 's3://<YOURPATH>'
PWD          = os.path.dirname(__file__)
SETUP_DIR    = os.path.join(PWD,'..')
PIPELINE_CLS = 'pipelines.test_api_run.DataMaker'
OUTPUT_BUNDLE = 'test_local_run'


def test():
    """ Test the api.run() function.

    1.) Create the container via the api

    2.) Create a test context

    3.) Call run locally

    4.) Call run on AWS Batch (need to add MonkeyPatch)

    """

    test_arg = [1000,2000,8000]

    api.context(TEST_CONTEXT)
    api.remote(TEST_CONTEXT, TEST_CONTEXT, TEST_REMOTE, force=True)

    print ("--0: Create docker container")
    api.dockerize(SETUP_DIR, PIPELINE_CLS)

    print ("--1: Running container locally and storing results locally...")
    retval = api.run(TEST_CONTEXT, TEST_CONTEXT, OUTPUT_BUNDLE, PIPELINE_CLS,
                     pipeline_args={'int_array': test_arg},
                     remote=TEST_REMOTE,
                     no_pull=True,
                     no_push=True)
    print ("--1: 100 chars of RETVAL {}".format(retval[:100]))
    b = api.get(TEST_CONTEXT, OUTPUT_BUNDLE)
    assert(b is not None)
    print ("--1: Pipeline tried to store {} and we found {}".format(test_arg, b.cat()))
    assert(np.array_equal(b.cat(), test_arg))
    b.rm()

    print ("--2: Running container locally and pushing results ...")
    retval = api.run(TEST_CONTEXT, TEST_CONTEXT, OUTPUT_BUNDLE, PIPELINE_CLS,
                     pipeline_args={'int_array': test_arg},
                     remote=TEST_REMOTE,
                     no_pull=True,
                     no_push=False)
    print ("--2: 100 chars of RETVAL {}".format(retval[:100]))
    print ("--2B: Removing local output bundle...")
    api.get(TEST_CONTEXT, OUTPUT_BUNDLE).rm()
    print ("--2C: Pulling remote bundle and verifying...")
    api.pull(TEST_CONTEXT)
    b = api.get(TEST_CONTEXT, OUTPUT_BUNDLE)
    print ("--2C: Pipeline tried to store {} and we found {}".format(test_arg, b.cat()))
    assert(np.array_equal(b.cat(), test_arg))
    b.rm()

    print ("--3: Running container on AWS pulling and pushing results ...")
    print ("--3B: Push docker container")
    api.dockerize(SETUP_DIR, PIPELINE_CLS, push=True)
    print ("--3C: Run docker container on AWS Batch")
    retval = api.run(TEST_CONTEXT, TEST_CONTEXT, OUTPUT_BUNDLE, PIPELINE_CLS,
                     pipeline_args={'int_array': test_arg},
                     remote=TEST_REMOTE,
                     backend='AWSBatch')
    print ("--3C: RETVAL {}".format(retval))
    print ("--3D: Pulling remote bundle and verifying...")
    api.pull(TEST_CONTEXT)
    b = api.get(TEST_CONTEXT, OUTPUT_BUNDLE)
    print ("--3D: Pipeline tried to store {} and we found {}".format(test_arg, b.cat()))
    assert(np.array_equal(b.cat(), test_arg))
    b.rm()

    print ("--4: Running with no submit ...")
    print ("--4B: Reusing docker container")
    print ("--4C: Submit Job on AWS Batch")
    retval = api.run(TEST_CONTEXT, TEST_CONTEXT, OUTPUT_BUNDLE, PIPELINE_CLS,
                     pipeline_args={'int_array': test_arg},
                     remote=TEST_REMOTE,
                     backend='AWSBatch',
                     no_submit=True)
    print ("--4C: RETVAL {}".format(retval))

    api.delete_context(TEST_CONTEXT)


class DataMaker(PipeTask):
    """ Run this by itself.
    Then B requires DataMaker as external, and A. """

    int_array = luigi.ListParameter(default=[0,1])

    def pipe_requires(self, pipeline_input=None):
        self.set_bundle_name("DataMaker")
        return

    def pipe_run(self, pipeline_input=None):
        return np.array(self.int_array)


class Root(PipeTask):

    int_array = luigi.ListParameter(default=None)

    def pipe_requires(self, pipeline_input=None):
        self.add_dependency('datamaker', DataMaker, {'int_array': self.int_array})

    def pipe_run(self, pipeline_input=None, datamaker=None):
        print ("Root received a datamaker {}".format(datamaker))
        return datamaker.mean()


if __name__ == '__main__':
    test()
