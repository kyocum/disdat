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
TEST_REMOTE  = 's3://disdat-prod-486026647292-us-west-2/beta/'


def test():
    """ Test the api.run() function.

    1.) Create the container via the api

    2.) Create a test context

    3.) Call run locally

    4.) Call run on AWS Batch (need to add MonkeyPatch)

    """

    api.context(TEST_CONTEXT)


    """def run(local_context,
        remote_context,
        output_bundle,
        transform,
        params,
        remote=None,
        backend=Backend.default(),
        input_tags=None,
        output_tags=None,
        force=False,
        no_pull=False,
        no_push=False,
        no_push_int=False,
        vcpus=2,
        memory=4000,
        workers=1,
        aws_session_token_duration=42300,
        job_role_arn=None):
        """
    # First run locally
    api.run(TEST_CONTEXT, TEST_CONTEXT, 'test_local_run', 'DataMaker', pipeline_args={'int_array': '[1000,2000,3000]'},
            remote=TEST_REMOTE, no_pull=True, no_push=True)

    b = api.get(TEST_CONTEXT, 'test_local_run')

    assert(b is not None)

    b.rm()

    #api.apply(TEST_CONTEXT, '-', '-', 'Root')

    #b = api.get(TEST_CONTEXT, 'PreMaker_auf_root')

    #assert(b is not None)

    #api.delete_context(TEST_CONTEXT)


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
