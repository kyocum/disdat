from __future__ import print_function
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

import pytest
import json

import disdat.api as api

from tests.functional.common import run_test, TEST_CONTEXT


test_json_args_data = {'str_arg': 'some string',
                       'int_arg': 10,
                       'list_arg': [1,3,5],
                       'list_str_arg': ['farg','barg'],
                       'dict_float_arg': {'farg': 0.01, 'barg': 3.14}}

serialized_json_args = {k: json.dumps(v) for k, v in test_json_args_data.items()}


def test_args_bundle():
    """ Create bundle, store args.
    """

    with api.Bundle(TEST_CONTEXT) as b:
        b.add_params(serialized_json_args)
        b.name = 'output'

    b = api.get(TEST_CONTEXT, 'output')

    assert(b.params == serialized_json_args)


if __name__ == "__main__":
    pytest.main([__file__])
