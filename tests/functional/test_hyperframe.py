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

import numpy as np
import six

from disdat import hyperframe
from disdat import hyperframe_pb2


def test_record_from_string_types():

    for dtype in (six.binary_type, six.text_type):

        array = np.array(['hello', 'world', 'sailor', ''])
        array = array.astype(dtype)

        result = hyperframe.FrameRecord.from_ndarray(hfid='', name='', nda=array)

        assert result.pb.type == hyperframe_pb2.STRING
        shape = tuple(map(int, result.pb.shape))
        assert shape == array.shape
