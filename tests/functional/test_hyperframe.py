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
