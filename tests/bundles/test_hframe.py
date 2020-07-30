"""
Test for hyperframe implementations.
"""


from sqlalchemy import create_engine
import disdat.hyperframe as hyperframe
from disdat.common import BUNDLE_URI_SCHEME
from disdat.hyperframe import r_pb_db, r_pb_fs, w_pb_db, w_pb_fs
import os
import shutil
import hashlib
import tempfile
import uuid
import numpy as np


def _make_linkauth_records():
    """
    :return: s3 auth record, vertica auth record
    """
    slar = hyperframe.S3LinkAuthRecord('id1234', 'keyabcd', 'tokenX', 'wildprofile')
    return slar


def _make_link_records():
    """

    Returns:
        file_link, s3_link, db_link

    """

    fake_hfid = str(uuid.uuid1())
    fake_laid = str(uuid.uuid1())

    file_link = hyperframe.FileLinkRecord(fake_hfid, fake_laid, BUNDLE_URI_SCHEME+"Users/someuser/somefile.txt")
    s3_link = hyperframe.S3LinkRecord(fake_hfid, fake_laid, BUNDLE_URI_SCHEME+"ds-bucket/keyone/keytwo/target.sql")
    return file_link, s3_link


def _make_lineage_record(hframe_name, hframe_uuid, depends_on=None):
    """

    Args:
        hframe_name:
        hframe_uuid:
        depends_on:

    Returns:
        (`LineageRecord`)

    """

    lr = hyperframe.LineageRecord(hframe_proc_name=hframe_name, hframe_uuid=hframe_uuid,
                                  code_repo='bigdipper', code_name='unknown',
                                  code_semver='0.1.0', code_hash='5cd60d3',
                                  code_branch='develop', code_method='unknown', depends_on=depends_on)

    return lr

bytes_data = b'\x00\x01\x02\x03\x04\x05\x06\x07'

test_data = {'int_data'   : np.array([0, -11, 12345, -314968], dtype=np.int32),
             'nd_int_data' : np.array([[0, -11, 12345, -314968], [20, -211, 212345, -2314968]], dtype=np.int32),
             'uint_data'   : np.array([0, 11, 12345, 314968], dtype=np.uint32),
             'float32_data' : np.array([0.0, -1.1, 1.2345, -3.14968], dtype=np.float32),
             'float64_data' : np.array([0.0, -1.1, 1.2345, -3.14968], dtype=np.float64),
             'nd_float64_data' : np.array([[0.0, -1.1, 1.2345, -3.14968],[20.0, -21.1, 21.2345, -23.14968]], dtype=np.float64),
             'bool_data'  : np.array([True, True, False, True, False], dtype=np.bool),
             'string_data'  : np.array(['This', ' is', ' a', ' test!'], dtype=np.string_),
             'unicode_data' : np.array(['This', ' is', ' a', ' test!'], dtype=np.unicode_)
             }


def _make_hframe_record(name, tags=None, hframes=None):
    """

    Create a hyperframe.   Attach three frames.
    1.) 1 Frame w ith 8 bytes of int data
    2.) 1 Frame with 1 file link -- no linkauth
    3.) 1 Frame point to a Frame with byte-wise data -- no linkauth

    Args:
        tags (:dict:(str,str)):  Optional set of tags.
        hframes (`HyperFrameRecords`): Optional hframes to attach

    Returns:
        (`HyperFrameRecord`)

    """

    hfid = str(uuid.uuid1())

    if tags is None:
        tags = {'datagroup':'lab', 'description':'regress the covmat'}

    frames = []

    # Raw bytes
    frames.append(hyperframe.FrameRecord(name='bytes_data', hframe_uuid=hfid,
                                         type='INT32',
                                         shape=(len(bytes_data),),
                                         data=bytes_data))

    # This code tests our ability to turn ndarrays into pb messages and back
    if True:
        for test_name, nda in test_data.items():
            frames.append(hyperframe.FrameRecord.from_ndarray(hfid, test_name, nda))
            if 'int' in test_name or 'float' in test_name:
                test_series = nda.byteswap().newbyteorder()
                frames.append(hyperframe.FrameRecord.from_ndarray(hfid, test_name+"_swapped", test_series))

    file_link = hyperframe.FileLinkRecord(hfid, None, BUNDLE_URI_SCHEME+'Users/someuser/somefile.txt')

    frames.append(hyperframe.FrameRecord(name='links', hframe_uuid=hfid,
                                         type='LINK',
                                         shape=(1,),
                                         links=[file_link,]))

    if hframes is not None:
        frames.append(hyperframe.FrameRecord(name='hframes', hframe_uuid=hfid,
                               type='HFRAME',
                               shape=(len(hframes),),
                               hframes=hframes))

    lr = _make_lineage_record(name, hfid)

    hf = hyperframe.HyperFrameRecord(owner='vklartho', human_name=name, uuid=hfid, frames=frames, lin_obj=lr, tags=tags)

    return hf


def validate_hframe_record(hfr):
    """
    Given an HFR, validate the frames contain similar data.
    Enumerate the frames and, for each, compare against source ndarray

    Args:
        hfr (`hyperframe.HyperFrameRecord`):

    Returns:

    """

    for fr in hfr.get_frames(None, testing_dir=testdir):
        if 'bytes_data' in fr.pb.name:
            if bytes_data != fr.pb.data:
                print("Frame {} busted".format(fr.pb.name))
                print("original: {}".format(bytes_data))
                print("found:    {}".format(fr.pb.data))
            else:
                print("Verified Frame\t{}\t\tdtype {}.".format(fr.pb.name, None))

        elif fr.pb.name.endswith('_swapped'):
            # a byte-swapped, byte-order swapped array, test against original values
            original_nda = test_data[ fr.pb.name.replace('_swapped','') ]
            found_nda = fr.to_ndarray()
            if not np.array_equal(original_nda, found_nda):
                print("Frame {} failed validation step:".format(fr.pb.name))
                print("original: {}".format(original_nda))
                print("found:    {}".format(found_nda))
            else:
                print("Verified Frame\t{}\t\tdtype {}\t{}.".format(fr.pb.name, found_nda.dtype, found_nda.dtype.type))

        elif fr.pb.name in test_data:
            original_nda = test_data[fr.pb.name]
            found_nda = fr.to_ndarray()
            if not np.array_equal(original_nda, found_nda):
                print("Frame {} failed validation step:".format(fr.pb.name))
                print("original: {}".format(original_nda))
                print("found:    {}".format(found_nda))
            else:
                print("Verified Frame\t{}\t\tdtype {}\t{}".format(fr.pb.name, found_nda.dtype, found_nda.dtype.type))


##########################################
# Protocol Buffer Read/Write to local FS test calls
##########################################

testdir = os.path.join(tempfile.gettempdir(), "hframetests")

if os.path.exists(testdir):  # and os.path.isfile(os.path.join(meta_dir,META_CTXT_FILE)):
    shutil.rmtree(testdir)

os.makedirs(testdir)


def test_hframe_rw_pb():
    """
    Write HyperFrame PBs to disk and back.
    :return:
    """

    hf1 = _make_hframe_record('inner_record')
    hf2 = _make_hframe_record('outer_record', hframes=[hf1, ])

    """ Write out protocol buffers """

    w_pb_fs(testdir, hf2)

    for fr in hf2.get_frames(None, testing_dir=testdir):
        w_pb_fs(testdir, fr)

    """ Read in protocol buffers """

    hf2_read = r_pb_fs(os.path.join(testdir, hf2.get_filename()), hyperframe.HyperFrameRecord)

    validate_hframe_record(hf2_read)


def test_linkauth_rw_pb():
    """
    Write LINKAUTH PBs to disk and back.
    :return:
    """

    slar = _make_linkauth_records()

    """ Write out protocol buffers """

    w_pb_fs(testdir, slar)

    """ Read in protocol buffers """

    r_pb_fs(os.path.join(testdir, slar.get_filename()), hyperframe.S3LinkAuthRecord)


def test_link_rw_pb():
    """
    Write LINK PBs to disk and back.
    :return:
    """

    file_link, s3_link = _make_link_records()

    """ Write out protocol buffers """

    w_pb_fs(testdir, file_link)
    w_pb_fs(testdir, s3_link)

    """ Read in protocol buffers """

    r_pb_fs(os.path.join(testdir, file_link.get_filename()), hyperframe.FileLinkRecord)
    r_pb_fs(os.path.join(testdir, s3_link.get_filename()), hyperframe.S3LinkRecord)



##########################################
# Database Test Calls
##########################################


""" Create in-memory DB """
engine_g = create_engine('sqlite:///:memory:', echo=True)


def test_hframe_rw_db():
    """
    Create a pb buffer
    write to db
    read from db
    :return:
    """
    global engine_g

    """ Create table """

    hyperframe.HyperFrameRecord.create_table(engine_g)

    """ Create some PB records """

    hf1 = _make_hframe_record('inner_record')
    hf2 = _make_hframe_record('outer_record', hframes=[hf1, ])

    """ Write out PBs as rows """

    hf_hash = hashlib.md5(hf2.pb.SerializeToString()).hexdigest()
    w_pb_db(hf2, engine_g)

    """ Read in PBs as rows"""

    hf_results = r_pb_db(hyperframe.HyperFrameRecord, engine_g)

    hf_hash2 = None
    for x in hf_results:
        hf_hash2 = hashlib.md5(x.pb.SerializeToString()).hexdigest()

    assert (hf_hash == hf_hash2)


def test_linkauth_rw_db():
    """
    Create a pb buffer
    write to db
    read from db
    :return:
    """
    global engine_g

    """ Create table """

    hyperframe.LinkAuthBase.create_table(engine_g)

    """ Create some PB records """

    slar = _make_linkauth_records()

    """ Write out PBs as rows """

    slar_hash = hashlib.md5(slar.pb.SerializeToString()).hexdigest()
    w_pb_db(slar, engine_g)

    """ Read in PBs as rows"""

    link_auth_results = r_pb_db(hyperframe.LinkAuthBase, engine_g)

    slar_hash2 = None
    for x in link_auth_results:
        if x.pb.WhichOneof('auth') == 's3_auth':
            slar_hash2 = hashlib.md5(x.pb.SerializeToString()).hexdigest()

    assert (slar_hash == slar_hash2)


def test_link_rw_db():
    """
    Create a pb buffer
    write to db
    read from db
    :return:
    """
    global engine_g

    """ Create table """

    hyperframe.LinkBase.create_table(engine_g)

    """ Create some PB records """

    local_link, s3_link = _make_link_records()

    """ Write out PBs as rows """

    local_hash = hashlib.md5(local_link.pb.SerializeToString()).hexdigest()
    w_pb_db(local_link, engine_g)

    s3_hash = hashlib.md5(s3_link.pb.SerializeToString()).hexdigest()
    w_pb_db(s3_link, engine_g)

    """ Read in PBs as rows"""

    link_results = r_pb_db(hyperframe.LinkBase, engine_g)

    local_hash2 = None
    s3_hash2 = None

    for x in link_results:
        if x.pb.WhichOneof('link') == 'local':
            local_hash2 = hashlib.md5(x.pb.SerializeToString()).hexdigest()
        if x.pb.WhichOneof('link') == 's3':
            s3_hash2 = hashlib.md5(x.pb.SerializeToString()).hexdigest()

    assert (local_hash == local_hash2)
    assert (s3_hash == s3_hash2)




