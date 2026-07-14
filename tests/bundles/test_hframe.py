"""
Test for hyperframe implementations.
"""

import calendar
import hashlib
import os
import shutil
import tempfile
import uuid
from datetime import datetime

import numpy as np
import pytest

import disdat.hyperframe as hyperframe
from disdat.common import BUNDLE_URI_SCHEME
from disdat.hyperframe import make_engine, r_pb_db, r_pb_fs, w_pb_db, w_pb_fs


def _make_linkauth_records():
    """
    :return: s3 auth record, vertica auth record
    """
    slar = hyperframe.S3LinkAuthRecord("id1234", "keyabcd", "tokenX", "wildprofile")
    return slar


def _make_link_records():
    """

    Returns:
        file_link, s3_link, db_link

    """

    fake_hfid = str(uuid.uuid1())
    fake_laid = str(uuid.uuid1())

    file_link = hyperframe.FileLinkRecord(
        fake_hfid, fake_laid, BUNDLE_URI_SCHEME + "Users/someuser/somefile.txt"
    )
    s3_link = hyperframe.S3LinkRecord(
        fake_hfid, fake_laid, BUNDLE_URI_SCHEME + "ds-bucket/keyone/keytwo/target.sql"
    )
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

    lr = hyperframe.LineageRecord(
        hframe_proc_name=hframe_name,
        hframe_uuid=hframe_uuid,
        code_repo="bigdipper",
        code_name="unknown",
        code_semver="0.1.0",
        code_hash="5cd60d3",
        code_branch="develop",
        code_method="unknown",
        depends_on=depends_on,
    )

    return lr


bytes_data = b"\x00\x01\x02\x03\x04\x05\x06\x07"

test_data = {
    "int_data": np.array([0, -11, 12345, -314968], dtype=np.int32),
    "nd_int_data": np.array(
        [[0, -11, 12345, -314968], [20, -211, 212345, -2314968]], dtype=np.int32
    ),
    "uint_data": np.array([0, 11, 12345, 314968], dtype=np.uint32),
    "float32_data": np.array([0.0, -1.1, 1.2345, -3.14968], dtype=np.float32),
    "float64_data": np.array([0.0, -1.1, 1.2345, -3.14968], dtype=np.float64),
    "nd_float64_data": np.array(
        [[0.0, -1.1, 1.2345, -3.14968], [20.0, -21.1, 21.2345, -23.14968]],
        dtype=np.float64,
    ),
    "bool_data": np.array([True, True, False, True, False], dtype=np.bool_),
    "string_data": np.array(["This", " is", " a", " test!"], dtype=np.bytes_),
    "unicode_data": np.array(["This", " is", " a", " test!"], dtype=np.str_),
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
        tags = {"datagroup": "lab", "description": "regress the covmat"}

    frames = []

    # Raw bytes
    frames.append(
        hyperframe.FrameRecord(
            name="bytes_data",
            hframe_uuid=hfid,
            type="INT32",
            shape=(len(bytes_data),),
            data=bytes_data,
        )
    )

    # This code tests our ability to turn ndarrays into pb messages and back
    if True:
        for test_name, nda in test_data.items():
            frames.append(hyperframe.FrameRecord.from_ndarray(hfid, test_name, nda))
            if "int" in test_name or "float" in test_name:
                # numpy 2.0 removed ndarray.newbyteorder(); reinterpret via a
                # byte-swapped dtype view instead.
                test_series = nda.byteswap().view(nda.dtype.newbyteorder())
                frames.append(
                    hyperframe.FrameRecord.from_ndarray(
                        hfid, test_name + "_swapped", test_series
                    )
                )

    file_link = hyperframe.FileLinkRecord(
        hfid, None, BUNDLE_URI_SCHEME + "Users/someuser/somefile.txt"
    )

    frames.append(
        hyperframe.FrameRecord(
            name="links",
            hframe_uuid=hfid,
            type="LINK",
            shape=(1,),
            links=[
                file_link,
            ],
        )
    )

    if hframes is not None:
        frames.append(
            hyperframe.FrameRecord(
                name="hframes",
                hframe_uuid=hfid,
                type="HFRAME",
                shape=(len(hframes),),
                hframes=hframes,
            )
        )

    lr = _make_lineage_record(name, hfid)

    hf = hyperframe.HyperFrameRecord(
        owner="vklartho",
        human_name=name,
        uuid=hfid,
        frames=frames,
        lin_obj=lr,
        tags=tags,
    )

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
        if "bytes_data" in fr.pb.name:
            if bytes_data != fr.pb.data:
                print("Frame {} busted".format(fr.pb.name))
                print("original: {}".format(bytes_data))
                print("found:    {}".format(fr.pb.data))
            else:
                print("Verified Frame\t{}\t\tdtype {}.".format(fr.pb.name, None))

        elif fr.pb.name.endswith("_swapped"):
            # a byte-swapped, byte-order swapped array, test against original values
            original_nda = test_data[fr.pb.name.replace("_swapped", "")]
            found_nda = fr.to_ndarray()
            if not np.array_equal(original_nda, found_nda):
                print("Frame {} failed validation step:".format(fr.pb.name))
                print("original: {}".format(original_nda))
                print("found:    {}".format(found_nda))
            else:
                print(
                    "Verified Frame\t{}\t\tdtype {}\t{}.".format(
                        fr.pb.name, found_nda.dtype, found_nda.dtype.type
                    )
                )

        elif fr.pb.name in test_data:
            original_nda = test_data[fr.pb.name]
            found_nda = fr.to_ndarray()
            if not np.array_equal(original_nda, found_nda):
                print("Frame {} failed validation step:".format(fr.pb.name))
                print("original: {}".format(original_nda))
                print("found:    {}".format(found_nda))
            else:
                print(
                    "Verified Frame\t{}\t\tdtype {}\t{}".format(
                        fr.pb.name, found_nda.dtype, found_nda.dtype.type
                    )
                )


##########################################
# Protocol Buffer Read/Write to local FS test calls
##########################################

testdir = os.path.join(tempfile.gettempdir(), "hframetests")

if os.path.exists(
    testdir
):  # and os.path.isfile(os.path.join(meta_dir,META_CTXT_FILE)):
    shutil.rmtree(testdir)

os.makedirs(testdir)


def test_hframe_rw_pb():
    """
    Write HyperFrame PBs to disk and back.
    :return:
    """

    hf1 = _make_hframe_record("inner_record")
    hf2 = _make_hframe_record(
        "outer_record",
        hframes=[
            hf1,
        ],
    )

    """ Write out protocol buffers """

    w_pb_fs(testdir, hf2)

    for fr in hf2.get_frames(None, testing_dir=testdir):
        w_pb_fs(testdir, fr)

    """ Read in protocol buffers """

    hf2_read = r_pb_fs(
        os.path.join(testdir, hf2.get_filename()), hyperframe.HyperFrameRecord
    )

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
engine_g = make_engine("sqlite:///:memory:", echo=True)


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

    hf1 = _make_hframe_record("inner_record")
    hf2 = _make_hframe_record(
        "outer_record",
        hframes=[
            hf1,
        ],
    )

    """ Write out PBs as rows """

    hf_hash = hashlib.md5(hf2.pb.SerializeToString()).hexdigest()
    w_pb_db(hf2, engine_g)

    """ Read in PBs as rows"""

    hf_results = r_pb_db(hyperframe.HyperFrameRecord, engine_g)

    hf_hash2 = None
    for x in hf_results:
        hf_hash2 = hashlib.md5(x.pb.SerializeToString()).hexdigest()

    assert hf_hash == hf_hash2


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
        if x.pb.WhichOneof("auth") == "s3_auth":
            slar_hash2 = hashlib.md5(x.pb.SerializeToString()).hexdigest()

    assert slar_hash == slar_hash2


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
        if x.pb.WhichOneof("link") == "local":
            local_hash2 = hashlib.md5(x.pb.SerializeToString()).hexdigest()
        if x.pb.WhichOneof("link") == "s3":
            s3_hash2 = hashlib.md5(x.pb.SerializeToString()).hexdigest()

    assert local_hash == local_hash2
    assert s3_hash == s3_hash2


##########################################
# sqlite3 migration coverage (rm-sqla)
#
# These exercise the query shapes and transaction semantics that the
# SQLAlchemy -> stdlib sqlite3 migration reimplemented, which the pre-existing
# round-trip tests above did not cover directly:
#   - select_hfr_db date filtering / ordering / maxbydate (depends on the
#     _adapt_datetime string format used for the creation_date column)
#   - tag-filtered select (_tag_query sub-select)
#   - update_hfr_db state transition + RecordState read-back
#   - _SqliteEngine.begin() rollback on exception
##########################################


def _write_hframe(engine, name, creation_ts, tags=None):
    """Create+write an hframe with a controlled creation timestamp."""
    hf = _make_hframe_record(name, tags=tags)
    # _write_row converts pb.lineage.creation_date (epoch float) via
    # datetime.utcfromtimestamp -> _adapt_datetime for the creation_date column.
    hf.pb.lineage.creation_date = creation_ts
    w_pb_db(hf, engine)
    return hf


def test_select_date_filter_and_order():
    """before/after filtering and orderby depend on the creation_date TEXT
    format produced by _adapt_datetime; assert they select/sort correctly.
    """
    engine = make_engine("sqlite:///:memory:")
    hyperframe.HyperFrameRecord.create_table(engine)

    # Three bundles at distinct, known times (seconds apart).
    t_old = calendar.timegm(datetime(2020, 1, 1, 0, 0, 0).timetuple())
    t_mid = calendar.timegm(datetime(2021, 6, 15, 12, 0, 0).timetuple())
    t_new = calendar.timegm(datetime(2022, 12, 31, 23, 59, 59).timetuple())
    _write_hframe(engine, "old", t_old)
    _write_hframe(engine, "mid", t_mid)
    _write_hframe(engine, "new", t_new)

    # orderby -> newest first
    ordered = hyperframe.select_hfr_db(engine, orderby=True)
    names = [h.pb.human_name for h in ordered]
    assert names == ["new", "mid", "old"], names

    # after= keeps only records on/after mid
    after_mid = hyperframe.select_hfr_db(engine, after=datetime(2021, 1, 1, 0, 0, 0))
    assert sorted(h.pb.human_name for h in after_mid) == ["mid", "new"]

    # before= keeps only records on/before mid
    before_mid = hyperframe.select_hfr_db(engine, before=datetime(2022, 1, 1, 0, 0, 0))
    assert sorted(h.pb.human_name for h in before_mid) == ["mid", "old"]

    engine.dispose()


def test_select_maxbydate_latest_per_name():
    """maxbydate returns the most recent bundle per human_name.

    The two writes are only microseconds apart (within the same wall-clock
    second) -- the real-world "add the same bundle name twice in quick
    succession" case. The sub-select joins on max(creation_date), so it relies
    on the stored creation_date preserving sub-second precision AND ordering
    lexicographically the same as chronologically. If _adapt_datetime dropped
    the microsecond fraction, both rows would share an identical timestamp and
    the "latest" tie-break would be ambiguous (this test would fail).
    """
    engine = make_engine("sqlite:///:memory:")
    hyperframe.HyperFrameRecord.create_table(engine)

    base = calendar.timegm(datetime(2022, 1, 1, 0, 0, 0).timetuple())
    older = base + 0.001  # same second ...
    newer = base + 0.900  # ... 0.899s later
    _write_hframe(engine, "shared", older)
    latest = _write_hframe(engine, "shared", newer)

    found = hyperframe.select_hfr_db(engine, human_name="shared", maxbydate=True)
    assert len(found) == 1
    assert found[0].pb.uuid == latest.pb.uuid

    engine.dispose()


def test_select_by_tags():
    """Tag-filtered select exercises the _tag_query uuid sub-select."""
    engine = make_engine("sqlite:///:memory:")
    hyperframe.HyperFrameRecord.create_table(engine)

    ts = calendar.timegm(datetime(2022, 1, 1, 0, 0, 0).timetuple())
    _write_hframe(engine, "green", ts, tags={"color": "green"})
    _write_hframe(engine, "red", ts, tags={"color": "red"})

    found = hyperframe.select_hfr_db(engine, tags={"color": "green"})
    assert len(found) == 1
    assert found[0].pb.human_name == "green"

    engine.dispose()


def test_update_state_roundtrip():
    """update_hfr_db writes a new state; from_row reads it back as a
    RecordState enum (stored/read by name).
    """
    engine = make_engine("sqlite:///:memory:")
    hyperframe.HyperFrameRecord.create_table(engine)

    ts = calendar.timegm(datetime(2022, 1, 1, 0, 0, 0).timetuple())
    hf = _write_hframe(engine, "to_delete", ts)

    # Freshly written rows are valid.
    got = hyperframe.select_hfr_db(engine, uuid=hf.pb.uuid)
    assert len(got) == 1
    assert got[0].state == hyperframe.RecordState.valid

    hyperframe.update_hfr_db(
        engine, hyperframe.RecordState.deleted, uuid=hf.pb.uuid
    )

    got = hyperframe.select_hfr_db(engine, uuid=hf.pb.uuid)
    assert len(got) == 1
    assert got[0].state == hyperframe.RecordState.deleted

    engine.dispose()


def test_begin_rolls_back_on_exception():
    """_SqliteEngine.begin() must roll back the transaction if the block raises,
    leaving no partial write behind.
    """
    engine = make_engine("sqlite:///:memory:")
    hyperframe.HyperFrameRecord.create_table(engine)

    assert hyperframe.bundle_count(engine) == 0

    with pytest.raises(RuntimeError):
        with engine.begin() as conn:
            conn.execute(
                "INSERT INTO hframes (uuid, owner, human_name, "
                "processing_name, creation_date, state, pb) "
                "VALUES ('u1', 'o', 'h', 'p', '2022-01-01 00:00:00', 'valid', NULL)"
            )
            raise RuntimeError("boom")

    # The insert must have been rolled back.
    assert hyperframe.bundle_count(engine) == 0

    engine.dispose()


if __name__ == "__main__":
    pytest.main([__file__])
