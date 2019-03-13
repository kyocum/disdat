#
# Copyright 2015, 2016  Human Longevity, Inc.
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
HyperFrame Data Objects

All objects are immutable.

Read/write from DB and PB files

HyperFrame -- contains Lineage PB and has pointers to Frames.   Has a Table storing PB and PB on disk
  Lineage    -- contains lineage information.                     No table**, no PB on disk
Frame      -- contains data literals and links.                 Has a Table storing PB** and PB on disk.
  Link       -- contains link literals and pointer to LinkAuth.   No table, no PB on disk
LinkAuth   -- contains auth creds.                              Has a Table storing PB, and PB on disk

**question design decision.  Might have lineage table.  Might not store frame pb in db.

Each Python object is called <Thing>Record
Each PB object is called <Thing>

HyperFrame contains UUIDs of Frames.  Supports downloading HyperFrames without downloading all contained data.
It may be stored in an HFrame table as a byte blob and re-inflated without worry that it will be
 excessively large.

"""
from __future__ import print_function

import sys
from collections import namedtuple, defaultdict
import hashlib
import time
import os
import tempfile
from datetime import datetime
import uuid

import numpy as np
import pandas as pd
import luigi
import six
import enum
from sqlalchemy import Table, Column, String, MetaData, BLOB, Text, Enum, UniqueConstraint, DateTime
from sqlalchemy.sql import text

import disdat.common as common
from disdat.db_link import DBLink
from disdat import hyperframe_pb2
from disdat import logger as _logger


HyperFrameTuple = namedtuple('HyperFrameTuple', 'columns, links, uuid, tags')

# UPSERT policy for inserts that violate constraints (explicit or primary key uniqueness)
# We can ROLLBACK, ABORT, FAIL, IGNORE, and REPLACE
# Most cases we want to REPLACE (UPSERT)
UPSERT_POLICY = 'FAIL'


class RecordState(enum.Enum):
    """
    Database records of hyperframes can be valid or deleted.
    A check of the db may result in marking entries as "uncertain"
    The system may not be able to find the frames listed in the PB.
    Deleted records indicate that someone tried to remove the hyperframe.
    If marked deleted, we should remove from disk and remove the record.
    """
    invalid = 0
    pending = 1
    valid   = 2
    deleted = 3


def r_pb_fs(file_path, read_pb_class):
    """
    Utility function to read pb from disk and return
      the xxxRecord that wraps the pb.  Checks hash if present

    Args:
        file_path (str):
        read_pb_class:

    Returns:
        instance of read_pb_class

    """
    with open(file_path, 'rb') as f:
        contents = f.read()
        pb_record = read_pb_class.from_str_bytes(contents)

    old_hash = pb_record.pb.hash
    pb_record.pb.ClearField('hash')
    new_hash = hashlib.md5(pb_record.pb.SerializeToString()).hexdigest()

    assert(old_hash == new_hash)
    pb_record.pb.hash = old_hash

    return pb_record


def w_pb_fs(file_prefix, pb_record, fq_file_path=None, atomic=False):
    """
    Write the pb contained in the xxxRecord.

    Note: If atomic=True, in almost all cases there won't actually be another file with
    this name.  But if there is, this will atomically replace the file with the new version.

    Note: Atomicity means we won't have a corrupt file.  But if two people update the
    file at the same time, we can have an issue.  Handle that at the db level.

    Note: Python 3.3+ has os.replace()

    Args:
        file_prefix (str): path prefix to where file should be
        pb_record (:object):
        fq_file_path (str): If set, save to this particular file.
        atomic (bool): Attempt an atomic file write.

    Returns:

    """

    if fq_file_path is None:
        fq_file_path = os.path.join(file_prefix, pb_record.get_filename())

    if not atomic:
        with open(fq_file_path, 'wb') as f:
            f.write(pb_record.pb.SerializeToString())
    else:
        with tempfile.NamedTemporaryFile(
                dir=os.path.dirname(fq_file_path), delete=False) as f:
            f.write(pb_record.pb.SerializeToString())
        #os.replace(fout.name, datafile)  For Python 3.3+
        os.rename(f.name, fq_file_path)


def w_pb_db(pb_record, engine_g):
    """
    Given a pb record, write it out to the database connected with engine
    We should ensure our tables have a unique key requirement on uuid column

    Args:
        pb_record:
        engine_g:

    Returns:

    """
    pb_hash = hashlib.md5(pb_record.pb.SerializeToString()).hexdigest()

    with engine_g.connect() as conn:
        pb_record.write_row(RecordState.valid, conn)

    return pb_hash


def r_pb_db(pb_cls, engine_g):
    """
    Given the type of hframe pb, read it from engine_g
    Reads all entries from the db

    Args:
        pb_cls:
        engine_g:

    Returns:
        results (list): a list of xxxRecord objects

    """
    from sqlalchemy.sql import text

    s = text(
        "SELECT * from {}".format(pb_cls.table_name)
    )

    with engine_g.connect() as conn:
        result = conn.execute(s)

    lars = pb_cls.from_row(result)

    return lars


def _groupby_clause(uuid=None, owner=None, human_name=None, processing_name=None):
    """
    Build the groupby clause.  Simply detect which fields are set, and group by those.

    Args:
        uuid:
        owner:
        human_name:
        processing_name:

    Returns:
        (str): "field, ..., field"

    """

    gbc = ''
    clauses = []

    if uuid is not None:
        clauses.append('uuid')

    if owner is not None:
        clauses.append('owner')

    if human_name is not None:
        clauses.append('human_name')

    if processing_name is not None:
        clauses.append('processing_name')

    if len(clauses) > 0:
        gbc =  ','.join(clauses)

    return gbc


def _translate(s):
    if '.*' in s or '.' in s:
        s = 'like "{}"'.format(s.replace('.*', '%').replace('.', '_'))
    else:
        s = '= "{}"'.format(s)
    return s


def _where_clause(uuid=None, owner=None, human_name=None,
                  processing_name=None, state=None,
                  before=None, after=None):
    """
    Build the where clause.  Note, if any string contains '.*' (zero or many of any ) or . (one of any).
    Translate that to '%' and '_' respectively.

    Note: If there are tags, this touches two tables.

    select * from frames where <conditions>

    Args:
        uuid (str):
        owner (str):
        human_name (str):
        processing_name (str):
        state (`RecordState`):
        before (datetime): inclusive before datetime
        after (datetime): inclusive after datetime

    Returns:

    """

    where = ''
    clauses = []

    if uuid is not None:
        clauses.append('uuid {}'.format(_translate(uuid)))

    if owner is not None:
        clauses.append('owner {}'.format(_translate(owner)))

    if human_name is not None:
        clauses.append('human_name {}'.format(_translate(human_name)))

    if processing_name is not None:
        clauses.append('processing_name {}'.format(_translate(processing_name)))

    if state is not None:
        clauses.append('state = "{}"'.format(state.name))

    if before is not None:
        clauses.append('creation_date <= "{}"'.format(before.strftime("%Y-%m-%d %X")))

    if after is not None:
        clauses.append('creation_date >= "{}"'.format(after.strftime("%Y-%m-%d %X")))

    if len(clauses) > 0:
        where = 'WHERE ' + ' AND '.join(clauses)

    return where


def _tag_query(tags):
    """
    Create the SQL query that returns all uuids with the all tags set.

    Args:
        tags:

    Returns:
        str: sql query to return list of uuids with these tags

    """

    tag_table_name = HyperFrameRecord.table_name + '_tags'

    tag_clauses = []

    if tags is None:
        return None

    if tags is not None:
        for k, v in tags.items():
            # SQLITE: critical to have single quotes around strings in sub-select query
            tag_clauses.append("(key = '{}' AND value = '{}')".format(k, v))

    tag_where = 'WHERE ' + ' OR '.join(tag_clauses)

    s = "SELECT uuid from {} {} GROUP BY uuid HAVING count(*) = {}".format(tag_table_name, tag_where, len(tags))

    return s


def select_hfr_db(engine_g, uuid=None, owner=None, human_name=None,
                  processing_name=None, tags=None, state=None,
                  orderby=False, groupby=False, maxbydate=False,
                  before=None, after=None):
    """
    Create an HFrame Record from a row in our DB.
    Where uuid= && owner= && human_name= && processing_name=

    Args:
        engine_g:
        uuid (str):  add to where clause
        owner (str): add to where clause
        human_name (str): add to where clause
        processing_name (str):  add to where clause
        tags (:dict): Dictionary of tags.  Exact match.
        state (`RecordState`):  The state of the entry
        orderby (bool): enable order by creation_date timestamp
        groupby (bool): enable grouping
        maxbydate (bool): Return the latest bundle matched by human_name
        before (datetime.datetime): records on or before this datetime
        after (datetime.datetime): records on or after this datetime

    Returns:
        results (list): a list of xxxRecord objects

    """

    pb_cls = HyperFrameRecord

    where = _where_clause(uuid, owner, human_name, processing_name, state, before, after)

    if tags is not None and tags:  # bool(l={}) = False
        if where == '':
            where = "WHERE uuid in (" + _tag_query(tags) + ")"
        else:
            where = where + " AND uuid in (" + _tag_query(tags) + ")"

    select = '*'

    if orderby:
        orderby = "ORDER BY creation_date DESC"
    else:
        orderby = ''

    if groupby:
        gbc = _groupby_clause(uuid, owner, human_name, processing_name)
        groupby = "GROUP BY " + gbc
        select  = gbc
    else:
        groupby = ''

    # add sub-query if we need to maxbydate, always group by 'human_name'
    sub_q = ''
    if maxbydate:
        sub_q = " AS a JOIN (SELECT human_name as hn, " + \
                    " max(creation_date) AS max_date FROM {} GROUP BY human_name ) as b ".format(pb_cls.table_name) + \
                " ON a.human_name = b.hn AND a.creation_date = b.max_date "

    s = text("SELECT {} FROM {} {} {} {} {}".format(select, pb_cls.table_name, sub_q, where, groupby, orderby))

    #print "Query {}".format(s)

    with engine_g.connect() as conn:
        result = conn.execute(s)
        hfrs = pb_cls.from_row(result) # returns rows if no pb in rows

    return hfrs


def update_hfr_db(engine_g, state, uuid=None, owner=None, human_name=None, processing_name=None):
    """
    Update HFrame row with a new state.
    Where uuid= && owner= && human_name= && processing_name=

    Args:
        engine_g:
        state (`RecordState`):
        uuid (str):
        owner (str):
        human_name (str):
        processing_name (str):

    Returns:
        result : query result

    """

    pb_cls = HyperFrameRecord

    where = _where_clause(uuid, owner, human_name, processing_name)

    s = text(
        'UPDATE {} SET state = "{}" {}'.format(pb_cls.table_name, state.name, where)
    )

    with engine_g.connect() as conn:
        result = conn.execute(s)

    return result


def delete_hfr_db(engine_g, uuid=None, owner=None, human_name=None, processing_name=None, state=None):
    """
    Delete HFrame row from a table where
    uuid= && owner= && human_name= && processing_name= && (optional) state = hyperframe.RecordState.deleted

    TODO: Should be a transaction for the tags and HFR entry table.

    Args:
        engine_g:
        uuid (str):
        owner (str):
        human_name (str):
        processing_name (str):
        state (enum):

    Returns:
        result : query result

    """

    pb_cls = HyperFrameRecord

    where = _where_clause(uuid, owner, human_name, processing_name, state)

    if where == '':
        raise Exception("HFrame DB Delete requires a valid where clause")

    hfr_del = text(
        "DELETE FROM {} {}".format(pb_cls.table_name, where)
    )

    where = _where_clause(uuid)

    tag_del = text(
        "DELETE FROM {} {}".format(pb_cls.table_name + '_tags', where)
    )

    results = []
    with engine_g.connect() as conn:
        results.append(conn.execute(hfr_del))
        results.append(conn.execute(tag_del))

    return results


def delete_fr_db(engine_g, hfr_uuid):
    """ Remove all frames from the database that belong to the hyperframe with uuid hfr_uuid

    Args:
        engine_g: query engine
        hfr_uuid: the hyperframe uuid to which the frames belong

    Returns:
        results
    """

    pb_cls = FrameRecord

    where = "WHERE hframe_uuid {}".format(_translate(hfr_uuid))

    fr_del = text(
        "DELETE FROM {} {}".format(pb_cls.table_name, where)
    )

    with engine_g.connect() as conn:
        results = conn.execute(fr_del)

    return [results]


def get_files_in_dir(dir):
    """ Look for files in a user returned directory
    1.) Only look one-level down (in this directory)
    2.) Do not include anything that looks like one of disdat's pbufs

    TODO: One place that defines the format of the Disdat pb file names
    See data_context.DataContext: rebuild_db() *_frame.pb, *_hframe.pb, *_auth.pb
    Args:
        (str): local directory
    Returns:
        (list:str): List of files in that directory
    """

    files = [os.path.join(dir, f) for f in os.listdir(dir) if os.path.isfile(os.path.join(dir, f))
             and ('_hframe.pb' not in f) and ('_frame.pb' not in f) and ('_auth.pb' not in f)]

    return files


def detect_local_fs_path(series):
    """
    Given a series, check whether all entries appear to be real paths and:
    1.) get their absolute paths
    2.) pre-pend file:///

    Args:
        series:

    Returns:
        series: Same series but absolute otherwise None

    """
    output = []
    for s in series:
        if not isinstance(s, six.string_types):
            return None
        if os.path.isfile(s):
            output.append("file://{}".format(os.path.abspath(s)))
        elif os.path.isdir(s):
            """ Find files one-level down """
            output.extend(["file://{}".format(os.path.join(s, f)) for f in get_files_in_dir(s)])
        else:
            del output
            return None
    return np.array(output)


def strip_file_prefix(series):
    """
    Given a series of local fs file links, strip the "file://" from each.

    Note: In-place modification

    Args:
        series: Strings with "file://" prefix

    Returns:
        None: Modifies input array to point to stripped strings.

    """
    for i in range(len(series)):
        assert series[i].startswith("file://")
        series[i] = series[i][7:]


class PBObject(object):
    """
    Most objects mirror PB objects.
    Thus they need to be able to be constructed from PBs
    And they need to be able to be read and written from sql tables.
    Each object has a two-deep object inheritance.   First, they have a
    xxxBase class.  This inherits from PBObject.   They must implement
    _create_table, _pb_type, and _write_row.

    Note that _create_table and _write_row must use the same strings for column
    identifiers.   _pb_type simply returns the protocol buffer object from the PB compiled
    python class.
    """

    def __init__(self):
        self.state = RecordState.invalid

    def init_internal_state(self):
        """
        ** OVERRIDDEN BY USER **
        Set up any internal state when object created with just the pb or
        row if not None.

        Args:
            row: data from db

        Returns:

        """
        pass

    @staticmethod
    def _create_table(metadata):
        """
        **IMPLEMENTED BY USER**
        Create unbound sqlalchemy table object
        For a PB object, we keep track of external key and any
        other items we want to have in the database beyond the
        byte blob or file pointer for the PB object.

        Args:
            metadata:

        Returns:
            sqlalchemy.Table or dict[str:<table>, sqlalchemy.Table]
        """

        raise NotImplementedError

    @staticmethod
    def _pb_type():
        """
        **IMPLEMENTED BY USER**

        Returns:
            hyperframe_pb2.<pb_type>()
        """

        raise NotImplementedError

    def _write_row(self):
        """
        **IMPLEMENTED BY USER**
        Return dictionary of column_name:value for each row.

        Returns:
             dict[str:<column>, <data>] or dict[str:<table>, list:dict<rows>]
        """

        raise NotImplementedError

    def get_filename(self):
        """

        Returns:
            (str): <uuid>_<hframe,frame,auth>.pb

        """
        raise NotImplementedError

    @classmethod
    def create_table(cls, db_engine):
        """
        Do not over-ride
        Create the table that the inheriting class has set up
        in _create_table(cls, metadata).  _create_table() may
        create multiple tables.

        Args:
            db_engine: sqlalchemy engine

        Returns:
            None
        """
        metadata = MetaData()
        metadata.bind = db_engine
        _ = cls._create_table(metadata)
        metadata.create_all()

    def write_row(self, state, db_conn):
        """
        Do not over-ride
        Given sqlalchemy connection, execute write.

        Some pb's write to multiple tables and multiple rows.
        So some pb's _create_table and _write_row will return a dictionary
        referring to the table and the rows to insert into that table.

        You set the state on your in-memory copy when you write to the db.
        You set the state on your in-memory copy when you read from the cb.

        We use sqlite at the moment, so
        https://docs.sqlalchemy.org/en/latest/dialects/sqlite.html
        See ON CONFLICT support for constraints

        Args:
            state (enum): invalid, valid, pending, deleted
            db_conn:

        Returns:
            conn.execute result
        """
        from sqlalchemy.exc import IntegrityError

        metadata = MetaData()
        self.state = state
        pb_tbls = self._create_table(metadata)
        pb_rows = self._write_row()

        try:
            if type(pb_tbls) is dict:
                assert (isinstance(pb_rows, dict) or isinstance(pb_rows, defaultdict))
                results = []
                for k, tbl in pb_tbls.items(): # dict of tables
                    for r in pb_rows[k]:           # dict of list of rows
                        ins = tbl.insert()
                        results.append(db_conn.execute(ins, r))
                return results
            else:
                assert (type(pb_tbls) is not list)
                assert (type(pb_tbls) is not tuple)
                ins = pb_tbls.insert()
                return db_conn.execute(ins, pb_rows)
        except IntegrityError as ie:
            _logger.info("Writing class pb {} to table encountered error {}".format(self._pb_type(), ie))
            return None

    @classmethod
    def from_str_bytes(cls, pb_str_bytes):
        """
        Return an object of this type from the serialized pb bytes

        Args:
        pb_str_bytes

        Returns:
            object
        """
        pb = cls._pb_type()
        if isinstance(pb_str_bytes, six.string_types):
            pb_str_bytes = six.b(pb_str_bytes)
        pb.ParseFromString(pb_str_bytes)
        obj = cls.__new__(cls)
        setattr(obj, 'pb', pb)

        obj.init_internal_state()
        return obj

    @classmethod
    def copy_from_pb(cls, other_pb):
        """
        Given another pb of same type, copy to this wrapper object

        Args:
            other_pb:

        Returns:
            None
        """

        pb = cls._pb_type()
        pb.CopyFrom(other_pb)
        obj = cls.__new__(cls)
        setattr(obj, 'pb', pb)
        obj.init_internal_state()
        return obj

    @classmethod
    def from_row(cls, sa_result):
        """
        Given sqlalchemy row, instantiate a cls
        Since these are small, we store the LAB as a blob
        and instantiate from it.

        NOTE: We instantiate the PB we stored in the table.
        But we are only setting the state variable.
        NOTE: This assumes a binary blob.

        NOTE: in the future we might simply return the row, not a xxxRecord
        with a protobuf in it.

        Args:
           sa_result:  a sqlalchemy result object

        Returns:
            [obj, ]
        """
        objs = []
        for row in sa_result:
            if 'pb' in row:
                pb = row['pb']
                if isinstance(pb, six.string_types):
                    pb = pb.encode('utf8')
                obj = cls.from_str_bytes(pb)
                obj.state = row['state']
            else:
                obj = row
            objs.append(obj)
        return objs

    def ser(self):
        assert (self.pb is not None)
        assert (self.pb.IsInitialized())
        return self.pb.SerializeToString()

    def deser(self, byte_str):
        assert (self.pb is not None)
        self.pb = self._pb_type()
        self.pb.ParseFromString(byte_str)


class HyperFrameRecord(PBObject):
    """
    HyperFrameRecord stores a named list of frames (or tensors)
    Includes lineage, tags, and links
    This is the in-python representation.   Each can import / export to PBs and DBs (via named tuples)
    """

    table_name = 'hframes'

    def __init__(self, owner='', human_name='', processing_name='', uuid='',
                 frames=None, lin_obj=None, tags=None, presentation=hyperframe_pb2.DEFAULT):
        """
        Create a HyperFrame

        Note: human_name used to be "bundle_name" -- a special tag.

        Note: Unlike old-style bundles, we no longer have a url / size.  Those are inside the frames.

        Note: This object has a "frame cache."  The PB only refers to frames by UUID.  As the user works with this
        object, they may ask for the referred to frames.  We fill in the cache as they request them.

        Note: This is denormalized.  Lineage, Tags, Frames all use this HyperFrame's UUID as their index.

        Args:
            owner (str): user or group that made the object -- special tag.
            human_name (str): the simple or given name of the bundle, e.g., dsdt add "STR.Results" -- special tag.
            processing_name (str): machine generated (pipe.unique_id()) name -- special tag.
            uuid (str):  Unique ID for this HyperFrame, if None, then we create.
            frames (:list:'FrameRecord' or :list:str):  List of Frames or UUID strings of Frames.
            lin_obj ('LineageRecord'):  A lineage record to attach
            tags (:dict:(str,str)) : Set of tags to semantically identify this dataset.
            presentation (enum): Default to HF, can be HF|DF|SCALAR|TUPLE|DICT

        """

        super(HyperFrameRecord, self).__init__()
        self.pb = self._pb_type()

        self.pb.owner = owner
        self.pb.human_name = human_name
        self.pb.processing_name = processing_name
        self.pb.uuid = uuid
        self.pb.presentation = presentation

        self.frame_cache = defaultdict(FrameRecord)
        self.frame_dict  = {}
        self.tag_dict    = {}

        if frames is not None:
            self.add_frames(frames)

        if tags is not None:
            self.add_tags(tags)

        if lin_obj is not None:
            self.add_lineage(lin_obj)

        self.pb.ClearField('hash')
        self.pb.hash = hashlib.md5(self.pb.SerializeToString()).hexdigest()

    def is_presentable(self):
        """
        Whether or not this HyperFrame is a presentable.
        All HF's made by individual Luigi Tasks are presentable.

        Returns:
            (bool)
        """
        if self.pb.presentation == hyperframe_pb2.DEFAULT:
            return False
        else:
            return True

    def mod_uuid(self, new_hfr_uuid):
        """
        Modify UUID of in-memory HFR

        Args:
            new_hfr_uuid:

        Returns:
            hyperframe.HyperFrameRecord

        """
        self.pb.uuid = new_hfr_uuid

        self.pb.lineage.hframe_uuid   = new_hfr_uuid

        return self._mod_finish()

    def mod_frames(self, new_frames):
        """
        Replace all frames of in-memory HFR

        NOTE: HFRs are immutable.  Never save this HFR unless you also (or will) call mod_uuid().

        Args:
            new_frames:

        Returns:
            hyperframe.HyperFrameRecord

        """
        # reset the internal frame_cache and frame_dict
        # these will be rebuilt on add_frames
        self.frame_cache = defaultdict(FrameRecord)
        self.frame_dict  = {}

        self.pb.ClearField('frames')
        self.add_frames(new_frames)

        return self._mod_finish()

    def replace_tags(self, new_tags):
        """
        Replace all tags of in-memory HFR

        NOTE: HFRs are immutable.  Never save this HFR unless you also (or will) call mod_uuid().

        Args:
            new_tags:

        Returns:
            hyperframe.HyperFrameRecord

        """

        self.tag_dict    = {}

        self.pb.ClearField('tags')
        self.add_tags(new_tags)

        return self._mod_finish(new_time=False)

    def mod_tags(self, new_tags):
        """
        Update tags of in-memory HFR

        NOTE: HFRs are immutable.  Never save this HFR unless you also (or will) call mod_uuid().

        Args:
            new_tags:

        Returns:
            hyperframe.HyperFrameRecord

        """

        raise NotImplementedError

    def mod_presentation(self, new_presentation):
        """
        Update tags of in-memory HFR

        NOTE: HFRs are immutable.  Never save this HFR unless you also (or will) call mod_uuid().

        Args:
            new_presentation:

        Returns:
            hyperframe.HyperFrameRecord

        """

        self.pb.presentation = new_presentation

        return self._mod_finish()

    def _mod_finish(self, new_time=True):
        """
        Finish mod by updating the creation timestamp and the hash.

        Args:
            new_time (bool): Whether to update the timestamp

        Returns:
            hyperframe.HyperFrameRecord

        """

        if new_time:
            self.pb.lineage.creation_date = time.time()

        self.pb.ClearField('hash')
        self.pb.hash = hashlib.md5(self.pb.SerializeToString()).hexdigest()

        return self

    def init_internal_state(self):
        """
        If you create a HFR and just set the pb, this will create
        the frame and tag dicts.

        Args:
            row:

        Returns:
            None
        """

        self.frame_cache = defaultdict(FrameRecord)
        self.frame_dict  = {}
        self.tag_dict    = {}

        for string_tuple in self.pb.frames:
            self.frame_dict[string_tuple.k] = string_tuple.v

        for string_tuple in self.pb.tags:
            self.tag_dict[string_tuple.k] = string_tuple.v

    @staticmethod
    def make_filename(uuid):
        return "{}_hframe.pb".format(uuid)

    def get_filename(self):
        """

        Returns:
            (str): <uuid>_<hframe,frame,auth>.pb

        """
        return HyperFrameRecord.make_filename(self.pb.uuid)

    @staticmethod
    def _create_table(metadata):
        """
        Create hframe table
        Create tags table

        :return: Table
        """
        hframes = Table(HyperFrameRecord.table_name, metadata,
                        Column('uuid', String(50), primary_key=True),# sqlite_on_conflict_primary_key=UPSERT_POLICY),
                        Column('owner', String),
                        Column('human_name', String),
                        Column('processing_name', String),
                        Column('creation_date', DateTime), #TIMESTAMP),
                        Column('state', Enum(RecordState)),
                        Column('pb', BLOB)
                        )

        tags = Table(HyperFrameRecord.table_name+'_tags', metadata,
                     Column('key', String),
                     Column('uuid', String(50)),
                     Column('value', String),
                     # explicit/composite unique constraint.  'name' is optional.
                     UniqueConstraint('key', 'uuid', name='uix_1')#, sqlite_on_conflict=UPSERT_POLICY)
                     )

        return {HyperFrameRecord.table_name: hframes,
                HyperFrameRecord.table_name+'_tags': tags}

    @staticmethod
    def _pb_type():
        """
        Returns:
            The hyperframe_pb2.<pb_type>()
        """
        return hyperframe_pb2.HyperFrame()

    def _write_row(self):
        """
        Returns:
             Dictionary of key columns (from _create_table) and values.
        """
        assert(self.pb is not None)

        rows = defaultdict(list)

        rows[HyperFrameRecord.table_name].append(
            {'uuid': self.pb.uuid,
             'owner': self.pb.owner,
             'human_name': self.pb.human_name,
             'processing_name': self.pb.processing_name,
             'creation_date': datetime.fromtimestamp(self.pb.lineage.creation_date),
             'state': self.state,
             'pb': self.pb.SerializeToString()})

        for string_tuple in self.pb.tags:
            r = {'uuid': self.pb.uuid,
                 'key': string_tuple.k,
                 'value': string_tuple.v}
            rows[HyperFrameRecord.table_name+'_tags'].append(r)

        return rows

    def add_frames(self, frames):
        """

        Add frames to the PB.  If UUID, just add the UUID.  If a FrameRecord,
        then add both the UUID in the pb.frames map and the FrameRecord in the frame_cache.

        Args:
            frames (:list:'FrameRecord' or :list:tuple:str,str): List of Frames to append or list of tuples(str,uuid)

        Returns:
            Nothing
        """

        for f in frames:
            if isinstance(f, tuple):
                k = f[0]
                v = f[1]
            elif isinstance(f, FrameRecord):
                # TODO: REMOVE THIS DEPENDENCY -- means we need to be very careful about FR objects.
                f.hframe_uuid = self.pb.uuid
                k = f.pb.name
                v = f.pb.uuid
                self.frame_cache[f.pb.name] = f
            else:
                print("Unable to add frame with type {}: Data {}".format(type(f), f))
                assert False

            st = self.pb.frames.add()
            st.k = k
            st.v = v
            self.frame_dict[k] = v

    def get_frames(self, data_context, testing_dir=None, names=None):
        """
        Get either all frames or frames by name.  We are not returning
        the UUID, we are returning the FrameRecord with PB inside.

        Args:
            data_context (:'DataContext'): The DataContext from which to find the frames
            names (:list:str):  Names to retrieve or all if none.

        Returns:
            (:obj:list FrameRecord)
        """

        def _resolve_frame(self, data_context, name, uuid, testing_dir=None):
            """
            Given hframe, context and name, return FrameRecord.
            Up to the context to resolve Frame PB given hframe.uuid and frame.uuid

            Args:
                self: the hyperframe record
                name: the name of the frame
                data_context: the context in which you think this exists

            Returns:
                FrameRecord

            """
            if name in self.frame_cache:
                return self.frame_cache[name]
            else:
                if data_context is not None:
                    # TODO: Move this into DataContext and handle non-local reads
                    fr = r_pb_fs(os.path.join(data_context.get_object_dir(),
                                              self.pb.uuid,
                                              FrameRecord.make_filename(uuid)), FrameRecord)
                    # NOTE: UGLY -- one extra variable in the FrameRecord that is not in the FrameRecord.pb
                    # TODO: REMOVE this dependency.   Means we have to be very careful about FR copies
                    fr.hframe_uuid = self.pb.uuid
                elif testing_dir is not None:
                    fr = r_pb_fs(os.path.join(testing_dir, FrameRecord.make_filename(uuid)), FrameRecord)
                else:
                    fr = None
                return fr

        if names is None:
            name_uuids = [(k, v) for k, v in self.frame_dict.items()]
        else:
            name_uuids = [(k, self.frame_dict[k]) for k in names]

        return [_resolve_frame(self, data_context, name, uuid, testing_dir=testing_dir) for name, uuid in name_uuids]

    def get_frame_ids(self, names=None):
        """
        Get either all frames or frames by name.  Return (name,uuid) tuples

        Args:
            names (:list:str):  Names to retrieve or all if none.

        Returns:
            (:obj:list (str,str))
        """
        if names is None:
            names = list(self.frame_dict.keys())

        return [(name, self.frame_dict[name]) for name in names]

    def add_tags(self, tags):
        """
        Add tags to the hyperframe.

        Args:
            tags (:dict: (string, string)): dictionary of tags to add

        Returns:
            Nothing
        """

        for k, v in tags.items():
            t = self.pb.tags.add()
            t.k = k
            t.v = v
            self.tag_dict[k] = v

    def get_tag(self, name):
        """Retrieve tag if exists else None

        Args:
            name (:obj:list string): list of strings

        Returns:
            string: tag if present else None
        """

        assert (name is not None)
        if name in self.tag_dict:
            return self.tag_dict[name]
        else:
            return None

    def get_tags(self):
        """Retrieve dictionary of existing tags on hyperframe

        Returns:
            dict or None
        """

        return self.tag_dict

    def get_human_name(self):
        """Retrieve human name string.

        Returns:
            (str)
        """

        return self.pb.human_name

    def add_lineage(self, lin_obj):
        """
        Copy the pb from lin_obj into the pb.lineage here.
        Note: This is destructive, will overwrite if already set.
        :param lin_obj:
        :return:
        """
        self.pb.lineage.CopyFrom(lin_obj.pb)

    def get_lineage(self):
        """
        :return: LineageRecord with a copy of the lineage PB in this hframe
        """
        if self.pb.HasField("lineage"):
            return LineageRecord.copy_from_pb(self.pb.lineage)
        else:
            return None

    def to_string(self):
        s = "HumanName[{}] ProcName[{}] Timestamp[{}] Owner[{}] uuid[{}] lineage[{}] presentation[{}]".format(self.pb.human_name,
                                                                                                              self.pb.processing_name,
                                                                                                              self.pb.lineage.creation_date,
                                                                                                              self.pb.owner,
                                                                                                              self.pb.uuid,
                                                                                                              self.pb.lineage.depends_on,
                                                                                                              self.pb.presentation)
        return s


class LineageRecord(PBObject):

    table_name = 'lineage'

    def __init__(self, hframe_name=None, hframe_uuid=None,
                 code_repo=None, code_name=None, code_semver=None,
                 code_hash=None, code_branch=None,
                 creation_date=None, depends_on=None):
        """
        LineageRecord -- a collection of information about how this bundle was created.

        params
        :hframe_name    -  name of this bundle
        :bundle_uuid -  the uuid of this bundle in the objectrecord
        :code_repo      -  git repo where code exists
        :code_name      -  module.class
        :code_semver    -  semver from code_version
        :code_hash      -  githash from code_version
        :code_branch    -  name of branch
        :creation_date  -  Time this bundle was created
        :depends_on = array[ (hframe_name, version uuid), ... ]

        returns:
            LineageRecord
        """

        super(LineageRecord, self).__init__()
        self.pb = self._pb_type()
        self.pb.hframe_name = hframe_name
        self.pb.hframe_uuid = hframe_uuid
        self.pb.code_repo = code_repo
        self.pb.code_name = code_name
        self.pb.code_semver = code_semver
        self.pb.code_hash = code_hash
        self.pb.code_branch = code_branch

        if creation_date is None:
            creation_date = time.time()

        self.pb.creation_date = creation_date

        # Note: depends_on tuple hframe_name is the processing name, very confusing! -- Todo: change in pb spec
        if depends_on is not None:
            _ = [self.pb.depends_on.add(hframe_name = tup[0], hframe_uuid = tup[1]) for tup in depends_on]

    @staticmethod
    def _create_table(metadata):
        """
        Create unbound table object
        Only enter the items that we want to index / search on
        :return: Table
        """
        lineage = Table(LineageRecord.table_name, metadata,
                         Column('hframe_uuid', String(50), primary_key=True),# sqlite_on_conflict_primary_key=UPSERT_POLICY),
                         Column('hframe_name', String),
                         Column('code_repo', String),
                         Column('code_hash', String(50)),
                         Column('creation_date', DateTime), #TIMESTAMP),
                         Column('state', Enum(RecordState)),
                         Column('pb', BLOB)
                         )
        return lineage

    @staticmethod
    def _pb_type():
        """
        Return the hyperframe_pb2.<pb_type>()
        :return:
        """
        return hyperframe_pb2.Lineage()

    def _write_row(self):
        """
        :return: dictionary of key columns (from _create_table) and values.
        """
        assert(self.pb is not None)

        print("Lineage Writing row with TS {}".format(time.ctime(self.pb.lineage.creation_date)))

        return {'hframe_uuid': self.pb.hframe_uuid,
                'hframe_name': self.pb.hframe_name,
                'code_repo': self.pb.code_repo,
                'code_hash': self.pb.code_hash,
                'creation_date': datetime.fromtimestamp(self.pb.creation_date),
                'state': self.state,
                'pb': self.pb.SerializeToString()}

    def to_string(self):
        s = "hframe[{}] uuid[{}] Timestamp[{}] Repo[{}] GitHash[{}] ".format(self.pb.hframe_name,
                                                                             self.pb.hframe_uuid,
                                                                             self.pb.creation_date,
                                                                             self.pb.code_repo,
                                                                             self.pb.code_hash)
        return s

    def get_filename(self):
        """

        Returns:
            (str): <uuid>_<hframe,frame,auth>.pb

        """
        return "{}_lineage.pb".format(self.pb.uuid)


class FrameRecord(PBObject):

    table_name = 'frames'

    def __init__(self, name=None, hframe_uuid=None, type=None, shape=None, data=None, byteorder=None, hframes=None, links=None):
        """
        Data is held in "Frames."  These are individual tensors or n-dimensional vectors.

        :param name:  Human readable name for this "column" or "tensor"
        :param hframe_uuid:  UUID of owning hyperframe
        :param type:  Tensors hold data of a single type hyperframe_pb2.Type.
        :param shape: (x,y,...,z)
        :param data:    the inline byte array or array of strings if type == hyperframe_pb.STRING
        :param hframes (:list:`HyperFrameRecords`) :  List of HyperFrameRecords
        :param links:   An array of LinkRecords
        """

        super(FrameRecord, self).__init__()

        if not ((data is None) and (hframes is None) and (links is None)):
            assert( ((data is not None) and (hframes is None) and (links is None)) or
                    ((data is None) and (hframes is not None) and (links is None)) or
                    ((data is None) and (hframes is None) and (links is not None)) )

        # TODO: REMOVE this dependency.   Means we have to be very careful about FR copies
        self.hframe_uuid = hframe_uuid

        self.pb = self._pb_type()
        self.pb.uuid = str(uuid.uuid1())
        self.pb.name = name
        self.pb.type = hyperframe_pb2.Type.Value(type)

        if shape is not None:
            self.pb.shape.extend(shape)

        if hframes is not None:
            self.pb.hframes.extend([HyperFrameRecord.copy_from_pb(hfrcd.pb).pb for hfrcd in hframes])

        if links is not None:
            self.pb.links.extend([LinkBase.copy_from_pb(lrcd.pb).pb for lrcd in links])

        if data is not None:
            if self.pb.type == hyperframe_pb2.STRING:
                self.pb.strings.extend(data)
            else:
                self.pb.data = data
            if byteorder is not None:
                self.pb.byteorder = FrameRecord.get_proto_byteorder(byteorder)
            else:
                self.pb.byteorder = hyperframe_pb2.NA

        self.pb.ClearField('hash')

        self.pb.hash = hashlib.md5(self.pb.SerializeToString()).hexdigest()

    @staticmethod
    def _create_table(metadata):
        """
        Create unbound table object

        Args:
            metadata:

        Returns:
            Table

        """

        frame_tbl = Table(FrameRecord.table_name, metadata,
                          Column('uuid', String(50), primary_key=True),# sqlite_on_conflict_primary_key=UPSERT_POLICY),
                          Column('hframe_uuid', String(50)),
                          Column('name', String),
                          Column('state', Enum(RecordState)),
                          Column('pb', BLOB)
                          )
        return frame_tbl

    @staticmethod
    def _pb_type():
        """

        Returns:
            the hyperframe_pb2.<pb_type>()

        """

        return hyperframe_pb2.Frame()

    def _write_row(self):
        """

        Returns:
            dictionary of key columns (from _create_table) and values.

        """

        assert(self.pb is not None)
        return {'hframe_uuid': self.hframe_uuid,
                'uuid': self.pb.uuid,
                'name': self.pb.name,
                'state': self.state,
                'pb': self.pb.SerializeToString()}

    def get_uuid(self):
        """
        Return the uuid for this frame.

        Returns:
            uuid (str)
        """
        return self.pb.uuid

    def get_hframes(self):
        """
        NOTE: This is returning copies!

        Returns:
            (:list:`HyperFrameRecords`): The ordered set of hyperframes in this frame or None

        """
        assert self.pb.type == hyperframe_pb2.HFRAME
        return [HyperFrameRecord.copy_from_pb(pb) for pb in self.pb.hframes]

    def get_link_urls(self):
        """
        Assuming a link FrameRecord, return all the URLs in the frame

        Returns:
            (:list:str):  An ordered set of link URLs

        """
        assert self.pb.type == hyperframe_pb2.LINK
        return [LinkBase.find_url(link) for link in self.pb.links]

    def get_links(self):
        """
        Assuming a link FrameRecord, return all the links

        Returns:
            (:list:str):  An ordered set of link URLs

        """
        return self.pb.links

    @staticmethod
    def make_filename(uuid):
        return "{}_frame.pb".format(uuid)

    def get_filename(self):
        """

        Returns:
            (str): <uuid>_<hframe,frame,auth>.pb

        """
        return FrameRecord.make_filename(self.pb.uuid)

    def add_links(self, links):
        """
        Add links to this frame.

        Returns:
            (`hyperframe.FrameRecord`)

        """
        assert(self.is_link_frame())
        assert(len(self.pb.links) == 0)

        self.pb.links.extend([LinkBase.copy_from_pb(lrcd.pb).pb for lrcd in links])

        self.pb.shape.extend((len(links),))

        self.pb.ClearField('hash')

        self.pb.hash = hashlib.md5(self.pb.SerializeToString()).hexdigest()

        return self

    def mod_hfr_uuid(self, new_hfr_uuid):
        """
        Modify this frame.  Replace the hfr uuid.  Also replace the current uuid of this frame.

        Args:
            new_hfr_uuid:

        Returns:
            (`hyperframe.FrameRecord`)
        """
        self.hframe_uuid = new_hfr_uuid

        self.pb.uuid = str(uuid.uuid1())

        self.pb.ClearField('hash')

        self.pb.hash = hashlib.md5(self.pb.SerializeToString()).hexdigest()

        return self

    @staticmethod
    def is_link_series(series_like):
        """

        NOTE: Expect a series of scalars.  Nested objects become json strings.  Yuck!

        Args:
            series_like:  a list-like

        Returns:
            (bool): Whether the series | ndarray appears to be a link column
        """

        # Welcome to duck typing.   Get the first element of
        # the series and check to see if it is some kind of recognizable
        # file element. If we get a TypeError (does not implement
        # __getitem__) or an attribute error (not a string) then we
        # definitely do not have a link series.
        try:
            tester = series_like[0]
            if isinstance(tester, luigi.Target):
                return True
            elif isinstance(tester, DBLink):
                return True
            elif (tester.startswith('file:///') or
                  tester.startswith('s3://') or
                  tester.startswith('db://')
                  ):
                return True
            else:
                return False
        except AttributeError:
            return False
        except TypeError:
            return False

    def is_link_frame(self):
        """
        Whether this frame contains links or not.

        Returns:
            (bool): whether this frame contains links or native data (False)
        """
        return self.pb.type == hyperframe_pb2.LINK

    def is_local_fs_link_frame(self):
        """
        Whether this frame contains local fs links

        Returns:
            (bool):
        """
        if not self.is_link_frame():
            return False

        assert(len(self.pb.links) > 0)
        link_pb = self.pb.links[0]
        return link_pb.WhichOneof('link') == 'local'

    def is_s3_link_frame(self):
        """
        Whether this frame contains s3 links

        Returns:
            (bool):
        """
        if not self.is_link_frame():
            return False

        assert(len(self.pb.links) > 0)
        link_pb = self.pb.links[0]
        return link_pb.WhichOneof('link') == 's3'

    def is_db_link_frame(self):
        """
        Whether this frame contains db links

        Returns:
            (bool):
        """
        if not self.is_link_frame():
            return False
        assert(len(self.pb.links) > 0)
        link_pb = self.pb.links[0]
        return link_pb.WhichOneof('link') == 'database'

    def is_hfr_frame(self):
        """
        Whether this frame contains hyperframes or not

        Returns:
            (bool): whether this frame contains links or native data (False)
        """
        return self.pb.type == hyperframe_pb2.HFRAME

    @staticmethod
    def get_proto_byteorder(numpy_byteorder):
        numpy_endianness = {
            '<': hyperframe_pb2.LITTLE,
            '>': hyperframe_pb2.BIG,
            '|': hyperframe_pb2.NA,
        }

        system_endianness = {
            'little': hyperframe_pb2.LITTLE,
            'big': hyperframe_pb2.BIG,
        }

        if numpy_byteorder is '=':
            return system_endianness[sys.byteorder]

        return numpy_endianness[numpy_byteorder]

    @staticmethod
    def get_numpy_byteorder(proto_byteorder):
        proto_endianness = {
            hyperframe_pb2.LITTLE: '<',
            hyperframe_pb2.BIG: '>',
            hyperframe_pb2.NA: '|'
        }

        return proto_endianness[proto_byteorder]

    @staticmethod
    def get_numpy_type(proto_type):
        numpy_types = {
            hyperframe_pb2.BOOL:    np.bool_,
            hyperframe_pb2.INT8:    np.int8,
            hyperframe_pb2.INT16:   np.int16,
            hyperframe_pb2.INT32:   np.int32,
            hyperframe_pb2.INT64:   np.int64,
            hyperframe_pb2.UINT8:   np.uint8,
            hyperframe_pb2.UINT16:  np.uint16,
            hyperframe_pb2.UINT32:  np.uint32,
            hyperframe_pb2.UINT64:  np.uint64,
            hyperframe_pb2.FLOAT16: np.float16,
            hyperframe_pb2.FLOAT32: np.float32,
            hyperframe_pb2.FLOAT64: np.float64,
            hyperframe_pb2.OBJECT:  np.object_
            # Special Case -- manual conversion on string type -- hyperframe_pb2.STRING:  np.string_
        }

        if proto_type in numpy_types:
            return numpy_types[proto_type]
        raise KeyError('Could not find a message array type for {}'.format(proto_type))

    @staticmethod
    def get_proto_type(numpy_type):
        """
        Note, we require the scalar numpy type, not the dtype.  If we find the dtype, convert to its
        scalar type with isinstance check.

        Args:
            numpy_type:

        Returns:
            (str): The string represerntation of the hyperframe_pb enumeration of this type

        """
        proto_types = {
            np.bool_:   'BOOL',
            np.int8:    'INT8',
            np.int16:   'INT16',
            np.int32:   'INT32',
            np.int64:   'INT64',
            np.uint8:   'UINT8',
            np.uint16:  'UINT16',
            np.uint32:  'UINT32',
            np.uint64:  'UINT64',
            np.float_:  'FLOAT64',
            np.float16: 'FLOAT16',
            np.float32: 'FLOAT32',
            np.float64: 'FLOAT64',
            six.binary_type: 'STRING',
            six.text_type:   'STRING',
            np.unicode_: 'STRING',
            np.string_: 'STRING',
            np.object_: 'OBJECT'
        }

        if isinstance(numpy_type, np.dtype):
            numpy_type = numpy_type.type

        if numpy_type in proto_types:
            return proto_types[numpy_type]
        raise KeyError('Could not find a message array type for {}'.format(numpy_type))

    def to_ndarray(self):
        """
        Convert a Frame to a numpy ndarray

        Returns:
            (`numpy.ndarray`):

        """

        if self.pb.type == hyperframe_pb2.HFRAME:
            # Choose to pass HyperFrames as UUIDs
            nda = np.array([hf_pb.uuid for hf_pb in self.pb.hframes], dtype=np.string_)

        elif self.pb.type == hyperframe_pb2.LINK:
            nda = np.array([LinkBase.find_url(lr) for lr in self.pb.links])

        elif self.pb.type == hyperframe_pb2.STRING:
            if len(self.pb.strings) > 0:
                if isinstance(self.pb.strings[0], six.binary_type):
                    nda = np.array(self.pb.strings, dtype=six.binary_type)
                elif isinstance(self.pb.strings[0], six.text_type):
                    nda = np.array(self.pb.strings, dtype=six.text_type)
                else:
                    raise Exception(
                        "Unable to convert pb strings to suitable type for ndarray {}".format(type(self.pb.strings[0])))
            else:
                nda = np.array(self.pb.strings)  # nothing there, defaults to object array
        else:
            nda = self.make_numpy_array()

        return nda

    def to_series(self):
        """
        Convert a Frame to a Pandas series.

        Returns:
            ('pandas.core.series.Series`):
        """

        nda = self.to_ndarray()
        if nda.ndim == 0:
            nda = nda.reshape((1,))

        return pd.Series(data=nda, name=self.pb.name)

    def make_numpy_array(self):
        """
        Create a np ndarray from native bytes in frame

        Returns:
            (`numpy.ndarray`)
        """

        assert (self.pb.type != hyperframe_pb2.LINK)
        assert (self.pb.type != hyperframe_pb2.HFRAME)
        assert (self.pb.type != hyperframe_pb2.STRING)

        dtype = np.dtype(FrameRecord.get_numpy_type(self.pb.type))
        dtype = dtype.newbyteorder(FrameRecord.get_numpy_byteorder(self.pb.byteorder))

        nda = np.frombuffer(self.pb.data, dtype=dtype)
        nda = nda.reshape(self.pb.shape)

        return nda

    @staticmethod
    def from_ndarray(hfid, name, nda):
        """
        Create frame pb from numpy ndarray

        Args:
            hfid:
            name:
            nda:

        Returns:

        """

        if nda.dtype.type == np.object_:
            # NOTE: EXPENSIVE TESTS for STRINGS that come from ndarrays inside of Pandas series
            if all(isinstance(x, six.binary_type) for x in nda):
                frame_type = FrameRecord.get_proto_type(six.binary_type)
                series_data = nda
            elif all(isinstance(x, six.text_type) for x in nda):
                frame_type = FrameRecord.get_proto_type(six.text_type)
                series_data = nda
            else:
                # ESCAPE HATCH -- Made from duct tape and JSON
                import json
                frame_type = FrameRecord.get_proto_type(str)
                series_data = [json.dumps(element) for element in nda]
                # raise Exception("make_native_frame does not yet support non-string objects")

        elif nda.dtype.type == np.unicode_ or nda.dtype.type == np.string_:
            # If it's an ndarray containing scalar string types
            frame_type = FrameRecord.get_proto_type(nda.dtype)
            if len(nda.shape) == 0:
                series_data = [nda.item()]
            else:
                series_data = nda
        else:
            frame_type = FrameRecord.get_proto_type(nda.dtype)
            series_data = nda.tobytes()

        frame = FrameRecord(name=name,
                            hframe_uuid=hfid,
                            type=frame_type,
                            byteorder=nda.dtype.byteorder,
                            shape=nda.shape,
                            data=series_data)

        return frame

    @staticmethod
    def from_serieslike(hfid, name, series_like):
        """
        Create frame pb from pandas Series

        Args:
            hfid (str): hyperframe id
            name (str): column name
            series_like (`pandas.Series`, `numpy.ndarray`): pandas series | ndarray

        Returns:
            (`FrameRecord`)
        """

        if isinstance(series_like, pd.Series):
            series_like = series_like.values

        if all(isinstance(x, HyperFrameRecord) for x in series_like):
            return FrameRecord.make_hframe_frame(hfid, name, series_like)
        else:
            return FrameRecord.from_ndarray(hfid, name, series_like)

    @staticmethod
    def make_hframe_frame(hfid, name, hframes):
        """
        Given a list of hframes, return a Frame containing them.

        Args:
            hfid (str):  The hyperframe id
            name (str):  Name of this Frame
            hframes (list:`hyperframe.HyperFrameRecord`):

        Returns:
            (FrameRecord)
        """

        frame = FrameRecord(name=name,
                            hframe_uuid=hfid,
                            type='HFRAME',
                            shape=(len(hframes),),
                            hframes=hframes)
        return frame

    @staticmethod
    def make_link_frame(hfid, name, file_paths, managed_path):
        """ Create link frame from file paths (file, s3, or db) or luigi.Target objects.

        Assumes file_paths are 'file:///' or 's3://' or 'db://'
        Assumes that the files are already copied into the bundle directory.

        Note: This will only store the *relative* path of the link object (except for db)

        Note: No LinkAuth yet.

        Args:
            hfid: hyperframe id
            name: column name
            file_paths (:list:str): array of paths or luigi.Target objects
            managed_path (str): The current directory structure

        Returns:
            (FrameRecord)
        """

        if isinstance(file_paths[0], luigi.LocalTarget):
            file_paths = ['file://{}'.format(lt.path) if lt.path.startswith('/') else lt.path for lt in file_paths]

        if isinstance(file_paths[0], DBLink):
            link_type = DatabaseLinkRecord
        elif file_paths[0].startswith('file:///'):
            link_type = FileLinkRecord
        elif file_paths[0].startswith('s3://'):
            link_type = S3LinkRecord
        elif file_paths[0].startswith('db://'):
            _logger.error("Found string-based database reference[{}], use DBLink object instead.".format(file_paths[0]))
            raise Exception("hyperframe:make_link_frame: error trying to copy in string-based database reference.")
        else:
            raise ValueError("Bad file paths -- cannot determine link type: example path {}".format(file_paths[0]))

        if link_type is FileLinkRecord:
            to_remove = "file:///" + managed_path
        elif link_type is S3LinkRecord:
            to_remove = "s3://" + managed_path

        frame = FrameRecord(name=name,
                            hframe_uuid=hfid,
                            type='LINK')

        frame_uuid = frame.get_uuid()

        if link_type is DatabaseLinkRecord:
            # What the user sees in a db link URL
            # db://<database>.<schema>.<disdat>_<context>_<virt_name>_<uuid prefix>@servername
            links = [link_type(frame_uuid,  # hframe_uuid
                               None,  # linkauth_uuid
                               db_tgt.url(), # url
                               db_tgt.servername, # servername
                               db_tgt.database,   # database
                               db_tgt.schema,     # schema
                               db_tgt.tn, # table name, i.e, no schema, disdat_prefix, context, or uuid
                               None, # columns
                               db_tgt.port, # port
                               db_tgt.dsn # data source name
                               ) for db_tgt in file_paths]
        else:
            file_paths = [common.BUNDLE_URI_SCHEME + fn[len(to_remove):] for fn in file_paths]
            links = [link_type(frame_uuid, None, fn) for fn in file_paths]

        return frame.add_links(links)

"""
Tables

contexts -- set of contexts.

hframes  -- hyperframes
frames   -- frames referencing their hframe
lineage  -- lineage referencing their hframe
link     -- link information referencing hframe
linkauth -- unique linkauth records, links may refer to them by uuid, but policy
            may dictate whether the user can get access to the linkauth.

We use SqlAlchemy to give us one way of interacting with a database.   The user may have
a database locally (sqlite) but we might have a server (postgres) that also has these tables.
To have our objects be read/written identically we leverage sqlalchemy's core.

Each object has a
write_row(connection) - uses existing connection to write the row
read_row(connection)  - uses existing connection read the row and return a new object
"""


class LinkAuthBase(PBObject):
    """
    The authoritative information in a Link.
    This is effectively a capability or key.
    Each one is uniquely identified.
    They have no owner, they may be passed around, be careful.

    row (uuid, type, blob)
    """

    table_name = 'linkauth'

    def __init__(self):
        super(LinkAuthBase, self).__init__()
        self.pb = self._pb_type()

    @staticmethod
    def _create_table(metadata):
        """
        Create unbound table object
        :return: Table
        """
        linkauth = Table(LinkAuthBase.table_name, metadata,
                         Column('uuid', String(50), primary_key=True),# sqlite_on_conflict_primary_key=UPSERT_POLICY),
                         Column('profile', String),
                         Column('state', Enum(RecordState)),
                         Column('pb', BLOB)
                         )
        return linkauth

    @staticmethod
    def _pb_type():
        """
        Return the hyperframe_pb2.<pb_type>()
        :return:
        """
        return hyperframe_pb2.LinkAuth()

    def _write_row(self):
        """
        :return: dictionary of key columns (from _create_table) and values.
        """
        assert(self.pb is not None)
        return {'uuid': self.pb.uuid,
                'profile': self.pb.profile,
                'state': self.state,
                'pb': self.pb.SerializeToString()}

    def __deploy_ini(self, ini_file):
        """
        Update INI file with dict in object
        If it exists, update the profile in profile with the information here.
        :return:
        """
        from six.moves import configparser

        config = configparser.RawConfigParser()
        if os.path.exists(ini_file):
            config.read(ini_file)

        config.add_section(self.pb.profile)
        for k,v in self.__dict__.items():
            if v is not None:
                config.set(self.pb.profile, k, v)

        with open(ini_file, 'wb') as configfile:
            config.write(configfile)

    def get_filename(self):
        """

        Returns:
            (str): <uuid>_<hframe,frame,auth>.pb

        """
        return "{}_auth.pb".format(self.pb.uuid)


class S3LinkAuthRecord(LinkAuthBase):
    """
    Information required to access an S3 bucket
    """
    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None,
                 aws_session_token=None, profile=None):
        super(S3LinkAuthRecord, self).__init__()

        self.pb.profile = 'default-disdat' if profile is None else profile
        self.pb.uuid = str(uuid.uuid1())

        self.pb.s3_auth.aws_access_key_id = aws_access_key_id
        self.pb.s3_auth.aws_secret_access_key = aws_secret_access_key
        self.pb.s3_auth.aws_session_token = aws_session_token

        self.pb.ClearField('hash')

        self.pb.hash = hashlib.md5(self.pb.SerializeToString()).hexdigest()

        assert (self.pb.IsInitialized())

    def deploy(self):
        """
        Deploy ini file updates
        """
        self.__deploy_ini("~/.aws/test_credentials")


class DBLinkAuthRecord(LinkAuthBase):
    """
    DB Authentication information
    Note: Does not contain password, instead capture description (DSN)
    """
    def __init__(self, driver, description, database, servername, uid, pwd, port, sslmode, profile=None):
        super(DBLinkAuthRecord, self).__init__()

        self.pb.profile = 'default-disdat' if profile is None else profile
        self.pb.uuid = str(uuid.uuid1())

        self.pb.db_auth.driver = driver
        self.pb.db_auth.description = description
        self.pb.db_auth.database = database
        self.pb.db_auth.servername = servername
        self.pb.db_auth.uid = uid
        self.pb.db_auth.pwd = pwd
        self.pb.db_auth.port= port
        self.pb.db_auth.sslmode = sslmode

        self.pb.ClearField('hash')
        self.pb.hash = hashlib.md5(self.pb.SerializeToString()).hexdigest()

        assert (self.pb.IsInitialized())

    def deploy(self):
        """
        Deploy ini file updates
        :return:
        """
        self.__deploy_ini("~/.odbc.ini")


class LinkBase(PBObject):
    """
    Base class with getters / setters
    When using __metaclass__ it means we are interposing on type()
    and we are using ABCMeta as our class creator
    """

    table_name = 'links'

    def __init__(self, frame_uuid, linkauth_uuid=None):
        super(LinkBase, self).__init__()
        self.pb = self._pb_type()
        self.pb.frame_uuid = frame_uuid
        if linkauth_uuid is not None:
            self.pb.linkauth_uuid = linkauth_uuid

    def get_linkauth(self):
        assert (self.pb is not None)
        return self.pb.linkauth_uuid

    def set_linkauth(self, linkauth_uuid):
        assert (self.pb is not None)
        self.pb.linkauth_uuid = linkauth_uuid

    @staticmethod
    def _create_table(metadata):
        """
        id   -- primary key auto-increment
        frame_uuid -- hyperframe uuid
        linkauth_uuid -- linkauth uuid
        url  -- Most links have some form of URL
        pb   -- protocol buffer blob
        :return: Table
        """
        link = Table(LinkBase.table_name, metadata,
                     Column('frame_uuid', String(50)),
                     Column('linkauth_uuid', String(50)),
                     Column('url', Text),
                     Column('state', Enum(RecordState)),
                     Column('pb', BLOB)
                     )
        return link

    @staticmethod
    def _pb_type():
        """
        :return: hyperframe_pb2.Link()
        """
        return hyperframe_pb2.Link()

    @staticmethod
    def find_url(link_pb):
        """
        **Update Me if You Add a New Link Type**

        Return the URL-like string of the link.
        :param link_pb: the link-like pb
        :return: an URL-like string
        """
        if link_pb.WhichOneof('link') == 's3':
            url = link_pb.s3.url
        elif link_pb.WhichOneof('link') == 'local':
            url = link_pb.local.path
        elif link_pb.WhichOneof('link') == 'database':
            url = link_pb.database.url
        else:
            url = None

        return url

    def _write_row(self):
        """
        Returns:
             (dict): Dictionary of key columns (from _create_table) and values.
        """
        assert (self.pb is not None)

        url = LinkBase.find_url(self.pb)

        return {'frame_uuid': self.pb.frame_uuid,
                'linkauth_uuid': self.pb.linkauth_uuid,
                'url':  url,
                'state': self.state,
                'pb': self.pb.SerializeToString()}

    def get_managed_path(self):
        """
        :return: The directory where this data-thing resides
        """
        assert (self.pb is not None)
        return os.path.dirname(LinkBase.find_url(self.pb))

    def get_filename(self):
        """

        Returns:
            (str): <uuid>_<hframe,frame,auth>.pb

        """
        return "{}_link.pb".format(self.pb.uuid)


# With BUNDLE_URI_SCHEME file and s3 link records start looking a *lot* similar.
# The bundle representation doesn't change.  The file could be on s3 or it could be local.
# The meta data does not change.
# TODO: Unify these types

class FileLinkRecord(LinkBase):
    def __init__(self, hframe_uuid, linkauth_uuid, path):
        """

        Args:
            hframe_uuid (str):
            linkauth_uuid (str):
            path (str):  Local path to file
        """
        super(FileLinkRecord, self).__init__(hframe_uuid, linkauth_uuid)
        assert (path.startswith(common.BUNDLE_URI_SCHEME))
        self.pb.local.path = path

        self.pb.ClearField('hash')
        self.pb.hash = hashlib.md5(self.pb.SerializeToString()).hexdigest()
        # XXX Add size?   self.size = 0
        assert (self.pb.IsInitialized())


class S3LinkRecord(LinkBase):
    def __init__(self, hframe_uuid, linkauth_uuid, url):
        """

        Args:
            hframe_uuid:
            linkauth_uuid:
            url:
        """
        super(S3LinkRecord, self).__init__(hframe_uuid, linkauth_uuid)
        assert (url.startswith(common.BUNDLE_URI_SCHEME))
        self.pb.s3.url = url

        self.pb.ClearField('hash')
        self.pb.hash = hashlib.md5(self.pb.SerializeToString()).hexdigest()
        # XXX Add size?   self.size = 0
        assert (self.pb.IsInitialized())


class DatabaseLinkRecord(LinkBase):
    def __init__(self, hframe_uuid, linkauth_uuid, url, servername, database, schema, table, columns, port, dsn):
        """

        At this time we store the DSN in the database_link.   This is to avoid users placing userids and passwords
        in code to create DBLinks.  Only committed bundles can be shared, so only the user creating the bundle
        should be able to commit it.

        Args:
            hframe_uuid (str):  The UUID of the hyperframe
            linkauth_uuid (str): The UUID of the linkauth.  Currently unused.
            url (str): The string passed to the user representing this resource: "db://<virt-table>".  Note that we transform
            this into the presented name in the bundle based on the context, uuid, and whether this bundle is committed.
            servername (str):  DNS name for the database in question
            database (str):    The name of the database containing the schema
            schema (str):      The schema name
            table (str):       The virtual table name
            columns (list:str): A list of strings of column names.  Currently unused.
            port (int):  The port at which the server is listening
            dsn (str): data source name
        """
        super(DatabaseLinkRecord, self).__init__(hframe_uuid, linkauth_uuid)
        assert (url.startswith('db://'))
        self.pb.database.url = url
        self.pb.database.servername = servername
        self.pb.database.database = database
        self.pb.database.schema = schema
        self.pb.database.table = table
        self.pb.database.columns.extend(columns)
        self.pb.database.port = port
        self.pb.database.dsn = dsn

        self.pb.ClearField('hash')
        self.pb.hash = hashlib.md5(self.pb.SerializeToString()).hexdigest()
        assert (self.pb.IsInitialized())


