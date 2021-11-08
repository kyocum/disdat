#
# Copyright 2015, 2016, 2017  Human Longevity, Inc.
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
A DisDat context
"""
import os
import json
import glob
import shutil

from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import luigi
from luigi.contrib.s3 import S3Target
import urllib
import boto3

import disdat.constants as constants
import disdat.hyperframe_pb2 as hyperframe_pb2
import disdat.hyperframe as hyperframe
import disdat.common as common
import disdat.utility.aws_s3 as aws_s3
from disdat.common import DisdatConfig
from disdat.db_link import DBLink
from disdat import logger as _logger


META_CTXT_FILE = 'ctxt.json'
DB_FILE = 'ctxt.db'
DEFAULT_LEN_UNCOMMITTED_HISTORY = 1


class DataContext(object):
    """
    State for a particular data context.

    Every data context is indexed by a database.   The index can be rebuilt by scanning
    all the objects.  Each context organizes its repository in the following way.
    .disdat/context/<context name>/objects/<uuid>/{uuid_hframes.pb, uuid_frames.pb, uuid_auths.pb}

    For local operation, the context is indexed into a sqlite db.   For shared operation,
    it may be indexed by a postgres database.

    It's only valid if we've written it to disk or read it from disk

    Note:  Local contexts are stored in the ~/.disdat directory

    Note:  There is also meta information in the local db in .disdat

    """

    def __init__(self, ctxt_dir, remote_ctxt=None, local_ctxt=None, remote_ctxt_url=None):
        """
        Data context resides in file:///meta_dir/context/<context_name>/
        Objects are in          file:///meta_dir/context/<context_name>/objects/<uuid>/{uuid_<type>.pb}
        DB is in                file:///meta_dir/context/<context_name>/ctxt.db

        Assumes that the context has already been made via DataContext.create_branch()

        It may also be backed by a "remote" db.

        Args:
            ctxt_dir:  Where the contexts are stored
            remote_ctxt: The remote context name
            local_ctxt:  The local context name
            remote_ctxt_url:  The URL of the db for the global context

        """
        self.local_ctxt_dir = ctxt_dir
        self.remote_ctxt = remote_ctxt
        self.local_ctxt = local_ctxt
        self.remote_ctxt_url = remote_ctxt_url
        self.local_engine = None
        self.remote_engine = None
        self.valid = False
        self.len_uncommitted_history = DEFAULT_LEN_UNCOMMITTED_HISTORY

        self.init_local_db()
        self.init_remote_db()

    @staticmethod
    def create_branch(ctxt_dir, local_ctxt_name):
        """
        Initialize the directory for a new branch from this data repo.

        Args:
            ctxt_dir:
            local_ctxt_name:

        Returns:
        """
        # if the path exists, but there's nothing there, then assume we can remake it.
        local_ctxt_dir = os.path.join(ctxt_dir, local_ctxt_name, 'objects')
        if os.path.exists(local_ctxt_dir):
            assert (len(os.listdir(local_ctxt_dir)) == 0)
        else:
            os.makedirs(local_ctxt_dir)

    def delete_context(self, force=False):
        """
        Any checks on the local context before we delete all the objects in here?

        Returns:

        """
        if self.unpushed_data() and not force:
            print(("Disdat found un-pushed data in context {}, use -f to delete".format(self.local_ctxt)))
            return

        self.local_engine.dispose()

    def bind_remote_ctxt(self, remote_context, s3_url):
        """
        A local branch can be bound to a remote shared FS where you can push/pull hyperframes.
        If the remote_context directory does not exist, a push will create it.

        This call expects the bucket in `s3_url` to exist, but the key does not have to point to an existing object.

        Note that pull is lazy.  If the user rebinds to a new remote context then Disdat will not
        be able to localize bundles from the prior context.

        Args:
            remote_context (str): remote context name
            s3_url (str):  remote context url -- this points to the root of all disdat data -- does not include context dir

        Returns:
            None

        """
        assert (urllib.parse.urlparse(s3_url).scheme == 's3')

        if self.remote_ctxt_url is not None and self.remote_ctxt == remote_context and \
                        os.path.normpath(os.path.dirname(self.remote_ctxt_url)) == os.path.normpath(s3_url):
            print("Context already bound to remote at {}".format(s3_url))
            return

        bucket, key = aws_s3.split_s3_url(s3_url)
        if not aws_s3.s3_bucket_exists(bucket):
            _logger.error("Unable to bind context {} because bucket {} does not exist.".format(remote_context, bucket))
            raise RuntimeError

        if self.remote_ctxt_url is not None:
            print("You are re-binding this local context to a new remote context.")
            print("There may be un-localized bundles.")

        _logger.debug("Binding local branch {} context {} to URL {}".format(self.local_ctxt, self.remote_ctxt, s3_url))
        self.remote_ctxt = remote_context
        self.remote_ctxt_url = os.path.join(s3_url, common.DISDAT_CONTEXT_DIR)
        self.save()

    def unbind_remote_ctxt(self):
        """
        Remove remote context binding.

        Args:
            s3_url:

        Returns:
            None

        """
        _logger.debug("Un-binding local branch {} context {} current URL {} to None".format(self.local_ctxt,
                                                                                            self.remote_ctxt,
                                                                                            self.remote_ctxt_url))
        self.remote_ctxt_url = None
        self.save()

    def unpushed_data(self):
        """
        Determine if there is data that has not been pushed to origin

        Returns:
            (bool)
        """
        return False

    def save(self):
        """
        Write out the context's meta data as json to .disdat directory.
        This is distinct from the state of all the hyperframes / bundles.
        That state is in the local db and the local FS.

        Returns:
            None
        """
        assert(os.path.isdir(self._get_local_context_dir()))

        meta_ctxt_file = os.path.join(self._get_local_context_dir(), META_CTXT_FILE)

        with open(meta_ctxt_file, 'w') as json_file:
            save_dict = {'remote_ctxt': self.remote_ctxt,
                         'local_ctxt': self.local_ctxt,
                         'remote_ctxt_url': self.remote_ctxt_url}
            json_file.write(json.dumps(save_dict))

    @staticmethod
    def load(target_contexts=[]):
        """
        Load the data contexts described at meta_dir.  Each of these is a "remote."
        Args:
            target_contexts (list(str)): If not None, try to load just this context.

        Returns:
            (dict) of 'name':context pairs.

        """
        ctxt_dir = DisdatConfig.instance().get_context_dir()

        if ctxt_dir is None:
            raise Exception("Unable to load context without a metadata directory argument")

        contexts = {}

        files = glob.glob(os.path.join(ctxt_dir, '*'))

        for ctxt in files:
            if len(target_contexts) > 0 and ctxt not in target_contexts:
                continue

            #_logger.debug("Loading context {}...".format(ctxt))
            meta_file = os.path.join(ctxt_dir, ctxt, META_CTXT_FILE)

            if not os.path.isfile(meta_file):
                _logger.debug("No disdat {} meta ctxt data file found.".format(meta_file))
            else:
                with open(meta_file, 'r') as json_file:
                    dc_dict = json.loads(json_file.readline())
                    dc = DataContext(ctxt_dir, **dc_dict)
                contexts[dc.local_ctxt] = dc

        return contexts

    def _get_local_context_dir(self):
        """
        Return the current local context directory

        Returns:
            (str): The directory of the whole context
        """
        return os.path.join(self.local_ctxt_dir, self.local_ctxt)

    def get_object_dir(self):
        """
        Return the current contexts object directory

        Returns:
            (str): The directory where we store objects
        """
        return os.path.join(self._get_local_context_dir(), constants._MANAGED_OBJECTS)

    @staticmethod
    def s3_remote_from_url(remote_ctxt_url):
        """ remove '/context' """
        if remote_ctxt_url is None:
            return 'None'
        else:
            return remote_ctxt_url[:-len('/context')]

    @staticmethod
    def extract_uuid_from_pb_path(pb_path, pb_type):
        """
        Extract the uuid that we put in each pb file name.
        Both hframes and frames are <uuid>_frame.pb

        Args:
            pb_path(str):
            pb_type(disdat.hyperframe.PBObject): either hyperframe.HyperFrameRecord or hyperframe.FrameRecord

        Returns:
            str: the uuid or None
        """
        if pb_type == hyperframe.FrameRecord:
            frame_file_suffix = '_frame.pb'
        elif pb_type == hyperframe.HyperFrameRecord:
            frame_file_suffix = '_hframe.pb'
        else:
            assert False, "Unknown pb_type {} in extract_uuid_from_pb_path.".format(pb_type)
        assert pb_path.endswith(frame_file_suffix)
        return pb_path[:-len(frame_file_suffix)]

    def get_remote_object_dir(self):
        """
        Where objects live on remote.

        Returns:
            (str):
        """
        if self.remote_ctxt_url is None:
            return None
        return os.path.join(self.remote_ctxt_url, self.remote_ctxt, constants._MANAGED_OBJECTS)

    def get_remote_name(self):
        return self.remote_ctxt

    def get_local_name(self):
        return self.local_ctxt

    @property
    def context(self):
        """ Return fully qualified context string """
        return f"local [{self.local_ctxt}] remote [{self.remote_ctxt}@{self.remote_ctxt_url}/{self.remote_ctxt}]"

    def init_remote_db(self):
        """
        Currently a no-op.  Will connect to something like dynamodb
        when we have external indices for objects in cloud storage (aka S3).

        Called when we first create a data_context object.
        At this point it may or may not have a remote.
        If we have a remote, then we assume AWS access.
        If we have AWS access, then we try to associate with dynamo.

        """
        if self.remote_ctxt_url is None:
            return

        # Enable when we start to use Dynamo for an index.
        if not self.remote_engine and False:
            try:
                self.remote_engine = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")
            except Exception as e:
                _logger.debug("Failed to get dynamo AWS resource: {}".format(e))
        return

    def init_local_db(self, in_memory=False):
        """
        Initialize the data context's local database (sqlite).
        If there is already a sqlite db, use that.
        Otherwise build db.

        Create db engine.  If no location, create in memory database engine.
        Can be used with either a local, in memory, or remote db

        Examples
        'postgresql://scott:tiger@localhost:5432/mydatabase'
        'sqlite:////absolute/path/to/foo.db'
        'sqlite:///:memory:'

        Args:
            in_memory: Directory where we expect the current context to be cached.
            force_rebuild: Force the rebuild even if ctxt.db exists.

        Returns:
            None

        """

        if in_memory:
            _logger.debug("Building in-memory database from local state...")
            self.local_engine = create_engine('sqlite:///:memory:', echo=False)
            self.rebuild_db()
        else:
            db_file = os.path.join(self._get_local_context_dir(), DB_FILE)
            self.local_engine = create_engine('sqlite:///' + db_file, echo=False)
            if not os.path.isfile(db_file):
                _logger.debug("No disdat {} local db data file found.".format(db_file))
                _logger.debug("\t  Rebuilding local database from local state...".format(db_file))
                self.rebuild_db()
        self.dbck()
        return

    @staticmethod
    def _weak_validate_hframe(hfr, found_frames_uuids):
        """
        This weakly validates the hframe.  First this is mainly called when we are rebuilding the
        database.  It is the case that occasionally the s3 copy does not copy down the frames (something
        to investigate).  In that case we do not want to index a hyperframe that is not complete on disk.

        But, to do a full check means we have to read each and every protocol buffer file.  When there
        are very large contexts (with 18 million pb's), this can take a long time.  Especially running
        on containers connected to EBS volumes with a limited budget of IO's.

        We make sure that there is a file path with the listed frame's UUID found
        inside the hframe.   But unlike _validate_hframe we do not validate the contents
        of the frame itself.  That is we do not look for each of the files the frame
        may refer to.

        NOTE: Only call this from rebuild_db to avoid more file reads than necessary.
        TODO: Implement dbck that can check the db on a machine with a dedicated ssd/nvm store.

        Args:
            hfr:
            found_frames_uuids (dict[str]): dict of frame uuids that exist on disk.

        Returns:
            bool: true if hframe passes weak test.

        """
        for str_tuple in hfr.pb.frames:
            fr_name = str_tuple.k
            fr_uuid = str_tuple.v
            if fr_uuid not in found_frames_uuids:
                # Frame was not on disk
                _logger.warn("HyperFrame {} Frame {} {} not present on disk".format(hfr.pb.uuid, fr_name, fr_uuid))
                return False
        return True

    @staticmethod
    def _validate_hframe(hfr, found_frames, found_auths):
        """
        for each frame UUID, is on disk?
        If yes
            Then for each frame if a link frame,
                for each link, is file in folder
                for each link, do we have the auth

        If all yes, then make sure this is in the db

        If any no, then we have an incomplete hframe
        a.) could be corrupted
        b.) could be in the process of being written to disk (another process)

        NOTE:  We bounce out early at the first inconsistency.  We do not record
        what wasn't correct.

        Args:
            hfr (`HyperFrameRecord`):
            found_frames:
            found_auths:

        Returns:
            bool: This hframe_record corresponds to what we have on disk
        """

        for str_tuple in hfr.pb.frames:
            fr_name = str_tuple.k
            fr_uuid = str_tuple.v
            if fr_uuid in found_frames:
                # Frame was on disk (in found_frames dict)
                valid_frame = DataContext._validate_frame(found_frames[fr_uuid], found_auths)
                if not valid_frame:
                    _logger.warn("HyperFrame {} Frame {} {} not valid".format(hfr.pb.uuid, fr_name, fr_uuid))
                    return False
            else:
                # Frame was not on disk
                _logger.warn("HyperFrame {} Frame {} {} not present on disk".format(hfr.pb.uuid, fr_name, fr_uuid))
                return False
        return True

    @staticmethod
    def _validate_frame(fr, found_auths):
        """
        Then for each frame if a link frame,
            for each link, is file in folder
            for each link, do we have the auth

        Args:
            fr (`FrameRecord`):
            found_auths (dict):

        Returns:
            valid (bool): This frame corresponds to what we have on disk
        """

        if fr.is_link_frame():
            for l in fr.pb.links:
                if l.linkauth_uuid != '':  # empty string is PB default value when not set
                    if l.linkauth_uuid not in found_auths:
                        _logger.warn("Frame name {} with invalid link auth {} in link {}".format(fr.pb.name,
                                                                                                 l.linkauth_uuid,
                                                                                                 l.uuid))
                        return False
                url = hyperframe.LinkBase.find_url(l)
                o = urllib.parse.urlparse(url)
                if o.scheme == 's3':
                    _logger.warn("Disdat FS TODO check on s3 for file {}".format(url))
                elif o.scheme == 'db':
                    pass
                    #_logger.warn("Disdat FS TODO support db tables in link columns")
                elif o.scheme == 'bundle':
                    # this is OK.  file links are bundle urls.
                    pass
                    #_logger.debug("Disdat FS TODO check on bundle links for {}".format(url))
                elif o.scheme == 'file':
                    if not os.path.exists(o.path):
                        _logger.warn("Frame name {} contains file {} not found on disk.".format(fr.pb.name,
                                                                                                os.path))
                        return False
                else:
                    raise Exception("Disdat Context _validate_frame {} found bad scheme: {}".format(fr.pb.name,
                                                                                              o.scheme))
        return True

    def rebuild_db(self, ignore_existing=True):
        """

        For this context, read in all pb's and rebuild tables.
        All state is immutable.
        1.) Read in all HFrame PBs
        2.) Ensure that each HFrame PB is WEAKLY consistent -- file names corresponding to the
        frames are on disk (but we do not check their contents or whether the file links exist).
        3.) A.) If yes, try to insert into db if it doesn't already exist.
            B.) If not consistent and not in db, leave it.  Could be concurrent add.
            C.) If not consistent and in db as 'valid', mark db entry as 'invalid'

        Args:
            ignore_existing (bool): If True, we ignore existing records (do not update). Else UPSERT on existing.

        Returns:
            num errors (int):

        """
        INSERT_BATCH = 1000
        hframes = {}
        frames = {}

        pb_types = [('*_hframe.pb', hyperframe.HyperFrameRecord, hframes),
                    ('*_frame.pb', hyperframe.FrameRecord, frames)]

        # Note: we no longer create the frame and auth tables.
        hyperframe.HyperFrameRecord.create_table(self.local_engine)

        for uuid_dir in os.listdir(self.get_object_dir()):
            for glb, rcd_type, store in pb_types:
                files = glob.glob(os.path.join(os.path.join(self.get_object_dir(), uuid_dir), glb))
                for f in files:
                    if rcd_type == hyperframe.HyperFrameRecord:
                        rcd = hyperframe.r_pb_fs(f, rcd_type)
                        store[rcd.pb.uuid] = rcd
                    elif rcd_type == hyperframe.FrameRecord:
                        base_f = os.path.basename(f)
                        store[DataContext.extract_uuid_from_pb_path(base_f, rcd_type)] = True

        hframe_count = len(hframes.values())
        ten_percent = max(1, int(hframe_count / 10))
        perc = 0
        insert_batch = []
        for i, hfr in enumerate(hframes.values()):
            if i % ten_percent == 0:
                _logger.debug("Disdat DB rebuild: written {} ({} percent) to db".format(i, perc))
                perc += 10
            if DataContext._weak_validate_hframe(hfr, frames):
                hfr_from_db_list = hyperframe.select_hfr_db(self.local_engine, uuid=hfr.pb.uuid)
                if not ignore_existing or len(hfr_from_db_list) == 0:
                    insert_batch.append(hfr)
            else:
                # invalid hyperframe, if present in db as valid, mark invalid
                hfr_from_db_list = hyperframe.select_hfr_db(self.local_engine, uuid=hfr.pb.uuid)
                assert(len(hfr_from_db_list) == 0 or len(hfr_from_db_list) == 1)
                if len(hfr_from_db_list) == 1:
                    hfr_from_db = hfr_from_db_list[0]
                    if hfr_from_db.state == hyperframe.RecordState.valid:
                        # If it is valid, and we know it isn't, mark invalid
                        hyperframe.update_hfr_db(self.local_engine, hyperframe.RecordState.invalid,
                                                 uuid=hfr.pb.uuid)
                    # else, pending, invalid, deleted is all OK with an invalid hyperframe
            if len(insert_batch) >= INSERT_BATCH:
                hyperframe.w_pb_db(insert_batch, self.local_engine)
                insert_batch = []
        if len(insert_batch) > 0:
            hyperframe.w_pb_db(insert_batch, self.local_engine)

    def bundle_count(self):
        """ Determine how many bundles in the current local context
        Returns:
            (int): Count of bundles in this local context
        """
        assert self.local_engine is not None

        return hyperframe.bundle_count(self.local_engine)

    def dbck(self):
        """
        Do a database check.
        For each record in the database:
            If marked invalid, remove pb's on disk, remove record.
            If marked valid, ensure it is still valid on disk.  If not remove

        Returns:
            None

        """
        self.valid = True

    def implicit_hframe_path(self, uuid):
        """
        Given uuid, build path to this object inside of this context.


        pb_types = [('*_hframe.pb', hyperframe.HyperFrameRecord, hframes),
                    ('*_frame.pb', hyperframe.FrameRecord, frames),
                    ('*_auth.pb', hyperframe.LinkAuthBase, auths)]

        Args:
            uuid (str):  UUID of the pb object in question

        Returns:
            path (str):  If no cls, return dir, else return path to file.

        """

        path = os.path.join(self.get_object_dir(), uuid)

        return path

    def util_get_managed_paths(self, uuid):
        """ Given a uuid, use the local and remote contexts
        to return the local directory for this bundle and
        the remote directory for this bundle.

        Note:  We do not validate the directories returned!

        Returns:
            (str, str):  A tuple of local_dir and remote_dir
        """

        assert(self.is_valid())

        local_dir = os.path.join("file:///", self.get_object_dir(), uuid)  # @ReservedAssignment

        if self.remote_ctxt_url is not None:
            remote_dir = os.path.join(self.get_remote_object_dir(), uuid)
        else:
            remote_dir = None

        return local_dir, remote_dir

    def make_managed_path(self, uuid=None):
        """
        Create a managed path for files created within this context.
        When there is a bound remote context, we will return a key
        including the UUID for that path.

        TODO: Do we create the "directory" on s3?  At the moment we just return the path

        Returns:
            local path, uuid, remote_path (tuple:(str,str)): A tuple with the path and uuid

        """
        # TODO: Hateful local import -- fix
        from disdat.fs import DisdatFS

        assert(self.is_valid())

        if uuid is None:
            _provided_uuid = DisdatFS.disdat_uuid()
        else:
            _provided_uuid = uuid

        local_dir, remote_dir = self.util_get_managed_paths(_provided_uuid)

        if os.path.exists(local_dir):
            raise Exception('Caught UUID collision {}'.format(uuid))

        os.makedirs(local_dir)

        return local_dir, _provided_uuid, remote_dir

    def rm_hframe(self, hfr_uuid):
        """
        Given a hfr_uuid, remove the hyperframe from the context.
        This is a destructive operation.  After this the hframe and all of its data
        is gone (unless links point to files outside of a managed path).

        Note pending removal in db
        Remove from stable storage
        Remove from db

        NOTE: Because we store all the files in 'objects/<uuid>/' we don't have to find and enumerate
        the link frames.

        Args:
            hfr_uuid (str):

        Returns:
            success (bool): Whether or not we deleted the bundle

        """
        try:
            hfr = self.get_hframes(uuid=hfr_uuid)
            assert hfr is not None
            assert len(hfr) == 1

            hyperframe.update_hfr_db(self.local_engine, hyperframe.RecordState.deleted, uuid=hfr_uuid)
            shutil.rmtree(self.implicit_hframe_path(hfr_uuid))
            hyperframe.delete_hfr_db(self.local_engine, uuid=hfr_uuid)

            return True
        except (IOError, os.error) as why:
            _logger.error("Removal of hyperframe directory {} failed with error {}.".format(self.implicit_hframe_path(hfr_uuid), why))

            # Clean up db if directory removal failed
            hyperframe.delete_hfr_db(self.local_engine, uuid=hfr_uuid, state=hyperframe.RecordState.deleted)

            return False

    def get_hframes(self, human_name=None, processing_name=None,
                    uuid=None, tags=None, state=None, groupby=False,
                    before=None, after=None, maxbydate=False):
        """
        Find all hframes with the given bundle_name

        Args:
            human_name (str): Given name
            processing_name (str): name of the process that created the hframe
            uuid (str): UUID
            tags (dict):
            state:
            groupby (bool): group by search
            before (datetime.datetime): Return records on or before datetime
            after (datetime.datetime): Return records on or after datetime
            maxbydate (bool): Return the most recent by name

        Returns:
            (list:`disdat.hyperframe.HyperFrameRecord'): list of HyperFrameRecords (or rows if groupby=True) ordered youngest to oldest

        """
        found = hyperframe.select_hfr_db(self.local_engine,
                                         human_name=human_name,
                                         processing_name=processing_name,
                                         uuid=uuid,
                                         tags=tags,
                                         state=state,
                                         orderby=True,
                                         groupby=groupby,
                                         maxbydate=maxbydate,
                                         before=before,
                                         after=after
                                         )

        return found

    def write_hframe_db_only(self, hfr):
        """
        Quick hack to write an HFR pb into the db from DisdatFS

        Args:
            hfr (`disdat.hyperframe.HyperFrameRecord`):

        Returns:

        """
        hyperframe.w_pb_db(hfr, self.local_engine)

    def write_hframe_local(self, hfr):
        """
        Given a HyperFrameRecord we need to record it in our current active context.
        Since we have a DB, it just means putting it in our DB.

        NOTE: writing it into the DB is *not* the same as writing it to disk.  The context
        is meta-information about the bundles.  For example, an hframe contains uuid references
        to frames.  The DB isn't guaranteed to always have the full binary blob of the PB.


        Args:
            hfr (`disdat.hyperframe.HyperFrameRecord`):


        Returns:

        """
        # Write DB HyperFrame
        result = hyperframe.w_pb_db(hfr, self.local_engine)

        # Write FS Frames
        for fr in hfr.get_frames(self):
            hyperframe.w_pb_fs(os.path.join(self.get_object_dir(), hfr.pb.uuid), fr)

        # Write FS HyperFrame
        hyperframe.w_pb_fs(os.path.join(self.get_object_dir(), hfr.pb.uuid), hfr)

        return result

    def write_hframe_remote(self, hfr, dry_run=False):
        """
        Given a HyperFrameRecord, write out pb records to remote

        Args:
            hfr (`disdat.hyperframe.HyperFrameRecord`):
            dry_run (bool): do not write files, just return src, dst pairs.

        Returns:
            list: (src, dst) file path pairs
        """
        local_obj_dir = os.path.join(self.get_object_dir(), hfr.pb.uuid)
        if not os.path.exists(local_obj_dir):
            raise Exception("Write HFrame to remote failed because hfr {} doesn't appear to be in local context".format(
                hfr.pb.uuid))
        to_copy_files = glob.glob(os.path.join(local_obj_dir, '*frame.pb'))
        dst_files = []
        remote_object_dir = os.path.join(self.get_remote_object_dir(), hfr.pb.uuid)
        for f in to_copy_files:
            dst_files.append(os.path.join(remote_object_dir, os.path.basename(f)))
            if not dry_run:
                aws_s3.put_s3_file(f, remote_object_dir)

        return [(src, dst) for src, dst in zip(to_copy_files, dst_files)]


    def atomic_update_hframe(self, hfr):
        """
        Given an HFR that has new meta information, such as tags, update the version on disk atomically,
        then make an update to the data base

        Note: This has only been spec'd to work when we update tags.   If you're making any other changes to the
        original HyperFrameRecord, you will need to review this code.

        TODO: This is not acid wrt to the database.  We need to make a transaction for this update.
        At least try / catch the remove before we update the file.

        Args:
            hfr (`disdat.hyperframe.HyperFrameRecord`):

        Returns:
            result object

        """

        # 1.) Delete DB record
        hyperframe.delete_hfr_db(self.local_engine, uuid=hfr.pb.uuid)

        # 2.) Write FS HyperFrame PB to a sister file and then move to original file.
        hyperframe.w_pb_fs(os.path.join(self.get_object_dir(), hfr.pb.uuid), hfr, atomic=True)

        # 3.) Write DB HyperFrame and tags
        result = hyperframe.w_pb_db(hfr, self.local_engine)

        return result

    def push_hfr_to_remote(self, hfr):
        """

        Args:
            hfr (`disdat.hyperframe.HyperFrameRecord`):

        Returns:

        """
        raise NotImplementedError

    def get_hframe_names(self):
        """
        Return all human names of all hframes in context
        Only get unique human_names.

        NOTE: Only chooses valid frames

        TODO: use a select that projects out human_name

        Returns:
            results (list:(str)): sorted name list
        """

        found = self.get_hframes(human_name='.*', state=hyperframe.RecordState.valid, groupby=True)

        if len(found) == 0:
            return []
        else:

            # if we want to read from disk ...
            # found = self.get_hframes(human_name='.*', state=hyperframe.RecordState.valid, groupby=False)
            #for f in found:
            #    print type(f)
            #    local_uuid_dir = os.path.join(self.get_object_dir(), f.pb.uuid)
            #    local_hfr_path = os.path.join(local_uuid_dir, hyperframe.HyperFrameRecord.make_filename(f.pb.uuid))
            #    local_hfr = hyperframe.r_pb_fs(local_hfr_path, hyperframe.HyperFrameRecord)
            #    print "context read on-disk hfr {} {} creation date {}".format(local_hfr.pb.human_name,
            #                                                                      local_hfr.pb.uuid,
            #                                                                      local_hfr.pb.lineage.creation_date)

            if isinstance(found[0], hyperframe.HyperFrameRecord):
                found.sort(key=lambda hfr: hfr.pb.human_name)
                return [hf.pb.human_name for hf in found]
            else:
                return [row['human_name'] for row in found]

    def get_hframe_processing_names(self):
        """
        Return all processing names of all hframes in context
        TODO: use a select that projects out processing_name

        Returns:
            results (list:(str)): sorted name list
        """
        found = self.get_hframes(state=hyperframe.RecordState.valid)

        return found.sort(key=lambda hfr: hfr.pb.processing_name)

    def convert_scalar2frame(self, hfid, name, scalar):
        """
        Convert a scalar into a frame.  First, place inside an ndarray,
        and then hand off to serieslike2frame()

        Args:
            hfid:
            name:
            scalar:

        Returns:
            ndarray wrapping scalar
        """

        assert (not (isinstance(scalar, list) or isinstance(scalar, tuple) or
                     isinstance(scalar,dict) or isinstance(scalar, np.ndarray)) )
        series_like = np.reshape(np.array(scalar), (1))
        return self.convert_serieslike2frame(hfid, name, series_like)

    def convert_serieslike2frame(self, hfid, name, series_like):
        """
        Convert series-like to a frame.
        If the frame has file paths, we will copy in if the managed path is set.

        Note: This is called from parse_pipe_return_vals().   The user might have
         created managed s3 paths and we do not need to copy the data to this local
         context.

        Args:
            hfid:
            name:
            series_like: a list-like

        Returns:
            (`hyperframe.FrameRecord`)

        """
        # Force everything to be ndarrays.
        try:
            if not isinstance(series_like, np.ndarray):
                series_like = np.array(series_like[0:])
        except TypeError:
            series_like = np.array(series_like)

        # If local files, append 'file://' to each path.
        # If local directory, raise exception
        local_files_series = hyperframe.detect_local_fs_path(series_like)

        if local_files_series is not None:
            series_like = local_files_series

        #print (series_like)

        # Make sure s3 files exist -- copy in does this check
        # series_like = hyperframe.detect_s3_fs_path(series_like)

        if hyperframe.FrameRecord.is_link_series(series_like):
            """ 
            If src is s3 file
              If s3 file not managed
                if have remote: 
                  copy in to remote
                else:
                  copy in to local
              if s3 file managed:
                if have remote:
                  do nothing
                else:
                  error
            if src is local:
              If file not managed
                copy in to local
              if file managed:
                do nothing
            """
            local_managed_path = os.path.join(self.get_object_dir(), hfid)
            remote_object_dir = self.get_remote_object_dir()
            if remote_object_dir is not None:
                remote_managed_path = os.path.join(remote_object_dir, hfid)
            else:
                remote_managed_path = None

            copied_in_series_like = []
            for src in series_like:
                if isinstance(src, S3Target):
                    src = src.path
                elif isinstance(src, luigi.LocalTarget):
                    src = urllib.parse.urljoin('file:', src.path)

                if urllib.parse.urlparse(src).scheme == 's3':
                    if remote_managed_path is not None:
                        copied_in_series_like.append(self.copy_in_files(src,
                                                                        remote_managed_path,
                                                                        localize=False))
                        continue
                copied_in_series_like.append(self.copy_in_files(src,
                                                                urllib.parse.urljoin('file:', local_managed_path),
                                                                localize=False))

            frame = hyperframe.FrameRecord.make_link_frame(hfid, name, copied_in_series_like,
                                                           local_managed_path, remote_managed_path)
        else:
            frame = hyperframe.FrameRecord.from_serieslike(hfid, name, series_like)
        return frame

    def convert_df2frames(self, hfid, df):
        """
        Given a Pandas dataframe, convert this into a set of frames.

        For each 'file' column, move the files, and make the links

        Note: If the csv/tsv was saved with an index, the name will be 'Unnamed: 0'.
        We ignore all Unnamed columns.   Currently frames / columns are re-indexed
        by default from [0,len(frame)-1]

        Args:
            hfid: hyperframe uuid
            df: dataframe of input data

        Returns:
            (list:`hyperframe.FrameRecord`)
        """
        frames = []

        for idx, c in enumerate(df.columns):  # @UnusedVariable
            if 'Unnamed:' in c:
                # ignore columsn without names, like default index columns
                continue
            frames.append(self.convert_serieslike2frame(hfid, c, df[c]))
        return frames

    @staticmethod
    def find_subdir(src, dst):
        """
        Given
        src: <uri:>//something/context/<somecontext>/objects/<some uuid>/sub1/.../sub2/file
        dst: <uri:>//otherthing/context/<somecontext>/objects/<same uuid>

        Extract 'sub1/.../subn/'

        Args:
            src: full path to the source file in a context
            dst: destination managed path directory -- should end in 'objects/<uuid>'

        Returns:
            (str):
        """
        # Strip file name from src, normalize, and split on /
        src_split = os.path.normpath(os.path.dirname(src)).split('/')
        dst_split = os.path.normpath(dst).split('/')
        sub_dir = list()
        found = False
        for i in range(len(src_split)-1,-1,-1):
            if src_split[i] == dst_split[-1]:
                if src_split[i-1] == dst_split[-2] and dst_split[-2] == 'objects':
                    found = True
                    break
            sub_dir.append(src_split[i])
        if found:
            return '/'.join(sub_dir[::-1])
        else:
            return ''

    def copy_in_files(self, src_files, dst_dir, localize=True, dry_run=False):
        """
         Given a set of link URLs, move them to the destination.
         The link URLs will have file:///, s3://, or db:// schemes

         This call works for src: dst pairs of the form:

         local fs : managed local fs dir
         local fs : managed s3 dir
         s3       : managed local fs dir
         s3       : managed s3 dir

         Assumes that src_files and dst_dir begin with scheme 'file'

         Args:
            src_files (:list:str):  A single file path or a list of paths
            dst_dir (str): Local or Remote managed dirs
            localize (bool): If True, then copy src s3 -> dst file:/// (Default).  Else do not copy.
            dry_run (bool): Return the destination files, but do not perform the copy

         Returns:
            file_set: set of new paths where files were copied.  Either one file or a list of files
        """
        file_set = []
        return_one_file = False
        dst_scheme = urllib.parse.urlparse(dst_dir).scheme
        assert dst_scheme == 'file' or dst_scheme == 's3', \
            "copy_in_files: dst_dir with unrecognized scheme {}".format(dst_scheme)

        if not isinstance(src_files, list) and not isinstance(src_files, tuple):
            return_one_file = True
            src_files = [src_files]

        for src_path in src_files:
            if isinstance(src_path, luigi.LocalTarget) or isinstance(src_path, S3Target):
                src_path = src_path.path

            src_urlparse = urllib.parse.urlparse(src_path)

            if src_urlparse.scheme == 's3' and not aws_s3.s3_path_exists(src_path):
                raise common.BadLinkError("copy_in_files: s3 path {} does not exit.".format(src_path))
            elif src_urlparse.scheme == 'file' and os.path.isdir(src_path):
                raise common.BadLinkError("copy_in_files: local path {} is a directory.".format(src_path))

            # Do not copy src file in to local if:
            # 1. Managed Local or S3 File  -- src path starts with dst path (dst is always a bundle directory)
            if src_path.startswith(dst_dir):
                # print("----> copy_in_files: ZERO COPY SRC {} to DST_DIR {}".format(src_path, dst_dir))
                file_set.append(src_path)
                continue

            # 2. Non Managed S3 File (Remote and push should be set)
            if self.remote_ctxt_url:
                """ If there is a remote and we see a source S3 path
                If it is an external S3 path, then copy it to the dst_dir (local or s3)
                If it is a managed s3 path and localizing, then copy it to the dst_dir (should be local)
                If it is a managed s3 path and not localizing, do nothing. 
                """
                uuid = os.path.basename(dst_dir.rstrip('/'))
                managed_path_s3 = os.path.join(self.get_remote_object_dir(), uuid)
                if src_path.startswith(managed_path_s3) and not localize:
                    file_set.append(src_path)
                    continue

            # Add the destination as the final location, adding sub directories if present
            sub_dir = DataContext.find_subdir(src_path, dst_dir)
            dst_file = os.path.join(dst_dir, sub_dir, os.path.basename(src_path))
            # print("----> copy_in_files: COPY SRC {} to DST FILE {}".format(src_path, dst_file))
            file_set.append(dst_file)  # Record it with 'file://'
            dst_file_parse = urllib.parse.urlsplit(dst_file)
            # But strip 'file://' so that the copies to/from work.   S3 paths work all the time with s3://
            if dst_file_parse.scheme == 'file':
                dst_file = dst_file_parse.path

            if not dry_run:
                try:
                    if src_urlparse.scheme == 's3':
                        # s3 to s3
                        if dst_scheme == 's3':
                            aws_s3.cp_s3_file(src_path, os.path.dirname(dst_file))
                        elif dst_scheme == 'file':
                            aws_s3.get_s3_file(src_path, dst_file)
                        else:
                            raise common.BadLinkError("copy_in_files: copy s3 to unsupported scheme {}".format(dst_scheme))

                    elif src_urlparse.scheme == 'db':  # left for back compat for now
                        _logger.debug("Skipping a db file on bundle add")

                    elif src_urlparse.scheme == 'file':
                        if dst_scheme == 's3':
                            # local to s3
                            aws_s3.put_s3_file(src_urlparse.path, os.path.dirname(dst_file))
                        elif dst_scheme == 'file':
                            # local to local
                            shutil.copy(src_urlparse.path, os.path.dirname(dst_file))
                        else:
                            raise common.BadLinkError("copy_in_files: copy local file to unsupported scheme {}".format(dst_scheme))
                    else:
                        raise common.BadLinkError("copy-in-file found bad scheme: {} from {}".format(src_urlparse.scheme,
                                                                                                     src_urlparse))
                except (IOError, os.error) as why:
                    _logger.error("Disdat add error: {} {} {}".format(src_path, dst_dir, str(why)))

        if return_one_file:
            return file_set[0]
        else:
            return file_set

    def actualize_link_urls(self, fr, strip_file_scheme=False):
        """
        Given an s3, local file link, or db frame, return paths to the data.

        Bundles are independent of their location (the current context in which they are stored).
        When a bundle is "read", we transform the link URLs to show local files in the local context and
         db URLs to be a database table using the current local context.

        Args:
            fr (`hyperframe.FrameRecord`):  A single link frame
            strip_file_scheme (bool): Return the files without 'file://' if local FS

        Returns:
            file_set: set of new paths where files exist

        """
        file_set = []

        if not (fr.is_local_fs_link_frame() or fr.is_s3_link_frame() or fr.is_db_link_frame()):
            _logger.error("actualize_link_urls called on non-link frame.")
            raise ValueError("actualize_link_urls called on non-link frame.")

        urls = fr.get_link_urls()

        if fr.is_db_link_frame():
            """ No-Op with db links """
            return urls
        else:
            """ Must be s3 or local file links.  All the files in the link must be present """
            assert urllib.parse.urlparse(urls[0]).scheme == common.BUNDLE_URI_SCHEME.replace('://', '')
            local_dir = self.get_object_dir()
            local_file_set = [os.path.join(local_dir, fr.hframe_uuid, f.replace(common.BUNDLE_URI_SCHEME, '')) for f in
                              urls]

        # Check to see which files are present and which must stay remote
        # This can now happen with individual link localize and delocalize.
        # The only state that tells us if the whole bundle has been pushed is to check if the
        # hyperframe <uuid>_hframe.pb exists on the remote.
        for lf, rurl in zip(local_file_set, urls):
            if os.path.isfile(lf):
                if not strip_file_scheme:
                    lf = urllib.parse.urljoin('file:', lf)
                file_set.append(lf)
            else:
                remote_dir = self.get_remote_object_dir()
                if remote_dir is not None:
                    file_set.append(os.path.join(remote_dir, fr.hframe_uuid, rurl.replace(common.BUNDLE_URI_SCHEME,'')))
                else:
                    _logger.info("actualize_link_urls: Files are not local, and no remote context bound.")
                    raise Exception("actualize_link_urls: Files are not local, and no remote context bound.")

        return file_set

    def convert_hfr2df(self, hfr):
        """
        Given a HyperFrameRecord, convert into a dataframe.  If no data, return empty dataframe

        Note: This process may a.) reduce data fidelity (pandas series are 1d!) and b.) may fail

        Note: This is an instance method.   A HyperFrameRecord may not have all its frames cached.
        To find its frames, we need to know the context we are in.

        Args:
            hfid: hyperframe uuid
            hfr: hyperframe to convert

        Returns:
            (`pandas.DataFrame`)
        """

        frames = hfr.get_frames(self)
        columns = []
        for fr in frames:
            if fr.is_local_fs_link_frame() or fr.is_s3_link_frame() or fr.is_db_link_frame():
                src_paths = self.actualize_link_urls(fr, strip_file_scheme=True)
                columns.append(pd.Series(data=src_paths, name=fr.pb.name))
            else:
                columns.append(fr.to_series())

        if len(columns) == 0:
            return pd.DataFrame()
        else:
            return pd.concat(columns, axis=1)

    def convert_hfr2scalar(self, hfr):
        """
        Convert a HyperFrameRecord into a single scalar value

        Args:
            hfr:

        Returns:
            (scalar)

        """
        frames = hfr.get_frames(self)
        assert len(frames) == 1
        fr = frames[0]

        if fr.is_local_fs_link_frame() or fr.is_s3_link_frame() or fr.is_db_link_frame():
            src_paths = self.actualize_link_urls(fr, strip_file_scheme=True)
            nda = np.array(src_paths)
        else:
            nda = fr.to_ndarray()

        return nda.item()

    def convert_hfr2json(self, hfr):
        """
        Convert a HyperFrameRecord into a single json output

        Args:
            hfr:

        Returns:
            (scalar)

        """
        frames = hfr.get_frames(self)
        assert len(frames) == 1
        fr = frames[0]

        assert not (fr.is_local_fs_link_frame() or fr.is_s3_link_frame() or fr.is_db_link_frame()), \
            "hfr2json, failed since this is a link frame. "

        nda = fr.to_ndarray()

        return json.loads(nda.item())

    def convert_hfr2ndarray(self, hfr):
        """
        Convert a HyperFrameRecord into an ndarray.
        Args:
            hfr:

        Returns:

        """
        frames = hfr.get_frames(self)
        assert len(frames) == 1
        fr = frames[0]

        if fr.is_local_fs_link_frame() or fr.is_s3_link_frame() or fr.is_db_link_frame():
            src_paths = self.actualize_link_urls(fr, strip_file_scheme=True)
            return np.array(src_paths)
        else:
            return fr.to_ndarray()

    def convert_hfr2row(self, hfr):
        """
        Convert a HyperFrameRecord into a tuple (row).  The user can input either a tuple (x,y,z), in which case we
        fabricate column names.  Or the user may pass a dictionary.   If there are multiple values to unpack then we
        will store them into Python lists.  Note, if the names are generic, we return the tuple form.

        Args:
            hfr:

        Returns:

        """
        frames = hfr.get_frames(self)
        row = []
        for fr in frames:
            if fr.is_local_fs_link_frame() or fr.is_s3_link_frame() or fr.is_db_link_frame():
                src_paths = self.actualize_link_urls(fr, strip_file_scheme=True)
                if len(src_paths) == 1:
                    row.append((fr.pb.name, src_paths[0]))
                else:
                    row.append((fr.pb.name, np.array(src_paths)))
            else:
                if fr.pb.shape[0] == 1:
                    row.append((fr.pb.name, fr.to_ndarray().item()))
                else:
                    row.append((fr.pb.name, fr.to_ndarray()))
        if common.DEFAULT_FRAME_NAME in frames[0].pb.name:
            # Drop the names and return a list of unkeyed values.
            tuple_of_lists = tuple([r[1] for r in row])
            if len(tuple_of_lists) == 1:
                return tuple(tuple_of_lists[0])
            return tuple_of_lists
        else:
            d = { t[0]: (t[1] if isinstance(t[1], (tuple, list, np.ndarray)) else [t[1]]) for t in row }
            return d

    def present_hfr(self, hfr):
        """
        If HyperFrame is presentable, return presentable data type.

        Args:
            hfr:

        Returns:
            one of DataFrame, ndarray, scalar, tuple, or just the hyperframe

        """
        assert hfr.pb.presentation != hyperframe_pb2.DEFAULT

        if hfr.pb.presentation == hyperframe_pb2.HF:
            frames = hfr.get_frames(self)
            if len(frames) == 0:
                # TODO: Remove on major release or adding true HF presentations
                _logger.warning("DEPRECATION: Presentation HF was a hack for NoneType returns."
                                " You should delete this bundle: UUID {}.".format(hfr.pb.uuid))
                print(hfr.pb)
                return None
            assert len(frames) == 1
            assert frames[0].pb.type == hyperframe_pb2.HFRAME
            return frames[0].get_hframes()

        elif hfr.pb.presentation == hyperframe_pb2.DF:
            return self.convert_hfr2df(hfr)

        elif hfr.pb.presentation == hyperframe_pb2.SCALAR:
            return self.convert_hfr2scalar(hfr)

        elif hfr.pb.presentation == hyperframe_pb2.TENSOR:
            return self.convert_hfr2ndarray(hfr)

        elif hfr.pb.presentation == hyperframe_pb2.ROW:
            return self.convert_hfr2row(hfr)

        elif hfr.pb.presentation == hyperframe_pb2.JSON:
            return self.convert_hfr2json(hfr)

        else:
            raise Exception("present_hfr with HFR using unknown presentation enumeration {}".format(hfr.pb.presentation))

    def is_valid(self):
        return self.valid
