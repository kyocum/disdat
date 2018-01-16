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

import disdat.constants as constants
import disdat.hyperframe_pb2 as hyperframe_pb2
import disdat.hyperframe as hyperframe
import disdat.common as common
import disdat.utility.aws_s3 as aws_s3
from disdat.common import DisdatConfig

from urlparse import urlparse
import logging
import os
import json
import glob
import shutil
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import luigi
from urlparse import urlparse, urljoin

_logger = logging.getLogger(__name__)

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

    def delete_branch(self, force=False):
        """
        Any checks on the local context before we delete all the objects in here?

        Returns:

        """
        if self.unpushed_data() and not force:
            print("Disdat found un-pushed data in context {}, use -f to delete".format(self.local_ctxt))
            return

        self.local_engine.dispose()

    def bind_remote_ctxt(self, context, s3_url, pfs, force=False):
        """
        A local branch can be bound to a remote shared FS where you can push/pull hyperframes.
        If the remote_context directory does not exist, a push will create it.  Note that pull is lazy.
        Thus we might have local bundles that refer to files in s3 in another context.   The user may
        pull these as normal -- the files will be pulled from the prior remote context.

        TODO: If the user has non-localized bundles after forcing a rebind, then Disdat will localize
        those links to the new location.  And then won't find them.  Automating this means either keeping
        track of the prior bound locations or asking the user when the push fails for the prior s3 path.

        Args:
            context: remote context name
            s3_url:  remote context url -- this points to the root of all disdat data -- does not include context dir
            pfs: DisdatFS
        Returns:
            None

        """
        assert (urlparse(s3_url).scheme == 's3')

        if self.remote_ctxt_url is not None and self.remote_ctxt == context and \
                        os.path.normpath(os.path.dirname(self.remote_ctxt_url)) == os.path.normpath(s3_url):
            print "Context already bound to remote at {}".format(s3_url)
            return

        if self.remote_ctxt != context:
            if not force:
                _logger.error("Unable to bind because branch {} ".format(self.local_ctxt) +
                              "is not on remote context {} (it is on remote context {}). Use '--force'".format(context,
                                                                                                               self.remote_ctxt))
                return
            else:
                self.remote_ctxt = context

        if not aws_s3.s3_path_exists(s3_url):
            _logger.error("Unable to bind context {} because URL {} does not exist.".format(context, s3_url))
            return

        if self.remote_ctxt_url is None:
            _logger.debug("Binding local branch {} context {} to URL {}".format(self.local_ctxt, self.remote_ctxt, s3_url))
        else:
            if not force:
                print "You are re-binding this branch to a different remote context.  You might have un-localized"
                print "files in pulled bundles.  First, issue 'dsdt pull --localize' to make data local. "
                print " Then run: `dsdt remote --force {}' to force re-binding the remote.".format(s3_url)
                return

            _logger.debug("Un-binding local branch {} context {} current URL {}".format(self.local_ctxt,
                                                                                        self.remote_ctxt,
                                                                                        self.remote_ctxt_url))
            _logger.debug("Re-binding to URL {}".format(s3_url))

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
    def load():
        """
        Load the data contexts described at meta_dir.  Each of these is a "remote."
        Args:
            local_ctxt_dir: Directory of contexts, e.g., ~/.disdat

        Returns:
            (dict) of 'name':context pairs.

        """
        ctxt_dir = DisdatConfig.instance().get_context_dir()
        if ctxt_dir is None:
            raise Exception("Unable to load context without a metadata directory argument")

        contexts = {}

        files = glob.glob(os.path.join(ctxt_dir, '*'))

        for ctxt in files:
            _logger.debug("Loading context {}...".format(ctxt))
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

    def get_remote_object_dir(self):
        """
        Where objects live on remote.

        Returns:
            (str):
        """
        if self.remote_ctxt_url is None:
            return None
        return os.path.join(self.remote_ctxt_url, self.remote_ctxt, constants._MANAGED_OBJECTS)

    def get_repo_name(self):
        return self.remote_ctxt

    def get_local_name(self):
        return self.local_ctxt

    def init_remote_db(self):
        """
        NOTE: Not sure what this was supposed to do...

        Returns:

        """
        pass

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
            meta_dir: Directory where we expect the current context to be cached.

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
        what wasn't correct.   TODO's.

        Args:
            hfr (`HyperFrameRecord`):
            found_frames:
            found_auths:

        Returns:
            valid (bool): This hframe_record corresponds to what we have on disk

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
                o = urlparse(url)
                if o.scheme == 's3':
                    _logger.warn("Disdat FS TODO check on s3 for file {}".format(url))
                elif o.scheme == 'vertica':
                    _logger.warn("Disdat FS TODO support vertica tables in link columns")
                elif o.scheme == 'bundle':
                    _logger.warn("Disdat FS TODO check on bundle links")
                elif o.scheme == 'file':
                    if not os.path.exists(o.path):
                        _logger.warn("Frame name {} contains file {} not found on disk.".format(fr.pb.name,
                                                                                                os.path))
                        return False
                else:
                    raise Exception("Disdat Context _validate_frame {} found bad scheme: {}".format(fr.pb.name,
                                                                                              o.scheme))
        return True

    def rebuild_db(self):
        """

        For this context, read in all pb's and rebuild tables.
        All state is immutable.
        1.) Read in all HFrame PBs
        2.) Ensure that each HFrame PB is consistent -- it was completely written
        to disk.
        3.) A.) If yes, try to insert into db if it doesn't already exist.
            B.) If not consistent and not in db, leave it.  Could be concurrent add.
            C.) If not consistent and in db as 'valid', mark db entry as 'invalid'

        dbck does the opposite process.  It will read the DB and see

        Returns:
            num errors (int):

        """
        hframes = {}
        frames = {}
        auths = {}

        pb_types = [('*_hframe.pb', hyperframe.HyperFrameRecord, hframes),
                    ('*_frame.pb', hyperframe.FrameRecord, frames),
                    ('*_auth.pb', hyperframe.LinkAuthBase, auths)]

        # Make all the tables first.
        for glb, rcd_type, store in pb_types:
            rcd_type.create_table(self.local_engine)

        for uuid_dir in os.listdir(self.get_object_dir()):
            for glb, rcd_type, store in pb_types:
                files = glob.glob(os.path.join(os.path.join(self.get_object_dir(), uuid_dir), glb))
                for f in files:
                    # hyperframes, frames, and links all have uuid fields
                    rcd = hyperframe.r_pb_fs(f, rcd_type)
                    store[rcd.pb.uuid] = rcd

        for hfr in hframes.itervalues():
            if DataContext._validate_hframe(hfr, frames, auths):
                # looks like a good hyperframe
                # print "Writing out HFR {} {}".format(hfr.pb.human_name, hfr.pb.uuid)
                hyperframe.w_pb_db(hfr, self.local_engine)
                for str_tuple in hfr.pb.frames:
                    fr_uuid = str_tuple.v
                    # The frame pb doesn't store the hfr_uuid, but the db
                    # does.  Since we are reading from disk, we need to
                    # set it back into the FrameRecord.
                    frames[fr_uuid].hframe_uuid = hfr.pb.uuid
                    hyperframe.w_pb_db(frames[fr_uuid], self.local_engine)
            else:
                # invalid hyperframe, if present in db as valid, mark invalid
                # Try to read it in
                hfr_from_db_list = hyperframe.select_hfr_db(self.local_engine, uuid=hfr.pb.uuid)
                assert(len(hfr_from_db_list) == 0 or len(hfr_from_db_list) == 1)
                if len(hfr_from_db_list) == 1:
                    hfr_from_db = hfr_from_db_list[0]
                    if hfr_from_db.state == hyperframe.RecordState.valid:
                        # If it is valid, and we know it isn't, mark invalid
                        hyperframe.update_hfr_db(self.local_engine, hyperframe.RecordState.invalid,
                                                 uuid=hfr.pb.uuid)
                    # else, pending, invalid, deleted is all OK with an invalid hyperframe

        #print "hframes {}".format(hframes)
        #print "frames {}".format(frames)
        #print "auths {}".format(auths)

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

        dir = os.path.join("file:///", self.get_object_dir(), _provided_uuid)  # @ReservedAssignment
        if os.path.exists(dir):
            raise Exception('Caught UUID collision {}'.format(uuid))
        os.makedirs(dir)

        if self.remote_ctxt_url is not None:
            remote_dir = os.path.join(self.get_remote_object_dir(), _provided_uuid)
        else:
            remote_dir = None

        return dir, _provided_uuid, remote_dir

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
            success (bool): Whether or not we

        """

        hyperframe.update_hfr_db(self.local_engine, hyperframe.RecordState.deleted, uuid=hfr_uuid)

        try:
            shutil.rmtree(self.implicit_hframe_path(hfr_uuid))
        except (IOError, os.error) as why:
            _logger.error("Removal of hyperframe directory {} failed with error {}. Continuing removal...".format(self.implicit_hframe_path(hfr_uuid), why))

        hyperframe.delete_hfr_db(self.local_engine, uuid=hfr_uuid)

    def get_hframes(self, human_name=None, processing_name=None, uuid=None, tags=None, state=None, groupby=False):
        """
        Find all hframes with the given bundle_name

        NOTE: Potentially expensive if the PB bytes are also in the db and no filtering.
        TODO: Return list of records without PB.

        Args:
            human_name (str): Given name
            processing_name (str): name of the process that created the hframe
            uuid (str): UUID
            tags (dict):
            state:
            groupby (bool): group by search

        Returns:
            results (list): list of HyperFrameRecords (or rows if groupby=True) ordered youngest to oldest

        """
        found = hyperframe.select_hfr_db(self.local_engine,
                                         human_name=human_name,
                                         processing_name=processing_name,
                                         uuid=uuid,
                                         tags=tags,
                                         state=state,
                                         orderby=True,
                                         groupby=groupby)

        return found

    def write_hframe_db_only(self, hfr):
        """
        Quick hack to write an HFR pb into the db from DisdatFS

        Args:
            hfr:

        Returns:

        """
        hyperframe.w_pb_db(hfr, self.local_engine)

        # Write DB Frames
        for fr in hfr.get_frames(self):
            hyperframe.w_pb_db(fr, self.local_engine)

    def _write_hframe_local(self, hfr):
        """

        Args:
            hfr:

        Returns:

        """
        # Write DB HyperFrame
        result = hyperframe.w_pb_db(hfr, self.local_engine)

        # Write FS Frames
        for fr in hfr.get_frames(self):
            hyperframe.w_pb_fs(os.path.join(self.get_object_dir(), hfr.pb.uuid), fr)

        # Write FS HyperFrame
        hyperframe.w_pb_fs(os.path.join(self.get_object_dir(), hfr.pb.uuid), hfr)

        # Write DB Frames
        for fr in hfr.get_frames(self):
            hyperframe.w_pb_db(fr, self.local_engine)

        self.prune_uncommitted_history(hfr.pb.human_name)

        return result

    def _write_hframe_remote(self, hfr):
        """

        Args:
            hfr:

        Returns:

        """
        local_obj_dir = os.path.join(self.get_object_dir(), hfr.pb.uuid)
        if not os.path.exists(local_obj_dir):
            raise Exception("Write HFrame to remote failed because hfr {} doesn't appear to be in local context".format(
                hfr.pb.uuid))
        to_copy_files = glob.glob(os.path.join(local_obj_dir, '*.pb'))
        for f in to_copy_files:
            aws_s3.put_s3_file(f, os.path.join(self.get_remote_object_dir(), hfr.pb.uuid))

        return None

    def atomic_update_hframe(self, hfr):
        """
        Given an HFR that has new meta information, such as tags, update the version on disk atomically,
        then make an update to the data base

        Note: This has only been spec'd to work when we update tags.   If you're making any other changes to the
        original HyperFrameRecord, you will need to review this code.

        TODO: This is not acid wrt to the database.  We need to make a transaction for this update.
        At least try / catch the remove before we update the file.

        Args:
            hfr:

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

    def write_hframe(self, hfr, to_remote=False):
        """
        Given a HyperFrameRecord we need to record it in our current active context.
        Since we have a DB, it just means putting it in our DB.

        NOTE: writing it into the DB is *not* the same as writing it to disk.  The context
        is meta-information about the bundles.  For example, an hframe contains uuid references
        to frames.  The DB isn't guaranteed to always have the full binary blob of the PB.

        Args:
            hfr (`hyperframe.HyperFrameRecord`);
            to_remote (bool): Push frame to remote -- Default False

        Returns:
            result : result of insert
        """

        if to_remote:
            return self._write_hframe_remote(hfr)
        else:
            return self._write_hframe_local(hfr)

    def prune_uncommitted_history(self, human_name):
        """
        As we create new data bundles, we prune the local history of the bundles that
            a.) Have the same human name
            b.) That do not have the committed flag attached to them.
            c.) That are outside of the len_uncommitted_history

        NOTE: Called *after* we have added the newest hframe

        TODO: Unify with DisdatFS.rm !

        Args:
            human_name (str): name of the bundle to prune

        Returns:

        """

        hfrs = self.get_hframes(human_name=human_name)
        removed_history = 0

        if len(hfrs) == 0:
            return

        for hfr in hfrs[1:]:

            if 'committed' in hfr.tag_dict:
                assert hfr.tag_dict['committed'] == 'True'
                continue

            if removed_history < self.len_uncommitted_history:
                removed_history += 1
                continue

            self.rm_hframe(hfr.pb.uuid)

    def push_hfr_to_remote(self, hfr):
        """

        Args:
            hfr:

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

    @staticmethod
    def convert_scalar2frame(hfid, name, scalar, managed_path):
        """
        Convert a scalar into a frame.  First, place inside an ndarray,
        and then hand off to serieslike2frame()

        Args:
            hfid:
            name:
            scalar:
            managed_path:

        Returns:
            ndarray wrapping scalar
        """

        assert (not (isinstance(scalar, list) or isinstance(scalar, tuple) or
                     isinstance(scalar,dict) or isinstance(scalar, np.ndarray)) )
        series_like = np.reshape(np.array(scalar), (1))
        return DataContext.convert_serieslike2frame(hfid, name, series_like, managed_path)

    @staticmethod
    def convert_serieslike2frame(hfid, name, series_like, managed_path):
        """
        Convert series-like to a frame.
        If the frame has file paths, we will copy in if the managed path is set.

        Args:
            hfid:
            name:
            series_like: a list-like
            managed_path: for copy_in

        Returns:
            (`hyperframe.FrameRecord`)

        """
        # Force everything to be ndarrays.
        try:
            if not isinstance(series_like, np.ndarray):
                series_like = np.array(series_like[0:])
        except TypeError:
            series_like = np.array(series_like)

        if hyperframe.FrameRecord.is_link_series(series_like):
            assert managed_path is not None
            series_like = [DataContext.copy_in_files(x, managed_path) for x in series_like]
            frame = hyperframe.FrameRecord.make_link_frame(hfid, name, series_like)
        else:
            frame = hyperframe.FrameRecord.from_serieslike(hfid, name, series_like)
        return frame

    @staticmethod
    def convert_df2frames(hfid, df, managed_path):
        """
        Given a Pandas dataframe, convert this into a set of frames.

        For each 'file' column, move the files, and make the links

        Note: If the csv/tsv was saved with an index, the name will be 'Unnamed: 0'.
        We ignore all Unnamed columns.   Currently frames / columns are re-indexed
        by default from [0,len(frame)-1]

        Args:
            hfid: hyperframe uuid
            df: dataframe of input data
            managed_path: Optional path when the df contains file pointers.

        Returns:
            (list:`hyperframe.FrameRecord`)
        """
        frames = []

        for idx, c in enumerate(df.columns):  # @UnusedVariable
            if 'Unnamed:' in c:
                # ignore columsn without names, like default index columns
                continue
            frames.append(DataContext.convert_serieslike2frame(hfid, c, df[c], managed_path))
        return frames

    @staticmethod
    def copy_in_files(src_files, dst_dir):
        """
        Given a set of link URLs, move them to the destination.

        The link URLs will have file:///, s3://, or vertica:// scheme's

        Do not call this to copy a set of links within an existing bundle.
        # move files from one managed path to another
        hfr = <get some hyperframe>
        new_managed_path = "s3://<some keys>"
        frs = [ fr for fr hfr.get_frames() if fr.is_link_frame() ]
        fr = frs[0] # just look at one
        bundle_paths = fr.get_link_urls()
        src_paths   = convert_bundle_url_to_path(bundle_paths)
        new_paths = self.copy_in_files(src_paths, managed_path)
        fr = hyperframe.FrameRecord.make_link_frame(new_hfr_uuid, fr.pb.name, new_paths)

        Note: currently works for
        local fs : local fs dir
        local fs : s3 dir
        s3       : s3 dir
        s3       : local fs dir

        Args:
            src_files (:list:str):  A single file path or a list of paths
            dst_dir (str):

        Returns:
            file_set: set of new paths where files were copies.  either one file or a list of files

        """
        file_set = []
        return_one_file = False

        if isinstance(src_files, basestring) or isinstance(src_files, luigi.LocalTarget):
            return_one_file = True
            src_files = [src_files]

        dst_scheme = urlparse(dst_dir).scheme

        for src_path in src_files:
            try:
                # If this is a luigi LocalTarget and it's in a managed path
                # space, convert the target to a path name but do not
                # actually copy.
                if src_path.path.startswith(dst_dir):
                    file_set.append(urljoin('file:', src_path.path))
                    continue
                else:
                    src_path = src_path.path
            except AttributeError:
                pass

            # TODO: Do something with Vertica links.
            if urlparse(src_path).scheme == 'vertica':
                file_set.append(src_path)
                continue

            dst_file = os.path.join(dst_dir, os.path.basename(src_path))

            if dst_scheme != 's3' and dst_scheme != 'vertica':
                file_set.append(urljoin('file:', dst_file))
            else:
                file_set.append(dst_file)

            if file_set[-1] == src_path:
                # This can happen if you re-push something already pushed that's not localized
                _logger.debug("DataContext: copy_in_files found src {} == dst {}".format(src_path, file_set[-1]))
                # but it can also happen if you re-bind and push.  So check that file is present!
                if urlparse(src_path).scheme == 's3' and not aws_s3.s3_path_exists(src_path):
                    print ("DataContext: copy_in_files found s3 link {} not present!".format(src_path))
                    print ("It is likely that this bundle existed on another remote branch and ")
                    print ("was not localized before changing remotes.")
                    raise Exception("copy_in_files: bad localized bundle push.")
                continue

            try:
                if not os.path.isdir(src_path):
                    o = urlparse(src_path)

                    if o.scheme == 's3':
                        # s3 to s3
                        if dst_scheme == 's3':
                            aws_s3.cp_s3_file(src_path, dst_dir)
                        elif dst_scheme != 'vertica':  # assume 'file'
                            aws_s3.get_s3_file(src_path, dst_file)
                        else:
                            raise Exception("copy_in_files: copy s3 to unsupported scheme {}".format(dst_scheme))

                    elif o.scheme == 'vertica':
                        _logger.debug("Skipping an vertica db file on bundle add")

                    elif o.scheme == 'file':
                        if dst_scheme == 's3':
                            # local to s3
                            aws_s3.put_s3_file(o.path, dst_dir)
                        elif dst_scheme != 'vertica':  # assume 'file'
                            # local to local
                            shutil.copy(o.path, dst_dir)
                        else:
                            raise Exception("copy_in_files: copy local file to unsupported scheme {}".format(dst_scheme))

                    else:
                        raise Exception("DataContext copy-in-file found bad scheme: {}".format(o.scheme))
                else:
                    _logger.info("DataContext copy-in-file: Not adding files in directory {}".format(src_path))
            except (IOError, os.error) as why:
                _logger.error("Disdat add error: {} {} {}".format(src_path, dst_dir, str(why)))

        if return_one_file:
            return file_set[0]
        else:
            return file_set

    def actualize_link_urls(self, fr):
        """
        Given an s3 or file link frame, return actual file paths to the data.

        Args:
            fr (`hyperframe.FrameRecord`):  A single link frame

        Returns:
            file_set: set of new paths where files exist

        """
        file_set = []

        if not (fr.is_local_fs_link_frame() or fr.is_s3_link_frame()):
            _logger.error("actualize_link_urls called on non-link frame.")
            raise ValueError("actualize_link_urls called on non-link frame.")

        urls = fr.get_link_urls()

        assert urlparse(urls[0]).scheme == common.BUNDLE_URI_SCHEME.replace('://', '')

        # if files are local, give local fs files.
        # note, all the files in the link must be present

        local_dir = self.get_object_dir()

        local_file_set = [os.path.join(local_dir, fr.hframe_uuid, f.replace(common.BUNDLE_URI_SCHEME,'')) for f in urls]

        if all(os.path.isfile(lf) for lf in local_file_set):
            file_set = [ "file://{}".format(lf) for lf in local_file_set ]
        else:
            # Note that remote_dir already includes the URL scheme
            remote_dir = self.get_remote_object_dir()
            if remote_dir is not None:
                file_set = [ "{}".format(os.path.join(remote_dir, fr.hframe_uuid, f.replace(common.BUNDLE_URI_SCHEME,''))) for f in urls]
            else:
                _logger.info("actualize_link_urls: Files are not local, and no remote context bound.")
                raise Exception("actualize_link_urls: Files are not local, and no remote context bound.")

        return file_set

    def convert_hfr2df(self, hfr):
        """
        Given a HyperFrameRecord, convert into a dataframe.

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
            if fr.is_local_fs_link_frame() or fr.is_s3_link_frame():
                src_paths = self.actualize_link_urls(fr)
                columns.append(pd.Series(data=src_paths, name=fr.pb.name))
            else:
                columns.append(fr.to_series())
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

        if fr.is_local_fs_link_frame() or fr.is_s3_link_frame():
            src_paths = self.actualize_link_urls(fr)
            nda = np.array(src_paths)
        else:
            nda = fr.to_ndarray()

        return nda.item()

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

        if fr.is_local_fs_link_frame() or fr.is_s3_link_frame():
            src_paths = self.actualize_link_urls(fr)
            return np.array(src_paths)
        else:
            return fr.to_ndarray()

    def convert_hfr2row(self, hfr):
        """
        Convert a HyperFrameRecord into a tuple (row).

        Args:
            hfr:

        Returns:

        """
        frames = hfr.get_frames(self)
        row = []
        for fr in frames:
            if fr.is_local_fs_link_frame() or fr.is_s3_link_frame():
                src_paths = self.actualize_link_urls(fr)
                assert len(src_paths) == 1
                row.append((fr.pb.name, src_paths[0]))
            else:
                row.append((fr.pb.name, fr.to_ndarray())) #fr.to_ndarray().item()))
        if common.DEFAULT_FRAME_NAME in frames[0].pb.name:
            # Drop the names and return a list of unkeyed values.
            return tuple([r[1] for r in row])
        else:
            return dict(row)

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

        else:
            raise Exception("present_hfr with HFR using unknown presentation enumeration {}".format(hfr.pb.presentation))

    def is_valid(self):
        return self.valid
