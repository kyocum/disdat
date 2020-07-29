#
# Copyright 2015, 2016, ... Human Longevity, Inc.
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
A Disdat API for creating, publishing, and finding bundles.

These calls are not thread safe.  If they operate on a context, they
require the user to specify the context.   This is unlike the CLI that maintains state on disk
that keeps track of your current context between calls.   The API won't change the CLI's context and vice versa.

Author: Kenneth Yocum
"""
from __future__ import print_function

import os
import json
import shutil
import getpass
import warnings
import errno
import hashlib
import collections

import disdat.apply   # <--- imports api, which imports apply, etc.
import disdat.run
import disdat.fs
import disdat.common as common
from disdat.pipe_base import PipeBase
from disdat.data_context import DataContext

from disdat.hyperframe import HyperFrameRecord, LineageRecord
from disdat.run import Backend, run_entry
from disdat.dockerize import dockerize_entry
from disdat import logger as _logger

PROC_ID_TRUNCATE_HASH = 10  # 10 ls hex digits


def _get_fs():
    """ Initializing FS, which needs a config.
    These are both singletons.

    TODO: Do we need the config instance here?  Most calls in fs / dockerize
    TODO: explicitly ask for an instance anyhow.

    Returns:
        `fs.DisdatFS`

    """
    disdat.fs.DisdatConfig.instance()

    return disdat.fs.DisdatFS()


def set_aws_profile(aws_profile):
    os.environ['AWS_PROFILE'] = aws_profile


class Bundle(HyperFrameRecord):

    def __init__(self,
                 local_context,
                 name=None,
                 data=None,
                 processing_name=None,
                 owner=None,
                 tags=None,
                 params=None,
                 dependencies=None,
                 code_method=None,
                 vc_info=None,
                 start_time=0,
                 stop_time=0
                 ):
        """ Create a bundle in a local context.

        There are three ways to create bundles:

        1.) Create a bundle with a single call.  Must include a data field!
        b = api.Bundle('examples', name='propensity_model',owner='kyocum',data='/Users/kyocum/model.tgz')

        2.) Create a bundle using a context manager.  The initial call requires only a context.
        with api.Bundle('examples') as b:
            b.add_data(file_list)
            b.add_code_ref('mymodule.mymethod')
            b.add_params({'path': path})
            b.add_tags(tags)

        Users can query the bundle object to create output files directly in the
        referred-to context.  They may also add tags, parameters, code and git info, and start/stop times.
        Once the bundles is "closed" via the context manager, it will be written to disk and immutable.
        Note that one may change anything about an "open" bundle except the context information.

        3.) Open and close manually.
        b = api.Bundle('examples').open()
        b.add_data(file_list)
        b.add_code_ref('mymodule.mymethod')
        b.add_params({'path': path})
        b.add_tags(tags)
        b.close()

        Default name:  If you don't provide a name, Disdat tries to use the basename in `code_ref`.
        Default processing_name:   If you don't provide a processing name, Disdat will use a default that
        takes into consideration your bundles upstream inputs, parameters, and code reference.

        Args:
            local_context (Union[str, `disdat.data_context.DataContext`): The local context name or context object
            name (str): Human name for this bundle.
            data (union(pandas.DataFrame, tuple, None, list, dict)):  The data this bundle contains.
            processing_name (str):  A name that indicates a bundle was made in an identical fashion.
            owner (str):  The owner of the bundle.  Default getpass.getuser()
            tags (dict):  (str,str) dictionary of arbitrary user tags.
            params (dict(str:str)): Dictionary of parameters that <code_method> used to produce this output.
            dependencies (dict(str:bundle)):  Dictionary of argname: bundle, Bundles used to produce this output.
            code_method (str):  A reference to code that created this bundle. Default None
            vc_info (tuple):  Version control information triple: e.g. tuple(git_repo , git_commit, branch)
            start_time (float):  Start time of the process that produced the bundle. Default time.now()
            stop_time (float):   Stop time of the process that produced the bundle. Default time.now()

        """

        self._fs = _get_fs()

        try:
            if isinstance(local_context, DataContext):
                self.data_context = local_context
            elif isinstance(local_context, str):
                self.data_context = self._fs.get_context(local_context)
            else:
                raise Exception("Unable to create Bundle because no context found with name[{}]".format(local_context))
        except Exception as e:
            _logger.error("Unable to allocate bundle in context: {} ".format(local_context, e))
            return

        self._local_dir = None
        self._remote_dir = None
        self._closed = False     # Bundle is closed and immutable
        self._data = None        # The df, array, dictionary the user wants to store

        super(Bundle, self).__init__(human_name=name, #'' if name is None else name,
                                     owner=getpass.getuser() if owner is None else owner,
                                     processing_name=processing_name, #'' if processing_name is None else processing_name
                                     )

        # Add the fields they have passed in.
        if tags is not None:
            self.add_tags(tags)
        if params is not None:
            self.add_params(params)
        if dependencies is not None:
            self.add_dependencies(dependencies.values(), dependencies.keys())
        if code_method is not None:
            self.add_code_ref(code_method)
        if vc_info is not None:
            self.add_git_info(vc_info)
        self.add_timing(start_time, stop_time)

        # Only close and make immutable if the user also adds the data field
        if data is not None:
            self.open()
            self.add_data(data)
            self.close()

    def abandon(self):
        """ Remove on-disk state of the bundle if it is abandoned before it is closed.
         that were left !closed have their directories harvested.
         NOTE: the user has the responsibility to make sure the bundle is not shared across
         threads or processes and that they don't remove a directory out from under another
         thread of control.  E.g., you cannot place this code in __del__ and then _check_closed() b/c
         a forked child process might have closed their copy while the parent deletes theirs.
         """
        self._check_open()
        _logger.debug(f"Disdat api clean_abandoned removing bundle obj [{id(self)}] process[{os.getpid()}")
        PipeBase.rm_bundle_dir(self._local_dir, self.uuid)

    def _check_open(self):
        assert not self._closed, "Bundle must be open (not closed) for editing."

    def _check_closed(self):
        assert self._closed, "Bundle must be closed."

    """ Getters """

    @property
    def closed(self):
        return self._closed

    @property
    def data(self):
        return self._data

    @property
    def name(self):
        return self.pb.human_name

    @property
    def processing_name(self):
        return self.pb.processing_name

    @property
    def owner(self):
        return self.pb.owner

    @property
    def uuid(self):
        return self.pb.uuid

    @property
    def creation_date(self):
        return self.pb.lineage.creation_date

    @property
    def local_dir(self):
        return self._local_dir

    @property
    def tags(self):
        """ Return the tags that the user set
        bundle.tags holds all of the tags, including the "hidden" parameter tags.
        This accesses everything but the parameter tags.
        bundle.params accesses everything but the user tags
        """
        return {k: v for k, v in self.tag_dict.items() if not k.startswith(common.BUNDLE_TAG_PARAMS_PREFIX)}

    @property
    def params(self):
        """ Return the tags that were parameters
        This returns the string version of the parameters (how they were serialized into the bundle)
        Note that we currently use Luigi Parameter parse and serialize to go from string and to string.
        Luigi does so to interpret command-line arguments.
        """
        return {k[len(common.BUNDLE_TAG_PARAMS_PREFIX):]: v for k, v in self.tag_dict.items() if k.startswith(common.BUNDLE_TAG_PARAMS_PREFIX)}

    @property
    def dependencies(self):
        """ Return the argnames and bundles used to produce this bundle.
        Note: We do not return a bundle reference because it may not
        be found in this context, but it remains a valid reference.

        NOTE: At the moment, returns key=processing_name: value=uuid

        Returns:
            dict: (arg_name:(proc_name, uuid))
        """
        found = {}
        arg_names = ['_arg_{}'.format(i) for i in range(0, len(self.pb.lineage.depends_on))]
        for an, dep in zip(arg_names, self.pb.lineage.depends_on):
            if dep.HasField("arg_name"):
                found[dep.arg_name] = (dep.hframe_proc_name, dep.hframe_uuid)
            else:
                found[an] = (dep.hframe_proc_name, dep.hframe_uuid)
        return found

    @property
    def code_ref(self):
        """
        Returns:
            str: The string representing the name of the code that produced this bundle
        """
        return self.pb.lineage.code_method

    @property
    def timing(self):
        """ Return the recorded start and stop times
        Returns:
            (float, float): (start_time, stop_time)
        """
        return self.pb.lineage.start_time, self.pb.lineage.stop_time

    @property
    def git_info(self):
        """ Return the recorded code versioning information.  Assumes
        a repo URL, commit hash, and branch name.
        Returns:
            (str, str, str): (repo, hash, branch)
        """
        return self.pb.lineage.code_repo, self.pb.lineage.code_hash, self.pb.lineage.code_branch

    @property
    def is_presentable(self):
        """ Bundles present as a set of possible type or just a HyperFrame.
        If there is a Python presentation, return True.

        Returns:
            (bool): Where this bundle has a Python presentation
        """
        return super(Bundle, self).is_presentable()

    """ Setters """

    @name.setter
    def name(self, name):
        """ Add the name to the bundle
        Args:
            name (str): The "human readable" name of this data

        Returns:
            self
        """
        self.pb.human_name = name

    @processing_name.setter
    def processing_name(self, processing_name):
        """ Add the processing name to the bundle
        Args:
            processing_name (str): Another way to denote versioning "sameness"
        Returns:
            self
        """
        self.pb.processing_name = processing_name
        self.pb.lineage.hframe_proc_name = processing_name

    def add_tags(self, tags):
        """ Add tags to the bundle.  Updates if existing.

        Args:
            k,v (dict): string:string dictionary

        Returns:
            self
        """
        self._check_open()
        super(Bundle, self).add_tags(tags)
        return self

    def add_params(self, params):
        """ Add (str,str) params to bundle
        """
        self._check_open()
        assert isinstance(params, dict)
        params = {f'{common.BUNDLE_TAG_PARAMS_PREFIX}{k}': v for k, v in params.items()}
        super(Bundle, self).add_tags(params)
        return self

    def add_dependencies(self, bundles, arg_names=None):
        """ Add one or more upstream bundles as dependencies

        Note: Metadata for correct re-use of re-execution semantics.

        Args:
            bundles (Union[list `api.Bundle`, `api.Bundle`]): Another bundle that may have been used to produce this one
            arg_names (list[str]): argument names of the dependencies.  Optional, otherwise default 'arg_<i>' used.

        Returns:
            self
        """
        self._check_open()
        if isinstance(bundles, collections.Iterable):
            if arg_names is None:
                arg_names = ['_arg_{}'.format(i) for i in range(0, len(bundles))]
            LineageRecord.add_deps_to_lr(self.pb.lineage, [(b.processing_name, b.uuid, an) for an, b in zip(arg_names, bundles)])
        else:
            if arg_names is None:
                arg_names = '_arg_0'
            LineageRecord.add_deps_to_lr(self.pb.lineage, [(bundles.processing_name, bundles.uuid, arg_names)])
        return self

    def add_code_ref(self, code_ref):
        """ Add a string referring to the code
        that generated this data.   For example, if Python, one could use
        "package.module.class.method"

        Note: Optional metadata for lineage.

        Args:
            code_ref (str):  String that refers to the code generating the data
        Returns:
              self
        """
        self._check_open()
        self.pb.lineage.code_method = code_ref
        return self

    def add_git_info(self, repo, commit, branch):
        """ Add a string referring to the code
        that generated this data.   For example, if Python, one could use
        "package.module.class.method"

        Note: Optional metadata for lineage.

        Args:
            repo (str):  Repository URL: e.g., "https://github.com/bamboozle/some-project"
            commit (str):  Commit hash: e.g.,  "010b867012ee3f45c63d0435f4af1dc87612a0fd"
            branch (str):  Repo branch: e.g., "HEAD"
        Returns:
              self
        """
        self._check_open()
        self.pb.lineage.code_repo = repo
        self.pb.lineage.code_hash = commit
        self.pb.lineage.code_branch = branch
        return self

    def add_timing(self, start_time, stop_time):
        """ The start and end of the processing.

        Note: Optional metadata.

        Args:
            start_time (float): start timestamp
            stop_time  (float): end timestamp
        """
        self._check_open()
        self.pb.lineage.start_time = start_time
        self.pb.lineage.stop_time = stop_time
        return self

    def add_data(self, data):
        """ Attach data to a bundle.   The bundle must be open and not closed.
            One attaches one data item to a bundle (dictionary, list, tuple, scalar, or dataframe).
            Calling this replaces the latest item -- only the latest will be included in the bundle on close.

            Note: One uses `add_data_row` or `add_data` but not both.  Adding a row after `add_data`
            removes the data.   Using `add_data` after `add_data_row` removes all previously added rows.

        Args:
            data (list|tuple|dict|scalar|`pandas.DataFrame`):

        Returns:
            self
        """
        self._check_open()
        if self._data is not None:
            _logger.warning("Disdat API add_data replacing existing data on bundle")
        self._data = data
        return self

    """ Alternative construction post allocation """

    def fill_from_hfr(self, hfr):
        """ Given an internal hyperframe, copy out the information to this user-side Bundle object.

        Assume the user has set the data_context appropriately when creating the bundle object

        The Bundle object inherits from HyperFrameRecord.  So we needs to:
        a.) set the pb to point to this pb, they may both point to the same pb, but this bundle will be *closed*
        b.) init internal state
        c.) Set bundle object specific fields.   Note we do not set or clear the self.data_context

        Args:
            hfr:

        Returns:
            self: this bundle object with self.data containing the kind of object the user saved.

        """
        self._check_open()
        self._closed = True
        self.pb = hfr.pb
        self.init_internal_state()
        self._data = self.data_context.present_hfr(hfr)
        return self

    """ Python Context Manager Interface """

    def __enter__(self):
        """ 'open'
        """
        return self.open()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """ 'close'
        If there has been an exception, let the user deal with the written created bundle.
        """
        self.close()

    def open(self, force_uuid=None):
        """ Management operations to open bundle for writing.
        At this time all of the open operations, namely creating the managed path
        occur in the default constructor or in the class fill_from_hfr constructor.

        Args:
            force_uuid (str): DEPRECATING - do not use.  Force to open a bundle with a specific bundle.

        Returns:
            None
        """
        if self._closed:
            _logger.error("Bundle is closed -- unable to re-open.")
            assert False
        self._local_dir, self.pb.uuid, self._remote_dir = self.data_context.make_managed_path(uuid=force_uuid)
        return self

    def close(self):
        """ Write out this bundle as a hyperframe.

        Parse the data, set presentation, create lineage, and
        write to disk.

        This closes the bundle so it may not be re-used.

        Returns:
            None
        """
        def extract_human_name(code_ref):
            return code_ref.split('.')[-1]

        try:
            presentation, frames = PipeBase.parse_return_val(self.uuid, self._data, self.data_context)
            self.add_frames(frames)
            self.pb.presentation = presentation
            assert self.uuid != '', "Disdat API Error: Cannot close a bundle without a UUID."
            self.pb.lineage.hframe_uuid = self.uuid
            if self.name == '':
                if self.code_ref != '':
                    self.name = extract_human_name(self.code_ref)
            if self.processing_name == '':
                self.processing_name = self.default_processing_name()
            self._mod_finish(new_time=True)   # Set the hash based on all the context now in the pb, record create time
            self.data_context.write_hframe(self)
        except Exception as error:
            """ If we fail for any reason, remove bundle dir and raise """
            self.abandon()
            raise

        self._closed = True

        return self

    """ Convenience Routines """

    def cat(self):
        """ Return the data in the bundle
        The data is already present in .data

        """
        self._check_closed()
        return self._data

    def rm(self):
        """ Remove bundle from the current context associated with this bundle object
        Only remove this bundle with this uuid.
        This only makes sense if the bundle is closed.
        """
        self._check_closed()
        self._fs.rm(uuid=self.uuid, data_context=self.data_context)
        return self

    def commit(self):
        """ Shortcut version of api.commit(uuid=bundle.uuid)

        Returns:
            self
        """
        self._fs.commit(None, None, uuid=self.uuid, data_context=self.data_context)
        return self

    def push(self):
        """ Shortcut version of api.push(uuid=bundle.uuid)

        Returns:
            (`disdat.api.Bundle`): this bundle

        """
        self._check_closed()

        if self.data_context.get_remote_object_dir() is None:
            raise RuntimeError("Not pushing: Current branch '{}/{}' has no remote".format(self.data_context.get_remote_name(),
                                                                                          self.data_context.get_local_name()))

        self._fs.push(uuid=self.uuid, data_context=self.data_context)

        return self

    def pull(self, localize=False):
        """ Shortcut version of api.pull()

        Note if localizing, then we update this bundle to reflect the possibility of new, local links

        """
        self._check_closed()
        self._fs.pull(uuid=self.uuid, localize=localize, data_context=self.data_context)
        if localize:
            assert self.is_presentable
            self._data = self.data_context.present_hfr(self) # actualize link urls
        return self

    def get_directory(self, dir_name):
        """ Returns path `<disdat-managed-directory>/<dir_name>`.  This gives the user a local
        output directory into which to write files.  This is useful when a user needs to give an external tool, such
        as Spark or Tensorflow, a directory to place output files.

        Add this path as you would add file paths to your output bundle.  Disdat will incorporate
        all the data found in this directory into the bundle.

        See Pipe.create_output_dir()

        Arguments:
            dir_name (str): A basedir of a directory to appear in the local bundle.

        Returns:
            str: A directory path managed by disdat
        """
        self._check_open()

        if dir_name[-1] == '/':
            dir_name = dir_name[:-1]

        # if the user erroneously passes in the directory of the bundle, return same
        if dir_name == self._local_dir:
            return self._local_dir

        fqp = os.path.join(self._local_dir, dir_name.lstrip('/'))

        try:
            os.makedirs(fqp)
        except OSError as why:
            if not why.errno == errno.EEXIST:
                _logger.error("Creating directory in bundle directory failed errno {}".format(why.strerror))
                raise
            # else directory exists is OK and fall through
        except IOError as why:
            _logger.error("Creating directory in bundle directory failed {}".format(why))
            raise

        return fqp

    def get_file(self, filename):
        """ Create a "managed path" to store file `filename` directly in the local data context.
        This allows you to create versioned data sets without file copies and without worrying about where
        your data files are to be stored.

        To use, you must a.) write data into this file-like object (a 'target'), and b.) add this
        target to the bundle by either including its file path or the luigi target object itself.

        TODO: for binary files add a binary=True, and return luigi.LocalTarget('test.npz', format=luigi.format.Nop)

        Arguments:
            filename (str,list,dict): filename to create in the bundle

        Returns:
            `luigi.LocalTarget`
        """
        self._check_open()
        return PipeBase.filename_to_luigi_targets(self._local_dir, filename)

    def get_remote_directory(self, dir_name):
        """ Returns path `<disdat-managed-remote-directory>/<dir_name>`.  This gives the user a remote (e.g., s3)
        output directory into which to write files.  This is useful when a user needs to give an external tool, such
        as Spark or Tensorflow, a directory to place output files.

        Add this path as you would add file paths to your output bundle.  Disdat will incorporate
        all the data found in this directory into the bundle.

        See Pipe.create_output_dir_remote()

        Arguments:
            dir_name (str): A basedir of a directory to appear in the remote bundle.

        Returns:
            str: A directory path managed by disdat
        """
        self._check_open()

        fqp = os.path.join(self._remote_dir, dir_name.lstrip('/'))

        return fqp

    def get_remote_file(self, filename):
        """  Create a "managed path" to store file `filename` directly in the remote data context.
        This allows you to create versioned data sets without file copies.

        To use, you must a.) write data into this file-like object (a 'target'), and b.) add this
        target to the bundle by either including its file path or the luigi target object itself.

        Note: This requires that the local context has been bound to a remote data context.

        Arguments:
            filename (str,list,dict): filename to create in the bundle

        Returns:
            `luigi.s3.S3Target`
        """
        self._check_open()

        if not self.data_context.remote_ctxt_url:
            raise Exception('Managed S3 path creation requires a remote context')

        return PipeBase.filename_to_luigi_targets(self._remote_dir, filename)

    @staticmethod
    def calc_default_processing_name(code_ref, params, dep_proc_ids):
        """
        Args:
            code_ref (str): The code ref string
            params (dict): argname:str
            dep_proc_ids (dict):  dict of argname:processing_id of upstream dependencies

        Returns:
            (str): The processing name
        """
        def sorted_md5(input_dict):
            ids = [input_dict[k] for k in sorted(input_dict)]
            as_one_str = '.'.join(ids)
            return hashlib.md5(as_one_str.encode('utf-8')).hexdigest()

        dep_hash = sorted_md5(dep_proc_ids)[:PROC_ID_TRUNCATE_HASH]
        param_hash = sorted_md5(params)[:PROC_ID_TRUNCATE_HASH]
        processing_id = code_ref + '_' + param_hash + '_' + dep_hash

        return processing_id

    def default_processing_name(self):
        """ The default processing name defines the set of bundles that were ostensibly created
        from the same code and parameters.  These bundles share the
        same code_ref, parameters, and dependency processing_names.   Thus different versions
        might very well be different because the code changed (but not the code_ref).  Or the code
        might read an external DB whose tables change.

        The name is <code_ref>_<param_hash>_<hash(dependencies.processing_name)

        Note: Cache the processing name so other bundles

        Returns:
            processing_name(str): The processing name.
        """

        if self._closed:
            return self.pb.processing_name

        # Iterate through dependencies dict argname: (processing_name, uuid)
        dep_proc_ids = {k: v[0] for k, v in self.dependencies.items()}

        return Bundle.calc_default_processing_name(self.code_ref, self.params, dep_proc_ids)


def _get_context(context_name):
    """Retrieve data context given name.   Raise exception if not found.

    Args:
        context_name(str): <remote context>/<local context> or <local context>

    Returns:
        (`disdat.data_context.DataContext`)
    """

    fs = _get_fs()

    data_context = fs.get_context(context_name)

    if data_context is None:
        # Try once to see if needs to be loaded
        data_context = fs.reload_context(context_name)

    if data_context is None:
        error_msg = "Unable to perform operation: could not find context {}".format(context_name)
        _logger.error(error_msg)
        raise RuntimeError(error_msg)

    return data_context


def current_context():
    """ Return the current context name (not object) """

    fs = _get_fs()

    try:
        return fs.curr_context_name
    except Exception as se:
        print(("Current context failed due to error: {}".format(se)))
        return None


def ls_contexts():
    """ Return list of contexts and their remotes

    Returns:
        List[Tuple(str,str)]: Return a list of tuples containing <local context>, <remote context>@<remote string>

    """
    # TODO: have the fs object provide a wrapper function
    fs = _get_fs()
    fs.reload_all_contexts()
    return list(fs._all_contexts.keys())


def context(context_name):
    """ Create a new context

    Args:
        context_name(str): <remote context>/<local context> or <local context>

    Returns:
        (int) : 0 if successfully created branch, 1 if branch exists
    """
    fs = _get_fs()
    return fs.branch(context_name)


def delete_context(context_name, remote=False, force=False):
    """ Delete a local context.  Will not delete any data if there is a remote attached.

    Args:
        context_name (str):  The name of the local context to delete.
        remote (bool): Delete remote context as well, force must be True
        force (bool): Force deletion if dirty

    Returns:
        None
    """
    fs = _get_fs()
    fs.delete_context(context_name, remote=remote, force=force)


def switch(context_name):
    """ Stateful switch to a different context.
    This changes the current default context used by the CLI commands.

    Args:
        context_name (str):  The name of the local context to switch into.

    Returns:
        None
    """
    fs = _get_fs()
    fs.switch(context_name)


def remote(local_context, remote_context, remote_url):
    """ Add a remote to local_context.

    Note that this local context may already have a remote bound.   This means that it might have
    references to bundles that have not been localized (file references will be 's3:`).

    Args:
        local_context (str):  The name of the local context to which to add the remote.
        remote_context (str):  The name of the remote context.
        remote_url (str): The S3 path that holds the contexts, e.g., s3://disdat-prod/beta/

    Returns:
        None

    """
    _logger.debug("Adding remote context {} at URL {} on local context '{}'".format(remote_context,
                                                                                    remote_url,
                                                                                    local_context))

    # Be generous and fix up S3 URLs to end on a directory.
    remote_url = '{}/'.format(remote_url.rstrip('/'))

    data_context = _get_context(local_context)

    data_context.bind_remote_ctxt(remote_context, remote_url)


def search(local_context, human_name=None, processing_name=None, tags=None,
           is_committed=None, find_intermediates=False, find_roots=False,
           before=None, after=None):
    """ Search for bundle in a local context.
    Allow for searching by human name, is_committed, is intermediate or is root task output, and tags.

    At this time the SQL interface in disdat.fs does not allow searching for entries without particular tags.

    Args:
        local_context (str): The name of the local context to search.
        human_name (str): Optional, interpret as a simple regex (one kleene star).
        processing_name (str): Optional, interpret as a simple regex (one kleene star).
        tags (dict): Optional, a set of key:values the bundles must have.
        is_committed (bool): Optional, if True return committed, if False return uncommitted
        find_intermediates (bool):  Optional, results must be intermediates
        find_roots (bool): Optional, results must be final outputs
        before (str): Optional, return bundles < "12-1-2009" or "12-1-2009 12:13:42"
        after (str): Optional, return bundles >= "12-1-2009" or "12-1-2009 12:13:42"

    Returns:
        (list): List of API bundle objects
    """
    results = []

    data_context = _get_context(local_context)

    if tags is None and (find_roots or find_intermediates):
        tags = {}

    if find_roots:
        tags.update({'root_task': True})

    if before is not None:
        before = disdat.fs._parse_date(before, throw=True)

    if after is not None:
        after = disdat.fs._parse_date(after, throw=True)

    for i, r in enumerate(data_context.get_hframes(human_name=human_name,
                                                   processing_name=processing_name,
                                                   tags=tags,
                                                   before=before,
                                                   after=after)):
        if find_intermediates:
            if r.get_tag('root_task') is not None:
                continue
        if is_committed is not None:
            if is_committed:
                if r.get_tag('committed') is None:
                    continue
            else:
                if r.get_tag('committed') is not None:
                    continue
        results.append(Bundle(data_context).fill_from_hfr(r))

    return results


def _lineage_single(ctxt_obj, uuid):
    """ Helper function to return lineage information for a single bundle uuid in a local context.
    Shortcut: `api.lineage(<uuid>)` is equivalent to `api.search(uuid=<uuid>)[0].lineage`

    Args:
        ctxt_obj (`data_context.DataContext`): The context in which the bundle lives.
        uuid (str): The UUID of the bundle in question

    Returns:
        `hyperframe_pb2.Lineage`: Python ProtoBuf object representing lineage

    """

    hfr = ctxt_obj.get_hframes(uuid=uuid)

    if hfr is None or len(hfr) == 0:
        return None

    assert(len(hfr) == 1)

    b = Bundle(ctxt_obj, 'unknown')
    b.fill_from_hfr(hfr[0])

    return b.pb.lineage


def lineage(local_context, uuid, max_depth=None):
    """ Return lineage information from a bundle uuid in a local context.
    This will follow in a breadth-first manner the lineage information.
    Shortcut: `api.lineage(<uuid>)` is equivalent to `api.search(uuid=<uuid>)[0].lineage`

    Args:
        local_context (str): The context in which the bundle lives.
        uuid (str): The UUID of the bundle in question
        max_depth (int): Maximum depth returned in search of lineage objects.  Default None is unbounded.

    Returns:
        list(`hyperframe_pb2.Lineage`): List of Protocol Buffer Lineage objects in BFS order
    """

    ctxt_obj = _get_context(local_context)

    l = _lineage_single(ctxt_obj, uuid)

    frontier = []  # BFS (depth, uuid, lineage)

    found = []     # Return [(depth, uuid, lineage),...]

    depth = 0      # Current depth of BFS

    while l is not None:
        if max_depth is not None and depth > max_depth:
            break
        found.append((depth, l.hframe_uuid, l))
        frontier.extend([(depth + 1, deps.hframe_uuid, _lineage_single(ctxt_obj, deps.hframe_uuid)) for deps in l.depends_on])
        l = None
        while len(frontier) > 0:
            depth, uuid, l = frontier.pop(0)
            if l is None:
                found.append((depth, uuid, None))  # not found locally
                continue
            else:
                break
    return found


def get(local_context, bundle_name, uuid=None, tags=None):
    """ Retrieve the latest (by date) bundle from local context, with name, uuid, and tags.

    Args:
        local_context (str):  The name of the local context to search.
        bundle_name (str):  The human bundle name to find.
        uuid (str):  The UUID to return.  Trumps `bundle_name`
        tags (dict):  The tags the bundle must have.

    Returns:
        `api.Bundle`
    """

    fs = _get_fs()

    data_context = _get_context(local_context)

    if uuid is None:
        hfr = fs.get_latest_hframe(bundle_name, tags=tags, data_context=data_context)
    else:
        hfr = fs.get_hframe_by_uuid(uuid, tags=tags, data_context=data_context)

    if hfr is None:
        return None

    b = Bundle(data_context).fill_from_hfr(hfr)

    return b


def rm(local_context, bundle_name=None, uuid=None, tags=None, rm_all=False, rm_old_only=False, force=False):
    """ Delete a bundle with a certain name, uuid, or tags.  By default removes the most recent bundle.
    Otherwise one may specify `rm_all=True` to remove all bundles, or `rm_old_only=True` to remove
    all bundles but the most recent.

    Args:
        local_context (str): Local context name from which to remove the bundle
        bundle_name (str): Optional human-given name for the bundle
        uuid (str): Optional UUID for the bundle to remove.  Trumps bundle_name argument if both given
        tags (dict(str:str)): Optional dictionary of tags that must be present on bundle to remove
        rm_all (bool): Remove latest and all historical if given bundle_name
        rm_old_only (bool): remove everything but latest if given bundle_name
        force (bool): If a db-link exists and it is the latest on the remote DB, force remove. Default False.

    Returns:
        None
    """

    fs = _get_fs()

    data_context = _get_context(local_context)

    fs.rm(human_name=bundle_name, rm_all=rm_all, rm_old_only=rm_old_only,
          uuid=uuid, tags=tags, force=force, data_context=data_context)


def add(local_context, bundle_name, path, tags=None):
    """  Create bundle bundle_name given path path_name.

    If path is a directory, then create bundle with items in directory as a list of links.
    If path is a file or set of files:
        Create bundle as links to this file.

    Note: Bundle presents as Python list of files, unless path is a single file.  In which case
    the Bundle presents as just a single file link.

    Args:
        local_context (str): The local context in which to create this bundle
        bundle_name (str):  The human name for this new bundle
        path (str):  The directory or file from which to create a bundle
        tags (dict):  The set of tags to attach to this bundle

    Returns:
        `api.Bundle`
    """

    _logger.debug('Adding file {} to bundle {} in context {}'.format(path,
                                                                     bundle_name,
                                                                     local_context))

    # The user can pass either a path or an iterable of paths,
    # convert single paths to iterable for compatibility
    paths = path
    try:
        assert os.path.exists(paths)
        paths = [paths]
    except TypeError:
        pass

    with Bundle(local_context=local_context, name=bundle_name, owner=getpass.getuser()) as b:
        file_list = []

        for path in paths:
            assert os.path.exists(path), "Disdat cannot find file at path: {}".format(path)

            if os.path.isfile(path):
                file_list.append(path)
            else:
                base_path = path
                for root, dirs, files in os.walk(path, topdown=True):
                    # create a directory at root
                    # /x/y/z/fileA
                    # /x/y/z/a/fileB
                    dst_base_path = root.replace(base_path, '')
                    if dst_base_path == '':
                        dst_base_path = b._local_dir
                    else:
                        dst_base_path = b.get_directory(dst_base_path)
                    for name in files:
                        dst_full_path = os.path.join(dst_base_path, name)
                        src_full_path = os.path.join(root,name)
                        shutil.copyfile(src_full_path, dst_full_path)
                        file_list.append(dst_full_path)

        # Return the list (unless it is length 1)
        file_list = file_list[0] if len(file_list) == 1 else file_list
        b.add_data(file_list)
        b.add_code_ref(add.__name__)
        b.add_params({'path': path})
        if tags is not None and len(tags) > 0:
            b.add_tags(tags)

    return b


def cat(local_context, bundle_name):
    """Retrieve the data representation of the latest bundle with name `bundle_name`

    Args:
        local_context (str): The name of the local context from which to get bundle
        bundle_name (str): The human name of bundle

    Returns:
        Object: The presentation data type with which this bundle was created.

    """

    fs = _get_fs()

    data_context = _get_context(local_context)

    return fs.cat(bundle_name, data_context=data_context)


def commit(local_context, bundle_name, tags=None, uuid=None):
    """ Commit bundle in this local context.  This adds a special `committed` tag
    to the bundle, allowing it to be pushed to a remote.   This also removes the bundle
    from the "uncommitted history" limit.  One can have as many versions of committed bundles
    as they wish, but only N uncommitted bundles.

    Args:
        local_context (str): The local context in which the bundle exists
        bundle_name (str): The human name of the bundle to commit
        tags (dict(str:str)): Optional dictionary of tags with which to find bundle
        uuid (str): UUID of the bundle to commit

    Returns:
        None
    """

    fs = _get_fs()

    data_context = _get_context(local_context)

    if tags is None:
        tags = {}

    fs.commit(bundle_name, tags, uuid=uuid, data_context=data_context)


def push(local_context, bundle_name, tags=None, uuid=None):
    """ Push a bundle to a remote repository.

    Args:
        local_context (str):  The local context to push to (must have a remote).
        bundle_name (str): human name of the bundle to push or None (if using uuid)
        tags (dict): Tags the bundle must have
        uuid (str): Optional UUID of the bundle to push.  UUID takes precedence over bundle name

    Returns:
        None

    """

    fs = _get_fs()

    data_context = _get_context(local_context)

    if data_context.get_remote_object_dir() is None:
        raise RuntimeError("Not pushing: Current branch '{}/{}' has no remote".format(data_context.get_remote_name(),
                                                                                      data_context.get_local_name()))

    fs.push(human_name=bundle_name, uuid=uuid, tags=tags, data_context=data_context)


def pull(local_context, bundle_name=None, uuid=None, localize=False):
    """ Pull bundles from the remote context into this local context.
    If there is no remote context associated with this context, then this is
    a no-op.  fs.pull will raise UserWarning if there is no remote context.

    Args:
        local_context (str): The local context whose remote the bundle will be pulled
        bundle_name (str): An optional human bundle name
        uuid (str): An optional bundle UUID
        localize (bool): Whether to bring linked files directly into bundle directory

    Returns:
        None
    """

    fs = _get_fs()

    data_context = _get_context(local_context)

    fs.pull(human_name=bundle_name, uuid=uuid, localize=localize, data_context=data_context)


def apply(local_context, transform, output_bundle='-',
          input_tags=None, output_tags=None, force=False,
          force_all=False, params=None,
          output_bundle_uuid=None, central_scheduler=False, workers=1,
          incremental_push=False, incremental_pull=False):
    """ Execute a Disdat pipeline natively on the local machine.   Note that `api.run` will execute
    a Disdat pipeline that has been dockerized (either locally or remotely on AWS Batch or AWS Sagemaker)

    Args:
        local_context (str):  The name of the local context in which the pipeline will run in the container
        transform (type[disdat.pipe.PipeTask]): A reference to the Disdat Pipe class
        output_bundle (str):  The name of the output bundle.  Defaults to `<task_name>_<param_hash>`
        input_tags: optional tags dictionary for selecting input bundle
        output_tags: optional tags dictionary to tag output bundle
        force (bool): Force re-running this transform, default False
        force_all (bool): Force re-running ALL transforms, default False
        params: optional parameters dictionary
        output_bundle_uuid: Force UUID of output bundle
        central_scheduler (bool): Use a central scheduler, default False, i.e., use local scheduler
        workers (int): Number of workers, default 1.
        incremental_push (bool): commit and push task bundles as they complete
        incremental_pull (bool): localize bundles from remote as they are required by downstream tasks

    Returns:
        result (int):  0 success, >0 if issue

    """

    # check for deprecated str input for transform
    if isinstance(transform, str):
        msg = ('PipeTask classes should be passed as references, not strings, '
               'support for string inputs will be removed in future versions')
        warnings.warn(msg, DeprecationWarning)
        transform = common.load_class(transform)

    data_context = _get_context(local_context)

    if input_tags is None:
        input_tags = {}

    if output_tags is None:
        output_tags = {}

    if params is None:
        params = {}

    # IF apply raises, let it go up.
    # If API, caller can catch.
    # If CLI, python will exit 1
    result = disdat.apply.apply(output_bundle, params, transform,
                                input_tags, output_tags, force, force_all,
                                output_bundle_uuid=output_bundle_uuid,
                                central_scheduler=central_scheduler,
                                workers=workers,
                                data_context=data_context,
                                incremental_push=incremental_push,
                                incremental_pull=incremental_pull)

    # If no raise, but luigi says not successful
    # If API (here), then raise for caller to catch.
    # For CLI, we exit with 1
    common.apply_handle_result(result, raise_not_exit=True)

    return result


def run(setup_dir,
        local_context,
        pipe_cls,
        pipeline_args=None,
        output_bundle='-',
        remote_context=None,
        remote_s3_url=None,
        backend=Backend.default(),
        input_tags=None,
        output_tags=None,
        force=False,
        force_all=False,
        pull=None,
        push=None,
        no_push_int=False,
        vcpus=2,
        memory=4000,
        workers=1,
        no_submit=False,
        aws_session_token_duration=42300,
        job_role_arn=None):
    """ Execute a pipeline in a container.  Run locally, on AWS Batch, or AWS Sagemaker

    Simplest execution is with a setup directory (that contains your setup.py), the local context in which to
    execute, and the pipeline to run.   By default this call runs the container locally, reading and writing data only
    to the local context.

    By default this call will assume the remote_context and remote_s3_url of the local context on this system.
    Note that the user must provide both the remote_context and remote_s3_url to override the remote context bound
    to the local context (if any).

    Args:
        setup_dir (str): The directory that contains the setup.py holding the requirements for any pipelines
        local_context (str): The name of the local context in which the pipeline will run in the container
        pipe_cls (str): The pkg.module.class of the root of the pipeline DAG
        pipeline_args (dict): Dictionary of the parameters of the root task
        output_bundle (str): The human name of output bundle
        remote_context (str): The remote context to pull / push bundles during execution. Default is `local_context`
        remote_s3_url (str): The remote's S3 path
        backend : Backend.Local | Backend.AWSBatch. Default Backend.local
        input_tags (dict): str:str dictionary of tags required of the input bundle
        output_tags (dict): str:str dictionary of tags placed on all output bundles (including intermediates)
        force (bool):  Re-run the last pipe task no matter prior outputs
        force_all (bool):  Re-run the entire pipeline no matter prior outputs
        pull (bool): Pull before execution. Default if Backend.Local then False, else True
        push (bool): Push output bundles to remote. Default if Backend.Local then False, else True
        no_push_int (bool):  Do not push intermediate task bundles after execution. Default False
        vcpus (int): Number of virtual CPUs (if backend=`AWSBatch`). Default 2.
        memory (int): Number of MB (if backend='AWSBatch'). Default 2000.
        workers (int):  Number of Luigi workers. Default 1.
        no_submit (bool): If True, just create the AWS Batch Job definition, but do not submit the job
        aws_session_token_duration (int): Seconds lifetime of temporary token (backend='AWSBatch'). Default 42300
        job_role_arn (str): AWS ARN for job execution in a batch container (backend='AWSBatch')

    Returns:
        json (str):

    """

    pipeline_arg_list = []
    if pipeline_args is not None:
        for k,v in pipeline_args.items():
            pipeline_arg_list.append(k)
            pipeline_arg_list.append(json.dumps(v))

    # Set up context as 'remote_name/local_name'
    if remote_context is None:
        assert remote_s3_url is None, "disdat.api.run: user must specify both remote_s3_url and remote_context"
        context = local_context
    else:
        assert remote_s3_url is not None, "disdat.api.run: user must specify both remote_s3_url and remote_context"
        context = "{}/{}".format(remote_context, local_context)

    retval = run_entry(output_bundle=output_bundle,
                       pipeline_root=setup_dir,
                       pipeline_args=pipeline_arg_list,
                       pipe_cls=pipe_cls,
                       backend=backend,
                       input_tags=input_tags,
                       output_tags=output_tags,
                       force=force,
                       force_all=force_all,
                       context=context,
                       remote=remote_s3_url,
                       pull=pull,
                       push=push,
                       no_push_int=no_push_int,
                       vcpus=vcpus,
                       memory=memory,
                       workers=workers,
                       no_submit=no_submit,
                       job_role_arn=job_role_arn,
                       aws_session_token_duration=aws_session_token_duration)

    return retval


def dockerize(setup_dir,
              config_dir=None,
              build=True,
              push=False,
              sagemaker=False):
    """ Create a docker container image using a setup.py and pkg.module.class description of the pipeline.

    Note:
        Users set the os_type and os_version in the disdat.cfg file.
        os_type: The base operating system type for the Docker image
        os_version: The base operating system version for the Docker image

    Args:
        setup_dir (str): The directory that contains the setup.py holding the requirements for any pipelines
        config_dir (str): The directory containing the configuration of .deb packages
        build (bool): If False, just copy files into the Docker build context without building image.
        push (bool):  Push the container to the repository
        sagemaker (bool): Create a Docker image executable as a SageMaker container (instead of a local / AWSBatch container).

    Returns:
        (int): 0 if success, 1 on failure

    """

    retval = dockerize_entry(pipeline_root=setup_dir,
                             config_dir=config_dir,
                             os_type=None,
                             os_version=None,
                             build=build,
                             push=push,
                             sagemaker=sagemaker
                             )

    return retval


def dockerize_get_id(setup_dir):
    """ Retrieve the docker container image identifier

    Args:
        setup_dir (str): The directory that contains the setup.py holding the requirements for any pipelines

    Returns:
        (str): The full docker container image hash
    """
    return dockerize_entry(pipeline_root=setup_dir, get_id=True)
