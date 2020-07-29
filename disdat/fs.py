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
The disdat file system interface.

This is the object used to access information about disdat bundles
from a variety of backend resources.

"""
from __future__ import print_function

import os
import json
import uuid
import time
from datetime import datetime
from enum import Enum
import shutil
import collections
import subprocess
import six

import pandas as pd

import disdat.hyperframe as hyperframe
import disdat.common as common
import disdat.utility.aws_s3 as aws_s3
from disdat.data_context import DataContext
from disdat.common import DisdatConfig, CatNoBundleError
from disdat import logger as _logger

CodeVersion = collections.namedtuple('CodeVersion', 'semver hash tstamp branch url dirty')

CONTEXTS = ['DEFAULT']
META_FS_FILE = 'fs.json'

ObjectTypes = Enum('ObjectTypes', 'bundle atom')
ObjectState = Enum('ObjectState', 'present removed')


def _run_git_cmd(git_dir, git_cmd, get_output=False):
    """Run a git command in a local git repository.

    :param git_dir: A path within a local git repository, i.e. it may be a
        subdirectory within the repository.
    :param git_cmd: The git command string to run, i.e., everything that
        would follow after :code:`git` on the command line.
    :param get_output: If :code:`True`, return the command standard output
        as a string; default is to return the command exit code.
    """

    verbose = False

    cmd = ['git', '-C', git_dir] + git_cmd.split()
    if verbose:
        _logger.debug('Running git command {}'.format(cmd))
    if get_output:
        try:
            with open(os.devnull, 'w') as null_file:
                output = subprocess.check_output(cmd, stderr=null_file)
        except subprocess.CalledProcessError as e:
            _logger.debug("Unable to run git command {}: exit {}: likely no git repo, e.g., running in a container.".format(cmd, e.returncode))
            return e.returncode
    else:
        with open(os.devnull, 'w') as null_file:
            output = subprocess.call(cmd, stdout=null_file, stderr=null_file)

    # If P3, this may be a byte array.   If P2, if not unicode, convert ...
    output = six.ensure_str(output)

    return output


def determine_pipe_version(pipe_root):
    """
    Given a pipe file path, return the repo status. If they are set, use the environment variables,
    otherwise run the git commands.


    Args:
        pipe_root: path to the root of the pipeline

    Returns:
        CodeVersion: populated object with the git hash, branch, fetch url, last updated date
        and "dirty" status. A pipeline is considered to be dirty if there are modified files
        which haven't been checked in yet.
    """

    def _check_type_and_rstrip(val, default):
        if isinstance(val, six.string_types):
            val = val.rstrip()
        else:
            val = default
        return val

    _logger.debug("PIPELINE_GIT_HASH = {}  cli hash = {}".format(os.environ.get('PIPELINE_GIT_HASH'),
                                                                 _run_git_cmd(pipe_root, 'rev-parse --short HEAD', get_output=True)))
    _logger.debug("git_branch = {os.environ.get('PIPELINE_GIT_BRANCH')}")

    git_hash = os.getenv('PIPELINE_GIT_HASH', _run_git_cmd(pipe_root, 'rev-parse HEAD',  get_output=True))
    git_hash = _check_type_and_rstrip(git_hash, "unknown")

    git_branch = os.getenv('PIPELINE_GIT_BRANCH', _run_git_cmd(pipe_root, 'rev-parse --abbrev-ref HEAD',  get_output=True))
    git_branch = _check_type_and_rstrip(git_branch, "unknown")

    git_fetch_url = os.getenv('PIPELINE_GIT_FETCH_URL', _run_git_cmd(pipe_root, 'config --get remote.origin.url',  get_output=True))
    git_fetch_url = _check_type_and_rstrip(git_fetch_url, "unknown")

    git_timestamp = os.getenv('PIPELINE_GIT_TIMESTAMP', _run_git_cmd(pipe_root, 'log -1 --pretty=format:%aI',  get_output=True))
    git_timestamp = _check_type_and_rstrip(git_timestamp, "unknown")

    git_dirty = bool(os.getenv('GIT_DIRTY', _run_git_cmd(pipe_root, 'diff --name-only',  get_output=True)))

    obj_version = CodeVersion(semver="0.1.0", hash=git_hash, tstamp=git_timestamp, branch=git_branch,
                              url=git_fetch_url, dirty=git_dirty)

    return obj_version


class DisdatFS(object):
    """
    HyperFrame (bundle) access layer (singleton)

    We have one class attribute that keeps track of allocated bundle directories.
    This is filled at the time we try to run apply.

    """
    __metaclass__ = common.SingletonType

    current_pipe_version = None

    class JsonConfig(object):
        ACTIVE_CONTEXT = '_mangled_curr_context_name'

    @staticmethod
    def get_pipe_version(pipe_root):
        """
        Given a pipeline root path, return the status of the repo. The git status will be
        stored on the FS singleton so it doesn't need to be calculated each time this
        getter function is called.

        Args:
            pipe_root:

        Returns:
            CodeVersion: populated object with the git hash, branch, fetch url, last updated date
            and "dirty" status. A pipeline is considered to be dirty if there are modified files
            which haven't been checked in yet.
        """

        # return the pipe version if we already have it
        if DisdatFS.current_pipe_version is not None:
            return DisdatFS.current_pipe_version

        pipe_version = determine_pipe_version(pipe_root)

        # set this so we don't need to calculate it each time
        DisdatFS.put_pipe_version(pipe_version)

        _logger.debug("pipe_root = {} pipe_version = {}".format(pipe_root, pipe_version))

        return pipe_version

    @staticmethod
    def put_pipe_version(pipe_version):
        """
        Sets current pipe version

        Args:
            pipe_version:
        """
        DisdatFS.current_pipe_version = pipe_version

    @staticmethod
    def clear_pipe_version():
        DisdatFS.current_pipe_version = None

    def __init__(self):
        """ Create an FS object.
        1.) Load our state from the meta_dir -- which context we are working in
        2.) Load all available contexts

        :return: new DisdatFS handle
        """
        # Lazily loaded properties
        self._curr_context = None
        self.__all_contexts = None

    @property
    def curr_context_name(self):
        return self.curr_context.local_ctxt

    @property
    def _all_contexts(self):
        if self.__all_contexts is None:
            self.reload_all_contexts()
        return self.__all_contexts

    @property
    def curr_context(self):
        if self._curr_context is None:
            try:
                active_context_name = self.load()
            except RuntimeError:
                return None
            try:
                self._curr_context = self._all_contexts[active_context_name]
            except KeyError:
                raise AssertionError("active context '{}' not found in available contexts".format(active_context_name))
        assert self._curr_context.is_valid(), 'not in a valid context'
        return self._curr_context

    @curr_context.setter
    def curr_context(self, value):
        self._curr_context = value
        self.save()

    def load(self):
        """
        Load the fs object found in the meta_dir.

        :return: (string) name of active context
        """
        meta_file = os.path.join(DisdatConfig.instance().get_meta_dir(), META_FS_FILE)

        if not os.path.isfile(meta_file):
            _logger.debug("No disdat {} meta fs data file found.".format(meta_file))
            raise RuntimeError("No current local context, please change context with 'dsdt switch'")
        else:
            with open(meta_file, 'r') as json_file:
                state_dict = json.loads(json_file.readline())
            try:
                return state_dict[self.JsonConfig.ACTIVE_CONTEXT]
            except KeyError:
                raise RuntimeError("No current local context, please change context with 'dsdt switch'")

    def save(self):
        """
        Write out the json describing the fs.
        Only current context now.

        Returns:
        """
        meta_file = os.path.join(DisdatConfig.instance().get_meta_dir(), META_FS_FILE)

        with open(meta_file, 'w') as json_file:
            state_dict = {self.JsonConfig.ACTIVE_CONTEXT: self.curr_context_name}
            json_file.write(json.dumps(state_dict))

    def reload_all_contexts(self):
        """ The set of available contexts can change, so this is a refresh hook.
        This happens if you use the API, then someone uses the CLI out-of-band.

        Returns:
            None
        """
        self.__all_contexts = DataContext.load()

    def reload_context(self, target_context):
        """ If a particular context is not available, load if it exists

        This happens if you use the API, then someone uses the CLI out-of-band.

        Returns:
            None
        """

        context = DataContext.load([target_context])

        if len(context) == 0:
            self.__all_contexts.pop(target_context, None)
            return None

        assert len(context) == 1
        self.__all_contexts[target_context] = context[0]
        return context[0]

    def in_context(self):
        """
        Are we currently in a valid context?
        :return:
        """
        return self.curr_context and self.curr_context.is_valid()

    def rmr(self, human_name=None, uuid=None, tags=None, dryrun=False, data_context=None):
        """
        Remove bundle from remote.  If human_name provided, remove the latest.
        This call uses the local index (sqlite db) to first find the uuid's and then
        issue the remove.  If the uuid is provided, rmr will remove from the remote even
        if it doesn't exist locally.

        Default: remove latest
        rm_old_only: remove everything except most recent
        rm_all: remove everything

        Args:
            human_name:  The human-given name for this hyperframe / bundle
            uuid (str): Remove the particular bundle
            tags (:dict):   A dict of (key,value) to find bundles
            dryrun (bool): Do not remove any bundles, only print out what would be removed
            data_context (`disdat.data_context.DataContext`): Context for particular removal

        Returns:
            results (list:str):  List of strings of removed bundles
        """
        return_strings = []

        data_context = self.ensure_data_context(data_context)
        if data_context is None:
            return return_strings

        return_strings.append("Disdat Context: {}".format(data_context.context))

        uuids = self.ls(human_name,
                        False, False, False, False, False,
                        uuid=uuid,
                        tags=tags,
                        print_uuid_only=True,
                        data_context=data_context)

        local_uuids_found = len(uuids)

        # if a specific uuid, it may not be found locally.
        if uuid is not None:
            uuids = [uuid,]

        if dryrun:
            if len(uuids) == 0:
                return_strings.append("Remove remote found no bundles to remove")
            for id in uuids:
                return_strings.append("Remove remote dryrun uuid {}".format(id))
            return return_strings

        try:
            s3_urls = [os.path.join(data_context.get_remote_object_dir(), id) for id in uuids]
            return_strings.append("Found {} local bundles to remove.".format(local_uuids_found))
            num_objects_deleted = aws_s3.delete_s3_dir_many(s3_urls)
            return_strings.append("Removed {} remote bundles from S3.".format(num_objects_deleted))
        except Exception as e:
            return_strings.append("Remote remote ERROR: {} ".format(e))
            #_logger.error(e)
        return return_strings

    def rm(self, human_name=None, rm_all=False, rm_old_only=False, uuid=None, tags=None, force=False, data_context=None):
        """
        Remove bundle with human_name or tag_value

        Default: remove latest
        rm_old_only: remove everything except most recent
        rm_all: remove everything

        Args:
            human_name:  The human-given name for this hyperframe / bundle
            rm_all:      Remove all the bundles matching name, tags.
            rm_old_only: Remove everything but the latest bundle matching name, tags
            uuid (str): Remove the particular bundle
            tags (:dict):   A dict of (key,value) to find bundles
            force (bool): If a committed bundle has a db link backing a view, you have to force removal.
            data_context (`disdat.data_context.DataContext`): Context for particular removal

        Returns:
            results (list:str):  List of strings of removed bundles
        """
        return_strings = []

        data_context = self.ensure_data_context(data_context)
        if data_context is None:
            return return_strings

        return_strings.append("Disdat Context {}".format(data_context.get_remote_name()))
        return_strings.append("On local branch {}".format(data_context.get_local_name()))

        hfrs = data_context.get_hframes(human_name=human_name, uuid=uuid, tags=tags)

        if len(hfrs) == 0:
            return_strings.append("No bundles to remove.")
            return return_strings

        if rm_old_only or rm_all:
            for hfr in hfrs[1:]:
                if data_context.rm_hframe(hfr.pb.uuid, force=force):
                    return_strings.append("Removing old bundle {}".format(hfr.to_string()))

        if not rm_old_only:
            if data_context.rm_hframe(hfrs[0].pb.uuid, force=force):
                return_strings.append("Removing latest bundle {}".format(hfrs[0].to_string()))

        return return_strings

    def get_latest_hframe(self, human_name, tags=None, getall=False, data_context=None):
        """
        Given bundle_name, what is the most recent one (by date created) in this context?

        Args:
            human_name (str):
            tags (:dict):
            getall:
            data_context (`disdat.data_context.DataContext`): Optional data context from which to source hframe

        Returns:
            None or (`hyperframe.HyperFrameRecord`): None or latest hframe
        """

        data_context = self.ensure_data_context(data_context)
        if data_context is None:
            raise Exception("No current context")

        found = data_context.get_hframes(human_name=human_name, tags=tags)

        # TODO: filter at SQL instead
        if len(found) > 0:
            if getall:
                return found
            else:
                return found[0]
        else:
            return None

    def get_hframe_by_uuid(self, uuid, tags=None, data_context=None):
        """
        Given uuid, get object
        Args:
            uuid:
            tags (:dict):
            data_context (`disdat.data_context.DataContext`): Optional data context from which to source hframe

        Returns:
            `hyperframe.HyperFrameRecord`:
        """

        data_context = self.ensure_data_context(data_context)
        if data_context is None:
            raise Exception("No current context")

        found = data_context.get_hframes(uuid=uuid, tags=tags)

        if len(found) == 1:
            return found[0]
        elif len(found) == 0:
            return None
        else:
            raise Exception("Many records {} found with uuid {}".format(len(found), uuid))

    def get_hframe_by_proc(self, processing_name, getall=False, data_context=None):
        """
        Given processing_name find Hyper Frame (aka bundle).  Return the most recent (latest)
        hframe by processing, unless

        NOTE: We can have more than one hyperframe for a single processing_name.  This occurs
        when you call 'dsdt add' for example.   Add forces a re-execution, as does 'dsdt apply --force'.
        In these cases the arguments may be identical, the processing_name identical, the human_name identical, etc.


        Args:
            processing_name:
            getall: Retrieve all the frames that share that processing ID
            data_context: The context from which to find the hframe.  If None, then use current one.

        Returns:
            None or HyperFrameRecord
        """

        data_context = self.ensure_data_context(data_context)
        if data_context is None:
            raise Exception("No current context")

        found = data_context.get_hframes(processing_name=processing_name)

        # TODO: filter at SQL instead
        if len(found) > 0:
            if getall:
                return found
            else:
                return found[0]
        else:
            return None

    def ensure_data_context(self, data_context):
        """
        Check if there's a valid data context,
        and if not, get default, if still none, print out and return none.

        Args:
            data_context:

        Returns:

        """
        if data_context is None:
            data_context = self.curr_context
            if data_context is None:
                print("No current context.  `dsdt switch <othercontext>`")
                return None
        return data_context

    def ls(self, search_name, print_tags, print_intermediates, print_roots, print_long, print_args,
           before=None, after=None, uuid=None, maxbydate=False, committed=None, tags=None, print_uuid_only=False,
           data_context=None):
        """
        Enumerate bundles (hyperframes) in this context.

        Args:
            search_name: May be None.  Interpret as a simple regex (one kleene star)
            print_tags (bool): Whether to print the bundle tags
            print_intermediates (bool): Show only intermediate bundles
            print_roots (bool): Show only root bundles
            print_long (bool): Display long header
            print_args (bool): Whether to print the arguments used to produce this bundle
            before (date.datetime): '01-03-19 02:40:37' or date '01-03-19' inclusive range
            after (date.datetime): '01-03-19 02:40:37' or date '01-03-19' inclusive range
            uuid (str): A specific UUID to list.  Trumps all other options.
            maxbydate (bool): return the latest by date
            committed (bool): If True, just committed, if False, just uncommitted, if None then ignore.
            tags: Optional. A dictionary of tags to search for.
            print_uuid_only (bool):  Return only a list of UUIDs (internal use)
            data_context (`disdat.data_context.DataContext`): Optional data context to operate in

        Returns:

        """

        output_strings = []

        data_context = self.ensure_data_context(data_context)
        if data_context is None:
            return output_strings

        # Print roots if
        # not print_intermediates and print_roots: just print roots
        # not print_intermediates and not print_roots: print everything
        # print_intermediates and print_roots: print everything
        # print_intermediates and not print_roots: just intermediates

        if not print_intermediates and print_roots:
            if tags is not None:
                tags.update({'root_task': True})
            else:
                tags = {'root_task': True}

        if print_long:
            output_strings.append(DisdatFS._pretty_print_header())

        for i, r in enumerate(data_context.get_hframes(human_name=search_name,
                                                       uuid=uuid,
                                                       tags=tags,
                                                       maxbydate=maxbydate,
                                                       before=before,
                                                       after=after)):
            if committed is not None:
                if committed:
                    if not r.get_tag('committed'):
                        continue
                else:
                    if r.get_tag('committed'):
                        continue

            if print_intermediates and not print_roots:
                if r.get_tag('root_task'):
                    continue

            if print_uuid_only:
                output_strings.append(r.pb.uuid)
            else:
                if print_long:
                    output_strings.append(DisdatFS._pretty_print_hframe(r, print_tags=print_tags, print_args=print_args))
                else:
                    if r.pb.human_name not in output_strings:
                        output_strings.append(r.pb.human_name)
        return output_strings

    @staticmethod
    def _pretty_print_header():
        header = "{:20}\t{:20}\t{:8}\t{:18}\t{:8}\t{:40}\t{}".format('NAME','PROC_NAME','OWNER','DATE','COMMITTED','UUID','TAGS')
        return header

    @staticmethod
    def _pretty_print_hframe(hfr, print_tags=False, print_args=False):

        if 'committed' in hfr.tag_dict:
            committed = 'True'
        else:
            committed = 'False'

        output_string = "{:20}\t{:20}\t{:8}\t{:18}\t{:8}\t{:40}".format(hfr.pb.human_name,
                                                                   hfr.pb.processing_name[:],
                                                                   hfr.pb.owner,
                                                                   time.strftime("%m-%d-%y %H:%M:%S ", time.localtime(hfr.pb.lineage.creation_date)),
                                                                   committed,
                                                                   hfr.pb.uuid)
        if print_tags:
            tags = ["[{}]:[{}]".format(k, v) for k, v in hfr.tag_dict.items() if k != 'committed' and common.BUNDLE_TAG_PARAMS_PREFIX not in k]
            output_string += ' '.join(tags)

        if print_args:
            tags = ["[{}]:[{}]".format(k[len(common.BUNDLE_TAG_PARAMS_PREFIX):], v)
                    for k, v in hfr.tag_dict.items() if common.BUNDLE_TAG_PARAMS_PREFIX in k]
            if len(tags) > 0:
                output_string += '\n\t ARGS: ' + ' '.join(tags)

        return output_string

    def cat(self, human_name, uuid=None, tags=None, file=None, data_context=None):
        """
        Given a bundle name and optional uuid, return the object that was saved in the bundle

        Args:
            human_name (str): The bundle name
            uuid (str):  The bundle UUID
            tags (:dict): A dictionary of `str`:`str`
            file (str): An optional output file to which to write this bundle.
            data_context (`disdat.data_context.DataContext`):

        Returns:
            (`DataFrame`) or (`numpy.ndarray`) or scalar type
        """

        data_context = self.ensure_data_context(data_context)
        if data_context is None:
            raise CatNoBundleError("No bundle found with name({}) uuid({})".format(human_name,uuid))

        if uuid is None:
            hfr = self.get_latest_hframe(human_name, tags=tags, data_context=data_context)
        else:
            hfr = self.get_hframe_by_uuid(uuid, tags=tags, data_context=data_context)

        if hfr is not None:
            other = data_context.present_hfr(hfr)
            if file is not None:
                df    = data_context.convert_hfr2df(hfr)
                print("Saving to file {}".format(file))
                df.to_csv(file, sep=',', index=False)
            return other
        else:
            raise CatNoBundleError("No bundle found with name({}) uuid({})".format(human_name,uuid))

    @staticmethod
    def _extract_uuid(managed_path):
        """
        Given a managed path, extract the uuid from it.
        It should ALWAYS be the second to last entry.

        :return: a local directory in the current context or s3 path
        """

        return os.path.split(os.path.dirname(managed_path))[1]

    @staticmethod
    def disdat_uuid():
        """
        """
        return common.create_uuid()

    @staticmethod
    def is_bundle_name(possible_bundle_name):
        """
        Determine if the syntax is <something.something>
        Just make sure there are two parts with one period.
        :param possible_bundle_name:
        :return: True / False
        """
        return len(possible_bundle_name.split('.')) == 2

    @staticmethod
    def is_input_param_bundle_name(possible_bundle_name):
        """
        Determine if the syntax is <something.something>
        Just make sure there are two parts with one period.

        input parameters might be literal strings.  So require
        that the the bundle name ends with .BNDL

        :param possible_bundle_name:
        :return: True / False
        """

        first  = DisdatFS.is_bundle_name(possible_bundle_name)
        second = possible_bundle_name.endswith(".BNDL")

        return first and second

    @staticmethod
    def _parse_fq_context_name(fq_context_name):
        """
        return repo, context_name
        Args:
            fq_context_name:

        Returns:
            (str,str)
        """

        try:
            if '/' in fq_context_name:
                repo, local_context = fq_context_name.split('/')
            else:
                repo = None
                local_context = fq_context_name
        except ValueError:
            error = "Invalid context_name: Expected <remote context>/<local context> or <local context> but got '%s'" % (fq_context_name)
            _logger.error(error)
            raise Exception(error)

        return repo, local_context

    def branch(self, fq_context_name):
        """

        Create a new context from <remote_context>/<context_name> or <context_name>
        Create a new local directory with this local context name.

        Args:
            fq_context_name:  The unique string for a context

        Returns:
            (int): 0 if context does not exist, 1 if context already exists

        """

        if fq_context_name is None:
            from termcolor import cprint
            for ctxt_name, ctxt in self._all_contexts.items():
                if self.curr_context is not None and self.curr_context is ctxt:
                    cprint("*", "white", end='')
                    cprint("\t{}".format(ctxt_name), "green", end='')
                    cprint("\t[{}@{}]".format(ctxt.remote_ctxt,
                                              DataContext.s3_remote_from_url(self.curr_context.remote_ctxt_url)))
                else:
                    print("\t{}\t[{}@{}]".format(ctxt.local_ctxt, ctxt.remote_ctxt,
                                                 DataContext.s3_remote_from_url(ctxt.remote_ctxt_url)))
            return 0

        remote_context, local_context = DisdatFS._parse_fq_context_name(fq_context_name)

        context_dir = DisdatConfig.instance().get_context_dir()

        ctxt_dir = os.path.join(context_dir, local_context)

        if local_context in self._all_contexts:
            assert(os.path.exists(ctxt_dir))
            _logger.info("The context '{}' already exists.".format(local_context))
            return 1

        DataContext.create_branch(context_dir, local_context)

        context = DataContext(context_dir,
                              remote_ctxt=remote_context,
                              local_ctxt=local_context,
                              remote_ctxt_url=None)

        context.save()

        self._all_contexts[local_context] = context

        _logger.info("Disdat created data context {}/{} at object dir {}.".format(remote_context,
                                                                                  local_context,
                                                                                  context.get_object_dir()))
        return 0

    def delete_context(self, fq_context_name, remote, force):
        """

        Delete a branch at <repo>/<context_name> or <context name>

        Args:
            fq_context_name:  The unique string for a context
            remote: whether to also remove the remote on S3
            force: whether to force delete a dirty context

        Returns:

        """
        repo, local_context = DisdatFS._parse_fq_context_name(fq_context_name)

        ctxt_dir = os.path.join(DisdatConfig.instance().get_context_dir(), local_context)

        if self.curr_context is not None and (fq_context_name == self.curr_context_name):
            print("Disdat deleting the current context {}, remember to 'dsdt switch <otherbranch>' afterwords!".format(fq_context_name))
            os.remove(os.path.join(DisdatConfig.instance().get_meta_dir(), META_FS_FILE))

        if local_context in self._all_contexts:
            dc = self._all_contexts[local_context]
            remote_context_url = dc.get_remote_object_dir()
            dc.delete_context(force=force)
            del self._all_contexts[local_context]

        if os.path.exists(ctxt_dir):
            shutil.rmtree(ctxt_dir)
            _logger.info("Disdat deleted local data context {}.".format(local_context))
            if remote:
                aws_s3.delete_s3_dir(remote_context_url)
                _logger.info("Disdat deleted remote data context {}.".format(remote_context_url))
        else:
            _logger.info("Disdat local data context {} appears to already have been deleted.".format(local_context))

    def get_context(self, local_context_name):
        """
        Return the context object for a given context name

        Args:
            local_context_name (str): May be <remote context>/<local context> or <local context>

        Returns:
            `disdat.data_context.DataContext`: the data context or None if not found.

        """

        repo, local_context = DisdatFS._parse_fq_context_name(local_context_name)

        if local_context not in self._all_contexts:
            _logger.error("Context {} not found.  Please create the context locally.".format(local_context, local_context))
            return None

        return self._all_contexts[local_context]

    def switch(self, local_context_name):
        """
        Switch to a different local context.

        Args:
            local_context_name (str): May be <remote context>/<local context> or <local context>
            save (bool): Whether to record context change on disk.
        Returns:

        """
        assert local_context_name is not None

        repo, local_context = DisdatFS._parse_fq_context_name(local_context_name)

        if self.curr_context is not None and self.curr_context_name == local_context_name:
            assert(local_context in self._all_contexts)
            assert(self.curr_context == self._all_contexts[local_context_name])
            _logger.info("Disdat already within a valid data context_name {}".format(local_context))

        new_context = self.get_context(local_context_name)

        if new_context is not None:
            self.curr_context = new_context
            print("Switched to context {}".format(self.curr_context_name))
        else:
            print("In context {}".format(self.curr_context_name))

    def commit(self, bundle_name, input_tags, uuid=None, data_context=None):
        """   Commit indicates that this is a primary version of this bundle.

        Commit in place.  Re-use existing bundle and add the commit tag.
        Database links are special.  Commits materialize special views of the physical table.

        Args:
            bundle_name (str): The name of the bundle to commit.  Ignored if uuid is set.
            input_tags (dict): Commit the bundle that has these tags
            uuid (str): The uuid of the bundle to commit.
            data_context (`disdat.data_context.DataContext`): Optional data context in which to find / commit bundle.

        Returns:
            None

        """

        data_context = self.ensure_data_context(data_context)
        if data_context is None:
            return

        _logger.debug('Committing bundle {} in context {}'.format(bundle_name, data_context.get_remote_name()))

        if uuid is not None:
            hfr = self.get_hframe_by_uuid(uuid,
                                          tags=input_tags,
                                          data_context=data_context)
        elif bundle_name is not None:
            hfr = self.get_latest_hframe(str(bundle_name),
                                         tags=input_tags if len(input_tags) > 0 else None,
                                         data_context=data_context)
        else:
            print("Push requires either a human name or a uuid to identify the hyperframe.")
            return None

        if hfr is None:
            print("No bundle with human name [{}] or uuid [{}] found.".format(bundle_name, uuid))
            return

        commit_tag = hfr.get_tag('committed')
        if commit_tag is not None and commit_tag == 'True':
            print("Bundle human name [{}] uuid [{}] already committed.".format(hfr.pb.human_name, hfr.pb.uuid))
            return

        tags = {'committed': 'True'}

        # Commit in memory:
        existing_tags = hfr.get_tags()
        existing_tags.update(tags)
        hfr.replace_tags(existing_tags)

        # Commit DBTarget links if present:
        data_context.commit_db_links(hfr)

        # Commit to disk:
        data_context.atomic_update_hframe(hfr)

    def _get_all_link_frames(self, outer_hfr, local_fs_frames=False, s3_frames=False, db_frames=False):
        """
        To push a hyperframe, we need to see if it contains any local file link frames.
        This is within the entire structure, not just at the top level.
        To do so we have to descend through all the HyperFrame frames as they are found.
        Here we return tuples (containing_hfr, link_frame)

        Args:
            outer_hfr:  The hyperframe to look within.
            local_fs_frames:  Return link frames holding local files
            s3_frames:        Return link frames holding s3 files
            db_frames    Return link frames holding db tables

        Returns:
            [list: tuple: (hyperframe.HyperFrameRecord, hyperframe.FrameRecord)]:  List of tuples containing hyperframe
            and a link FrameRecord.

        """

        hf_frontier = [outer_hfr, ]
        found_link_frames = []

        while len(hf_frontier) > 0:

            next_hf = hf_frontier.pop()

            for fr in next_hf.get_frames(self.curr_context):

                if local_fs_frames:
                    if fr.is_local_fs_link_frame():
                        found_link_frames.append( (next_hf, fr) )

                if s3_frames:
                    if fr.is_s3_link_frame():
                        found_link_frames.append( (next_hf, fr) )

                if db_frames:
                    if fr.is_db_link_frame():
                        found_link_frames.append( (next_hf, fr) )

                if fr.is_hfr_frame():
                    for hfr in fr.get_hframes(): # making a copy from the pb in this frame
                        hf_frontier.append(hfr)

        return found_link_frames

    def _copy_hfr_to_branch(self, hfr, data_context, to_remote=True):
        """
        Copy this HyperFrameRecord to a different branch.  Note that this works because
        we use relative Hyperframes (Link URLs have no location specific prefix).  If we
        happen to be re-binding to a new remote,

        NOTE: This copies the external files and the pb's.

        Copy top-level HFR, copy all Frames.
        If Frame is local FS -- copy_in to new directory
        If Frame is local FS and to_remote -- Make correct s3 paths, push files to s3
        If Frame is s3 -- Copy_in files to new s3 path and fix path (optimize in future)
        If Frame is db -- Do nothing

        Args:
            hfr: The hyperframe
            data_context: The data context to copy from
            to_remote (bool): Optional.  Write to the remote on the current context. Default true.

        Returns:
            None
        """

        assert data_context is not None

        for fr in hfr.get_frames(data_context):

            if fr.is_hfr_frame():
                # CASE 1: A frame containing HFRs.   Descend recursively.
                for next_hfr in fr.get_hframes():
                    self._copy_hfr_to_branch(next_hfr, data_context, to_remote=to_remote)
            else:
                # CASE 2:  If it is a local fs or an s3 frame, then we have to copy
                if to_remote:
                    obj_dir = data_context.get_remote_object_dir()
                else:
                    obj_dir = data_context.get_object_dir()
                self._copy_fr_links_to_branch(fr, obj_dir, data_context)

        # Push hyperframe to remote
        data_context.write_hframe(hfr, to_remote=to_remote)

        return

    @staticmethod
    def _copy_fr_links_to_branch(fr, branch_object_dir, data_context):
        """
        Given a frame, if a local fs or s3 frame, do the
        copy_in to this branch.

        NOTE: similar to _copy_fr() except we do not make a copy of the fr.

        Args:
            fr:  Frame to possibly copy_in files to managed_path
            branch_object_dir: s3:// or file:/// path of the object directory on the branch
            data_context: The context from which to copy.

        Returns:
            None
        """
        assert data_context is not None

        if fr.is_local_fs_link_frame() or fr.is_s3_link_frame():
            src_paths = data_context.actualize_link_urls(fr)
            bundle_dir = os.path.join(branch_object_dir, fr.hframe_uuid)
            _ = data_context.copy_in_files(src_paths, bundle_dir)
        return

    def push(self, human_name=None, uuid=None, tags=None, data_context=None):
        """

        Push a particular hyperframe to our remote context.   This only pushes the most recent (in time) version of
        the hyperframe.  It does not look for committed hyperframes (that's v2).

        If current context is bound, copy bundle / files to s3, updating link frames to point to new paths.
        Assumes s3 paths have already been sanitized (point to files in our context)

        NOTE: we push the most recent hyperframe unless the UUID is specified.  More complicated filters for future
        work.

        NOTE: Only push committed bundles.  If no committed tag, then will not push.

        TODO:  Currently we copy S3 files even if they are already within a frame in a context.

        Args:
            human_name (str): The name of this bundle
            uuid (str) : Uniquely identify the bundle to push.
            tags (:dict): Set of tags bundle must have
            data_context (`disdat.data_context.DataContext`): Optional data context in which to find / commit bundle.

        Returns:
            (`hyperframe.HyperFrameRecord`): The, possibly new, pushed hyperframe.

        """

        data_context = self.ensure_data_context(data_context)
        if data_context is None:
            return None

        if data_context.remote_ctxt_url is None:
            print("Push cannot execute.  Local context {} on remote {} not bound.".format(data_context.local_ctxt,
                                                                                          data_context.remote_ctxt))
            return None

        if tags is None:
            tags = {}

        tags['committed'] = 'True'

        if uuid is not None:
            hfr = self.get_hframe_by_uuid(uuid,
                                          tags=tags,
                                          data_context=data_context)
        elif human_name is not None:
            hfr = self.get_latest_hframe(human_name,
                                         tags=tags,
                                         data_context=data_context)
        else:
            print("Push requires either a human name or a uuid to identify the hyperframe.")
            return None

        if hfr is None:
            print("Push unable to find committed bundle name [{}] uuid [{}]".format(human_name, uuid))
            return None

        # All bundles contain relative paths.  Copying is a simple
        # recursive process that copies files and protobufs to the remote.
        try:
            self._copy_hfr_to_branch(hfr, data_context, to_remote=True)
        except Exception as e:
            print("Push unable to copy bundle to branch: {}".format(e))
            return None

        print("Pushed committed bundle {} uuid {} to remote {}".format(human_name, hfr.pb.uuid,
                                                                       data_context.remote_ctxt_url))

        return hfr

    @staticmethod
    def _localize_hfr(local_hfr, s3_uuid, data_context):
        """
        Given local hfr, read link frames and pull data from s3.

        Args:
            local_hfr:
            s3_uuid:
            data_context

        Returns:
            None
        """
        managed_path = os.path.join(data_context.get_object_dir(), s3_uuid)
        for fr in local_hfr.get_frames(data_context):
            if fr.is_link_frame():
                src_paths = data_context.actualize_link_urls(fr)
                for f in src_paths:
                    data_context.copy_in_files(f, managed_path)

    @staticmethod
    def fast_pull(data_context, localize):
        """

        First edition, over-write everything.
        Next edition, by smarter.  Basically implement "sync"

        Args:
            data_context:
            localize (bool): If True pull all files in each bundle, else just pull *frame.pb

        Returns:

        """
        _logger.info("Fast Pull synchronizing with remote context {}@{}".format(data_context.remote_ctxt,
                                                                                data_context.remote_ctxt_url))

        remote_s3_object_dir = data_context.get_remote_object_dir()
        s3_bucket, remote_obj_dir = aws_s3.split_s3_url(remote_s3_object_dir)
        all_keys = aws_s3.ls_s3_url_keys(remote_s3_object_dir,
                                         is_object_directory=data_context.bundle_count() > aws_s3.S3_LS_USE_MP_THRESH)
        if not localize:
            all_keys = [k for k in all_keys if 'frame.pb' in k]
        fetch_count = 0
        fetch_tuples = []
        for s3_key in all_keys:
            obj_basename = os.path.basename(s3_key)
            obj_suffix = s3_key.replace(remote_obj_dir,'')
            obj_suffix_dir = os.path.dirname(obj_suffix).strip('/')  # remote_obj_dir won't have a trailing slash
            local_uuid_dir = os.path.join(data_context.get_object_dir(), obj_suffix_dir)
            local_object_path = os.path.join(local_uuid_dir, obj_basename)
            if not os.path.exists(local_object_path):
                fetch_count += 1
                fetch_tuples.append((s3_bucket, s3_key, local_object_path))
        _logger.info("Fast pull fetching {} objects...".format(fetch_count))
        results = aws_s3.get_s3_key_many(fetch_tuples)
        _logger.info("Fast pull completed {} transfers -- process pool closed and joined.".format(len(results)))

    def pull(self, human_name=None, uuid=None, localize=False, data_context=None):
        """
        Either pull in any versions of a particular object, or update all
        objects.   There is no DB at a remote.  We are left to reading the
        entire directory.  We leverage s3 facilities to give us all the
        hyperframe pb's within the current bound context.

        Args:
            hfr: optional filter
            human_name:
            uuid:
            localize: Whether to download the files in this bundle locally
            data_context (`disdat.data_context.DataContext`): Optional data context from which to pull bundle.

        Returns:
            None

        Raise:
            UserWarning: If we are not in a valid context.
        """

        data_context = self.ensure_data_context(data_context)
        if data_context is None:
            return

        if data_context.remote_ctxt_url is None:
            print("Pull cannot execute.  Local context {} on remote {} not bound.".format(data_context.local_ctxt, data_context.remote_ctxt))
            raise UserWarning("Local context {} has no remote".format(data_context.local_ctxt))

        if human_name is None and uuid is None:
            # NOTE: This is fast and loose.  Another command might be editing the db.  Unit test.
            self.fast_pull(data_context, localize)
            data_context.rebuild_db()
            return

        if uuid is not None:
            local_hfr = self.get_hframe_by_uuid(uuid, data_context=data_context)
            if local_hfr is not None:
                if localize:
                    # TODO: Need fast check to see if it is already localized!
                    DisdatFS._localize_hfr(local_hfr, uuid, data_context)
                return
            # else fall through to see if we can pull from remote context

        # If uuid is supplied, just read objects at that key, not all the objects in the context
        if uuid is not None:
            object_directory = os.path.join(data_context.get_remote_object_dir(), uuid)
        else:
            object_directory = data_context.get_remote_object_dir()

        possible_hframe_objects = aws_s3.ls_s3_url_keys(object_directory,
                                                        is_object_directory=data_context.bundle_count() > aws_s3.S3_LS_USE_MP_THRESH)

        hframe_keys = [obj for obj in possible_hframe_objects if '_hframe.pb' in obj]

        for s3_hfr_key in hframe_keys:
            hfr_basename = os.path.basename(s3_hfr_key)
            # Note that this works because the UUID is prepended to the <uuid>_hframe.pb
            s3_uuid = hfr_basename.split('_')[0]

            if uuid is not None:   # filter by UUID
                if s3_uuid != uuid:
                    continue
                else:
                    print("Found remote bundle with UUID {}, checking local context for duplicates ...".format(uuid))

            local_hfr = self.get_hframe_by_uuid(s3_uuid, data_context=data_context)
            if local_hfr is not None:
                if not localize:
                    print("Found HyperFrame UUID {} present in local context, skipping . . .".format(s3_uuid))
                else:
                    if human_name is not None:
                        if human_name != local_hfr.pb.human_name:
                            continue
                        else:
                            print("Found remote bundle with human name {}, uuid {} localizing ...".format(local_hfr.pb.human_name,
                                                                                                          local_hfr.pb.uuid))
                    DisdatFS._localize_hfr(local_hfr, s3_uuid, data_context)
            else:
                s3_bucket, _ = aws_s3.split_s3_url(data_context.remote_ctxt_url)
                found_objects = aws_s3.s3_list_objects_at_prefix(s3_bucket, s3_hfr_key)
                assert len(found_objects) == 1
                obj = found_objects[0].Object().get()
                hfr_test = hyperframe.HyperFrameRecord.from_str_bytes(obj['Body'].read())
                if human_name is not None:
                    if human_name != hfr_test.pb.human_name:
                        continue
                    else:
                        print("Found remote bundle with human name {}, uuid {} ...".format(hfr_test.pb.human_name,
                                                                                           hfr_test.pb.uuid))

                _logger.info("Adding HyperFrame UUID {} to local context . . .".format(s3_uuid))

                local_uuid_dir = os.path.join(data_context.get_object_dir(), s3_uuid)
                local_hfr_path = os.path.join(local_uuid_dir, hfr_basename)
                if os.path.exists(local_uuid_dir):
                    print("Pull found existing data in local disdat db at UUID {}, overwriting . . .".format(s3_uuid))
                    shutil.rmtree(local_uuid_dir)

                os.makedirs(local_uuid_dir)

                hyperframe.w_pb_fs(None, hfr_test, local_hfr_path)

                # grab frames for this hyperframe
                s3_hfr_dir = os.path.join(data_context.get_remote_object_dir(), s3_uuid)
                possible_frame_objects = aws_s3.ls_s3_url_objects(s3_hfr_dir)
                frame_objects = [obj for obj in possible_frame_objects if '_frame.pb' in obj.key]
                for s3_fr_obj in frame_objects:
                    fr_basename = os.path.basename(s3_fr_obj.key)
                    local_fr_path = os.path.join(local_uuid_dir, fr_basename)
                    s3_fr_obj.Object().download_file(local_fr_path)

                data_context.write_hframe_db_only(hfr_test)

                if localize:
                    DisdatFS._localize_hfr(self.get_hframe_by_uuid(s3_uuid, data_context=data_context),
                                           s3_uuid, data_context)

    def remote_add(self, remote_context, s3_url):
        """
        Bind the context name to this s3 url.

        Args:
            remote_context (str):  the name of the remote context
            s3_url (str):   the s3 path to bind to this remote context

        Returns:
            None
        """

        ctxt_obj = self.curr_context

        assert ctxt_obj is not None, "Disdat must be in a context to use 'remote'"

        ctxt_obj.bind_remote_ctxt(remote_context, s3_url)


def _branch(fs, args):
    if args.delete:
        fs.delete_context(args.context, args.remote, args.force)
    else:
        fs.branch(args.context)


def _commit(fs, args):
    fs.commit(args.bundle, common.parse_args_tags(args.tag), uuid=args.uuid)


def _remote(fs, args):
    fs.remote_add(args.context, args.s3_url)


def _push(fs, args):
    bundle = None
    uuid   = None
    if args.bundle:
        bundle = args.bundle
    if args.uuid:
        uuid = args.uuid

    fs.push(bundle, uuid, tags=common.parse_args_tags(args.tag))


def _pull(fs, args):
    bundle = None
    uuid   = None
    if args.bundle:
        bundle = args.bundle
    if args.uuid:
        uuid = args.uuid

    fs.pull(bundle, uuid, localize=args.localize)


def _rm(fs, args):
    for f in fs.rm(args.bundle, rm_all=args.all, rm_old_only=args.old, tags=common.parse_args_tags(args.tag), uuid=args.uuid, force=args.force):
        print(f)


def _rmr(fs, args):
    for f in fs.rmr(args.bundle, tags=common.parse_args_tags(args.tag), uuid=args.uuid, dryrun=args.dryrun):
        print(f)


def _parse_date(date_string, throw=False):
    """

    NOTE: Also used in api.py

    Args:
        date_string (str): String we want to parse into a datetime object
        throw (bool): Raise exception instead of returning None on error

    Returns:
        datetime.datetime
    """
    try:
        if len(date_string.split(' ')) > 1:
            date = datetime.strptime(date_string, "%m-%d-%Y %X")
        else:
            date = datetime.strptime(date_string, "%m-%d-%Y")
    except ValueError as ve:
        print("Unable to parse date, must be like '12-1-2008' or '\"12-1-2008 13:12:05\"'")
        if not throw:
            return None
        else:
            raise
    return date


def _ls(fs, args):
    if len(args.bundle) > 1:
        print("dsdt ls takes zero or one bundle as arguments.")
        return

    arg = None
    if len(args.bundle) == 1:
        arg = args.bundle[0]

    committed = None
    if args.committed:
        committed = True
    elif args.uncommitted:
        committed = False

    after = None
    if args.after:
        after = _parse_date(args.after)
        if after is None:
            return

    before = None
    if args.before:
        before = _parse_date(args.before)
        if before is None:
            return

    for f in fs.ls(arg,
                   args.print_tags,
                   args.intermediates,
                   args.roots,
                   args.verbose,
                   args.print_args,
                   uuid=args.uuid,
                   committed=committed,
                   before=before,
                   after=after,
                   maxbydate=args.latest_by_date,
                   tags=common.parse_args_tags(args.tag)):
        print(f)


def _cat(fs, args):
    try:
        result = fs.cat(args.bundle, uuid=args.uuid, tags=common.parse_args_tags(args.tag), file=args.file)
        if isinstance(result, pd.DataFrame):
            # If df, make sure we print out all columns
            pd.set_option('display.max_colwidth', -1)
            print(result.to_string())
        else:
            # else default print the object
            print(result)
    except CatNoBundleError as cnbe:
        print("Disdat cat found no bundle with name {} or uuid {}".format(args.bundle, args.uuid))


def add_arg_parser(subparsers):
    """Initialize a command line set of subparsers with file system commands.

    Args:
        subparsers: A collection of subparsers as defined by `argsparse`.
    """
    fs = DisdatFS()

    # context
    context_p = subparsers.add_parser('context')
    context_p.add_argument('-f', '--force', action='store_true', help='Force remove of a dirty local context')
    context_p.add_argument('-d','--delete', action='store_true', help='Delete local context')
    context_p.add_argument('-r','--remote', action='store_true', help='Delete remote context along with local context')
    context_p.add_argument(
        'context',
        nargs='?',
        type=str,
        help="Create a new data context using <remote context>/<local context> or <local context>")
    context_p.set_defaults(func=lambda args: _branch(fs, args))

    # switch contexts
    switch_p = subparsers.add_parser('switch')
    switch_p.add_argument(
        'context',
        type=str,
        help='Switch contexts to "<local context>".')
    switch_p.set_defaults(func=lambda args: fs.switch(args.context))

    # commit
    commit_p = subparsers.add_parser('commit', description='Commit most recent bundle of name <bundle>.')
    commit_p.add_argument('bundle', type=str, nargs='?', default=None,
                          help='Bundle name to commit in the current context (optional)')
    commit_p.add_argument('-u', '--uuid', type=str, default=None, help='Bundle UUID to commit')
    commit_p.add_argument('-t', '--tag', nargs=1, type=str, action='append',
                          help="Having a specific tag: 'dsdt rm -t committed:True -t version:0.7.1'")
    commit_p.set_defaults(func=lambda args: _commit(fs, args))

    # rm
    rm_p = subparsers.add_parser('rm')
    rm_p.add_argument('bundle', nargs='?', type=str, default=None, help='The bundle in the current context')
    rm_p.add_argument('-f', '--force', action='store_true', default=False, help='Force remove of a committed bundle')
    rm_p.add_argument('-u', '--uuid', type=str, default=None, help='Bundle UUID to remove')
    rm_p.add_argument('-t', '--tag', nargs=1, type=str, action='append',
                      help="Having a specific tag: 'dsdt rm -t committed:True -t version:0.7.1'")
    rm_p.add_argument('--all', action='store_true',
                      help='Remove the current version and all history.  Otherwise just remove history')
    rm_p.add_argument('--old', action='store_true', default=False,
                      help='Remove everything except the most recent bundle.')
    rm_p.set_defaults(func=lambda args: _rm(fs, args))

    # rmr remove from remote
    rmr_p = subparsers.add_parser('rmr')
    rmr_p.add_argument('bundle', nargs='?', type=str, default=None, help='The bundle in the current context')
    rmr_p.add_argument('--dryrun', action='store_true', default=False, help='Do not delete found bundles')
    rmr_p.add_argument('-u', '--uuid', type=str, default=None, help='Bundle UUID to remove')
    rmr_p.add_argument('-t', '--tag', nargs=1, type=str, action='append',
                       help="Having a specific tag: 'dsdt rm -t committed:True -t version:0.7.1'")
    rmr_p.set_defaults(func=lambda args: _rmr(fs, args))

    # ls
    ls_p = subparsers.add_parser('ls')
    ls_p.add_argument('bundle', nargs='*', type=str, help="Show all bundles 'dsdt ls' or explicit bundle 'dsdt ls <somebundle>' in current context")
    ls_p.add_argument('-a', '--print-args', action='store_true', help="Print the arguments (if any) used to create the bundle.")
    ls_p.add_argument('-p', '--print-tags', action='store_true', help="Print each bundle's tags.")
    ls_p.add_argument('-i', '--intermediates', action='store_true',
                      help="List only intermediate outputs.")
    ls_p.add_argument('-r', '--roots', action='store_true',
                      help="List only bundles from root tasks (last task in pipeline).")
    ls_p.add_argument('-c', '--committed', action='store_true',
                      help="List only committed bundles.")
    ls_p.add_argument('-u', '--uuid', type=str, default=None,
                      help='list by bundle UUID')
    ls_p.add_argument('--uncommitted', action='store_true',
                      help="List only uncommitted bundles.")
    ls_p.add_argument('-l', '--latest-by-date', action='store_true',
                      help="Return the most recent bundle for any name.")
    ls_p.add_argument('-A', '--after',  type=str,
                      help="List bundles created on or after date or datetime: '--after 12-10-2008 13:40:30'")
    ls_p.add_argument('-B', '--before', type=str,
                      help="List bundles created on or before date or datetime: '--before 12-10-2008 13:40:30'")
    ls_p.add_argument('-v', '--verbose', action='store_true',
                      help="Print bundles with more information.")
    ls_p.add_argument('-t', '--tag', nargs=1, type=str, action='append',
                      help="Having a specific tag: 'dsdt ls -t committed:True -t version:0.7.1'")
    ls_p.set_defaults(func=lambda args: _ls(fs, args))

    # cat
    cat_p = subparsers.add_parser('cat')
    cat_p.add_argument('bundle', type=str, nargs='?', default=None, help='The bundle name in the current context')
    cat_p.add_argument('-t', '--tag', nargs=1, type=str, action='append',
                       help="Having a specific tag: 'dsdt ls -t committed:True -t version:0.7.1'")
    cat_p.add_argument('-f', '--file', type=str,
                       help="Save output dataframe as csv without index to specified file")
    cat_p.add_argument('-u', '--uuid', type=str, default=None, help='Bundle UUID to cat')
    cat_p.set_defaults(func=lambda args: _cat(fs, args))

    # remote add <name> <s3_url>
    remote_p = subparsers.add_parser('remote')
    remote_p.add_argument('context', type=str, help='Name of the remote context')
    remote_p.add_argument('s3_url', type=str, help="Remote context site, i.e, 's3://<bucket>/dsdt/'")
    remote_p.set_defaults(func=lambda args: _remote(fs, args))

    # push <name> --uuid <uuid>
    push_p = subparsers.add_parser('push')
    push_p.add_argument('bundle', type=str, nargs='?', default=None,
                        help='The bundle name in the current context')
    push_p.add_argument('-u', '--uuid', type=str, help='A UUID of a bundle in the current context')
    push_p.add_argument('-t', '--tag', nargs=1, type=str, action='append',
                        help="Having a specific tag: 'dsdt ls -t committed:True -t version:0.7.1'")
    push_p.set_defaults(func=lambda args: _push(fs, args))

    # pull <name --uuid <uuid>
    pull_p = subparsers.add_parser('pull')
    pull_p.add_argument('bundle', type=str, nargs='?', default=None, help='The bundle name in the current context')
    pull_p.add_argument('-u', '--uuid', type=str, help='A UUID of a bundle in the current context')
    pull_p.add_argument('-l', '--localize', action='store_true', help='Pull files with the bundle.  Default to leaving files at remote.')
    pull_p.set_defaults(func=lambda args: _pull(fs, args))
