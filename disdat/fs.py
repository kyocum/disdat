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
from multiprocessing import Pool, cpu_count

from luigi import retcodes
import pandas as pd

import disdat.hyperframe as hyperframe
import disdat.common as common
import disdat.utility.aws_s3 as aws_s3
from disdat.data_context import DataContext
from disdat.common import DisdatConfig
from disdat import logger as _logger


PipeCacheEntry = collections.namedtuple('PipeCacheEntry', 'instance uuid path rerun is_left_edge_task')

CONTEXTS = ['DEFAULT']
META_FS_FILE = 'fs.json'


ObjectTypes = Enum('ObjectTypes', 'bundle atom')
ObjectState = Enum('ObjectState', 'present removed')


class DisdatFS(object):
    """
    HyperFrame (bundle) access layer (singleton)
    
    We have one class attribute that keeps track of allocated bundle directories. 
    This is filled at the time we try to run apply.

    Note: The path cache requires that everyone import "disdat.fs" and not "import fs".   Otherwise we will put entries
    in a class object whose name is '<class 'fs.DisdatFS'>' but search in an instance of <class 'disdat.fs.DisdatFS'>.
    The driver uses the fs.DisdatFS class object, but when you run from a pipe defined outside the project, then the
    system finds the class object with the package name appended.   And that class object doesn't have a path_cache
    with anything in it.

    """
    __metaclass__ = common.SingletonType

    task_path_cache = {}  ## [<pipe/luigi task id>] -> PipeCacheEntry(instance, directory, re-run)

    @staticmethod
    def clear_path_cache():
        """
        If you grab the singleton instance, and you try to grab the task_path_cache, you will end up reading
        the class variable.  But if you try to set the class variable, you will make a copy and set it instead.
        So to really clear it, make a static method.

        Returns:
            None
        """
        DisdatFS.task_path_cache.clear()

    @staticmethod
    def get_path_cache(pipe_instance):
        """
        Given a pipe process name, return the resolved path.

        Note: The path cache has to use the pipe.pipe_id(), which is a string
        that takes into account enough of the configuration of the executed pipe that it is
        unique.  That is, it cannot use the pipeline_id(), as that typically is just the human-readable bundle name
        that is the output of the pipeline.

        Args:
            pipe_instance:

        Returns:
            (instance,path,rerun) as PipeCacheEntry

        """

        pipe_name = pipe_instance.pipe_id()

        return DisdatFS.get_path_cache_by_name(pipe_name)

    @staticmethod
    def get_path_cache_by_name(pipe_name):
        """
        Given a pipe name, return the resolved path.
        :return:  (instance,path,rerun) as PipeCacheEntry
        """

        #print "SEARCH PCECLZ {} PIPE {}".format(DisdatFS, pipe_name)

        if pipe_name in DisdatFS.task_path_cache:
            rval = DisdatFS.task_path_cache[pipe_name]
        else:
            rval = None

        return rval

    @staticmethod
    def path_cache():
        """       
        :return: cache dictionary
        """        
        return DisdatFS.task_path_cache

    @staticmethod
    def put_path_cache(pipe_instance, uuid, path, rerun, is_left_edge_task, overwrite=False):
        """  The path cache is used to associate a pipe instance with its output path and whether
        we have decided to re-run this pipe.   If rerun is True, then there should be no
        ouput at this path.  AND it should eventually be added as a new version of this bundle.

        Args:
            pipe_instance:     instance of a pipe
            uuid:              specific uuid of the output path
            path:              where to write the bundle
            rerun:             whether or not we are re-running or re-using
            is_left_edge_task: is this at the top of the DAG?
            overwrite:         overwrite existing entry (if exists)

        Returns:
            pce or raise KeyError

        """
        pipe_name = pipe_instance.pipe_id()
        pce = PipeCacheEntry(pipe_instance, uuid, path, rerun, is_left_edge_task)
        if pipe_name not in DisdatFS.task_path_cache:
            DisdatFS.task_path_cache[pipe_name] = pce
        else:
            if pce == DisdatFS.task_path_cache[pipe_name]: # The tuples are identical
                _logger.error("path_cache dup key: pipe {} already bound to same PCE {} ".format(pipe_name, pce))
            else:
                if overwrite:
                    DisdatFS.task_path_cache[pipe_name] = pce
                else:
                    raise KeyError("path_cache dup key: pipe {} bound to pce {} but trying to re-assign to {}".format(
                        pipe_name, DisdatFS.task_path_cache[pipe_name], pce))
        return pce

    def __init__(self):
        """ Create an FS object.
        1.) Load our state from the meta_dir -- which context we are working in
        2.) Load all available contexts

        :return: new DisdatFS handle
        """
        # Lazily loaded properties
        self.__curr_context = None
        self.__all_contexts = None
        # This is lazily loaded *but* we store this in json on disk, so we self mangle.
        self._mangled_curr_context_name = None

    @property
    def _curr_context_name(self):
        if self._mangled_curr_context_name is None:
            if self._curr_context is not None:
                self._mangled_curr_context_name = self._curr_context.local_ctxt
        return self._mangled_curr_context_name

    @_curr_context_name.setter
    def _curr_context_name(self, value):
        self._mangled_curr_context_name = value

    @property
    def _all_contexts(self):
        if self.__all_contexts is None:
            self.__all_contexts = DataContext.load()
        return self.__all_contexts

    @property
    def _curr_context(self):
        if self.__curr_context is None:
            self.load()
            if self._mangled_curr_context_name is not None:
                try:
                    self.__curr_context = self._all_contexts[self._mangled_curr_context_name]
                except KeyError as ke:
                    print("No current local context, please change context with 'dsdt switch'")
            self.save()
        return self.__curr_context

    @_curr_context.setter
    def _curr_context(self, value):
        self.__curr_context = value

    def get_curr_context(self):
        """
        Grab a pointer to the current context

        Returns:
            (`fs.DataContext`): Current context

        """
        return self._curr_context

    def load(self):
        """
        Load the fs object found in the meta_dir.

        Returns:
        """
        meta_file = os.path.join(DisdatConfig.instance().get_meta_dir(), META_FS_FILE)

        if not os.path.isfile(meta_file):
            _logger.debug("No disdat {} meta fs data file found.".format(meta_file))
        else:
            with open(meta_file, 'r') as json_file:
                state_dict = json.loads(json_file.readline())
            for k, v in state_dict.items():
                self.__dict__[k] = v

    def save(self):
        """
        Write out the json describing the fs.
        Only current context now.

        Returns:
        """
        meta_file = os.path.join(DisdatConfig.instance().get_meta_dir(), META_FS_FILE)

        with open(meta_file, 'w') as json_file:
            state_dict = {'_mangled_curr_context_name': self._mangled_curr_context_name}
            json_file.write(json.dumps(state_dict))

    def in_context(self):
        """
        Are we currently in a valid context?
        :return:
        """
        return self._curr_context and self._curr_context.is_valid()

    def reuse_hframe(self, pipe, hframe, is_left_edge_task, data_context=None):
        """
        Re-use this bundle, everything stays the same, just put in the cache
        Note: Currently doesn't use this FS instance, but to be consistent with
        new_output_bundle below.

        Args:
            pipe:
            hframe:
            is_left_edge_task:
            data_context:

        Returns:
            None
        """

        pce  = self.get_path_cache(pipe)
        if pce is None:
            _logger.debug("reuse_hframe: Adding a new (unseen) task to the path cache.")
        else:
            _logger.debug("reuse_hframe: Found a task in our dag already in the path cache: reusing!")
            return

        if data_context is None:
            if not self.in_context():
                _logger.warning('Not in a data context')
                raise RuntimeError('Not in a data context')
            data_context = self.get_curr_context()

        dir = data_context.implicit_hframe_path(hframe.pb.uuid)
        DisdatFS.put_path_cache(pipe, hframe.pb.uuid, dir, False, is_left_edge_task)

    def new_output_hframe(self, pipe, is_left_edge_task, force_uuid=None, data_context=None):
        """
        This proposes a new output hframe.
        1.) Create a new UUID
        2.) Create the directory in the context
        3.) Add this to the path cache

        Note: We don't add to context's db yet.  The job or pipe hasn't run yet.  So it
        hasn't made all of its outputs.  If it fails, by definition it won't right out the
        hframe to the context's directory.   On rebuild / restart we will delete the directory.
        However, the path_cache will hold on to this directory in memory.

        Args:
            pipe:
            is_left_edge_task:
            force_uuid:
            data_context:

        Returns:

        """

        pce  = self.get_path_cache(pipe)

        if pce is None:
            _logger.debug("new_output_hframe: Adding a new (unseen) task to the path cache.")
        else:
            _logger.debug("new_output_hframe: Found a task in our dag already in the path cache: reusing!")
            return

        if data_context is None:
            if not self.in_context():
                _logger.warning('Not in a data context')
                raise RuntimeError('Not in a data context')
            data_context = self.get_curr_context()

        dir, uuid, _ = data_context.make_managed_path(uuid=force_uuid)

        DisdatFS.put_path_cache(pipe, uuid, dir, True, is_left_edge_task)

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

        if data_context is None:
            if not self.in_context():
                _logger.warning('Not in a data context')
                return []
            data_context = self.get_curr_context()

        if not self.in_context():
            return_strings.append('[None]')
        else:
            return_strings.append("Disdat Context {}".format(data_context.get_repo_name()))
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

    def add(self, bundle_name, path_name, tags):
        """  Create bundle bundle_name given path path_name.
        The path may point to a file or a csv/tsv file.  If a file, create a simple bundle
        with a single link.  Otherwise create a bundle with the data in the csv/tsv file.
        The presentation is set to dataframe for these bundle creations.

        If bundle exists, create a new version with the same name.

        Args:
            bundle_name (str):
            path_name (str):
            tags (dict):

        Returns:

        """
        import disdat.add  # Note, FS->AddTask->PipeBase->FS, import cycle.

        if not self.in_context():
            _logger.warning('Not in a data context')
            return
        _logger.debug('Adding file {} to bundle {} in context {}'.format(path_name,
                                                                         bundle_name,
                                                                         self._curr_context.get_repo_name()))

        # we only make the instance to add the output bundle -- it MUST have the same args as args below!
        add_pipe = disdat.add.AddTask(path_name, bundle_name, tags)

        self.new_output_hframe(add_pipe, is_left_edge_task=False)

        args = [disdat.add.AddTask.task_family,
                '--local-scheduler',
                '--input-path', path_name,
                '--output-bundle', bundle_name,
                '--tags', json.dumps(tags)
                ]

        retcodes.run_with_retcodes(args)

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

        if data_context is None:
            if not self.in_context():
                _logger.warning('Not in a data context')
                raise RuntimeError('Not in a data context')
            data_context = self.get_curr_context()

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

        if data_context is None:
            if not self.in_context():
                _logger.warning('Not in a data context')
                raise RuntimeError('Not in a data context')
            data_context = self.get_curr_context()

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

        if data_context is None:
            if not self.in_context():
                _logger.warning('Not in a data context')
                raise RuntimeError('Not in a data context')
            data_context = self.get_curr_context()

        found = data_context.get_hframes(processing_name=processing_name)

        # TODO: filter at SQL instead
        if len(found) > 0:
            if getall:
                return found
            else:
                return found[0]
        else:
            return None

    def ls(self, search_name, print_tags, print_intermediates, print_long, print_args,
           before=None, after=None, maxbydate=False, committed=None, tags=None, data_context=None):
        """
        Enumerate bundles (hyperframes) in this context.

        Args:
            search_name: May be None.  Interpret as a simple regex (one kleene star)
            print_tags (bool): Whether to print the bundle tags
            print_intermediates (bool): Whether to show intermediate bundles
            print_args (bool): Whether to print the arguments used to produce this bundle
            before (date.datetime): '01-03-19 02:40:37' or date '01-03-19' inclusive range
            after (date.datetime): '01-03-19 02:40:37' or date '01-03-19' inclusive range
            committed (bool): If True, just committed, if False, just uncommitted, if None then ignore.
            maxbydate (bool): return the latest by date
            tags: Optional. A dictionary of tags to search for.
            data_context (`disdat.data_context.DataContext`): Optional data context to operate in

        Returns:

        """

        if data_context is None:
            if not self.in_context():
                _logger.warning('Not in a data context')
                return []
            data_context = self.get_curr_context()

        if not print_intermediates:
            if tags is not None:
                tags.update({'root_task': True})
            else:
                tags = {'root_task': True}

        output_strings = []

        if print_long:
            output_strings.append(DisdatFS._pretty_print_header())

        for i, r in enumerate(data_context.get_hframes(human_name=search_name, tags=tags,
                                                       maxbydate=maxbydate, before=before, after=after)):
            if committed is not None:
                if committed:
                    if not r.get_tag('committed'):
                        continue
                else:
                    if r.get_tag('committed'):
                        continue

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
                                                                   time.strftime("%m-%d-%y %H:%M:%S ",time.localtime(hfr.pb.lineage.creation_date)),
                                                                   committed,
                                                                   hfr.pb.uuid)
        if print_tags:
            tags = ["[{}]:[{}]".format(k, v) for k, v in hfr.tag_dict.items() if k != 'committed' and common.BUNDLE_TAG_PARAMS_PREFIX not in k]
            output_string += ' '.join(tags)

        if print_args:
            tags = ["[{}]:[{}]".format(k.strip(common.BUNDLE_TAG_PARAMS_PREFIX), v)
                    for k, v in hfr.tag_dict.items() if common.BUNDLE_TAG_PARAMS_PREFIX in k]
            if len(tags) > 0:
                output_string += '\n\t ARGS: ' + ' '.join(tags)

        return output_string

    def cat(self, human_name, uuid=None, tags=None, file=None, data_context=None):
        """
        Given a bundle name and optional uuid, return the object that was saved in the bundle

        Args:
            human_name (str):
            uuid (str):
            tags (:dict):
            file (str): output file
            data_context (`disdat.data_context.DataContext`):

        Returns:
            (`DataFrame`) or (`numpy.ndarray`) or scalar type
        """

        if data_context is None:
            if not self.in_context():
                _logger.warning('Not in a data context')
                return None
            data_context = self.get_curr_context()

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
            return None

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
        Identical to pythia_uuid()
        """
        return str(uuid.uuid4())

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
                if self._curr_context is not None and self._curr_context is ctxt:
                    cprint("*", "white", end='')
                    cprint("\t{}".format(ctxt_name), "green", end='')
                    cprint("\t[{}@{}]".format(ctxt.remote_ctxt, self._curr_context.get_remote_object_dir()))
                else:
                    print("\t{}\t[{}@{}]".format(ctxt.local_ctxt, ctxt.remote_ctxt, ctxt.get_remote_object_dir()))
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

    def delete_branch(self, fq_context_name, remote, force):
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

        if local_context == self._curr_context_name:
            print("Disdat on context {}, please 'dsdt switch <otherbranch>' before deleting.".format(local_context))
            return

        if local_context in self._all_contexts:
            dc = self._all_contexts[local_context]
            remote_context_url= dc.get_remote_object_dir()
            dc.delete_branch(force=force)
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

    def switch(self, local_context_name, save=True):
        """
        Switch to a different local context.

        Args:
            local_context_name (str): May be <remote context>/<local context> or <local context>
            save (bool): Whether to record context change on disk.
        Returns:
            prior_context_name (str):  String name of the prior context

        """

        assert local_context_name is not None

        repo, local_context = DisdatFS._parse_fq_context_name(local_context_name)

        prior_context_name = self._curr_context_name

        if self._curr_context_name == local_context:
            assert(local_context in self._all_contexts)
            assert(self._curr_context == self._all_contexts[local_context])
            _logger.info("Disdat already within a valid data context_name {}".format(local_context))
            return prior_context_name

        new_context = self.get_context(local_context_name)

        if new_context is not None:
            self._curr_context_name = local_context
            self._curr_context = new_context
            print("Switched to context {}".format(self._curr_context_name))
        else:
            print("In context {}".format(self._curr_context_name))

        if save:
            self.save()

        return prior_context_name

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

        if data_context is None:
            if not self.in_context():
                _logger.warning('Not in a data context')
                return
            data_context = self.get_curr_context()

        _logger.debug('Committing bundle {} in context {}'.format(bundle_name, data_context.get_repo_name()))

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

            for fr in next_hf.get_frames(self.get_curr_context()):

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

    def shallow_hfr_copy(self, hfr, new_uuid=None, tags=None, presentation=None):
        """
        Take this existing hypeframe, find all the link frames, copy data to a new directory / uuid.

        NOTE: Will mutate this HyperFrameRecord and the pb it contains.  If you read from the disk or you
        read from the sqlite db, we will have already made a copy for you.  I.e., 'get_hframes' makes copies.

        ASSUME: Local copy.

        If there is a hframe frame in this hyperframe, then we error out.  See _copy_hfr for a more sophisticated copy
        that looks at hfr's that can refer to other hyperframes.

        Args:
            hfr:
            new_uuid: Optional new uuid to use for this new copy
            tags (:dict): Optional new set of tags to place on this copy.
            presentation: Optional new presentation

        Returns:
            hyperframe.HyperFrameRecord

        """

        if new_uuid is None:
            local_fs_managed_path, new_hfr_uuid, _ = self._curr_context.make_managed_path()
        else:
            new_hfr_uuid = new_uuid
            local_fs_managed_path = self._curr_context.implicit_hframe_path(new_uuid)
            s3_managed_path = None  # unused _ above

        frame_copies = []
        need_to_copy = False

        # Move files in LINK frames to new local destination
        for fr in hfr.get_frames(self.get_curr_context()):

            if fr.is_hfr_frame():
                # CASE 1:  Currently do not support HFR references.
                raise Exception("Disdat shallow_hfr_copy used on a hyperframe that refers to other hyperframes.")
            else:
                # CASE 2:  Local or S3 LINK frames
                # If a local fs frame, copy files to this new managed path.
                # If s3 paths, then they have to be in a bundle in a context.
                # If I change the s3 paths to reflect the new uuid, then I should push that to S3.
                # If I don't change them, then we have an implicit reference to another bundle. *** easiest ***
                # OR we copy the files from s3 to this local drive.  *** this is expensive but safest ***
                # For now we copy the remote S3 file to the local bundle.  Requires connectivity.  But
                # this breaks the implicit dependency.

                possible_fr_copy = self._copy_fr(fr, new_hfr_uuid, local_fs_managed_path)

                if possible_fr_copy is not fr:
                    need_to_copy = True
                else:
                    # must update these frames - they must point to the right uuid and have new uuids
                    possible_fr_copy = possible_fr_copy.mod_hfr_uuid(new_hfr_uuid)

                frame_copies.append(possible_fr_copy)

        # update UUID -- must do this before other mods (especially mod_frames)!
        hfr = hfr.mod_uuid(new_hfr_uuid)

        # Update frames if there were LINK frames
        hfr = hfr.mod_frames(frame_copies)

        # update TAGS
        if tags is not None:
            hfr = hfr.replace_tags(tags)

        # update Presentation
        if presentation is not None:
            hfr = hfr.mod_presentation(presentation)

        return hfr

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
                    self._copy_fr_links_to_branch(fr, data_context.get_remote_object_dir(), data_context)
                else:
                    self._copy_fr_links_to_branch(fr, data_context.get_object_dir(), data_context)

        # Push hyperframe to remote
        data_context.write_hframe(hfr, to_remote=to_remote)

        return

    @staticmethod
    def _copy_fr_links_to_branch(fr, branch_object_dir, data_context):
        """
        Given a non-HyperFrame frame, if a local fs or s3 frame, do the
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
            _ = DataContext.copy_in_files(src_paths, bundle_dir)
        return

    def _copy_hfr(self, hfr, copy_to='local', force_uuid=None):
        """
        Copy this HyperFrameRecord to a new HyperFrameRecord for S3 destination

        NOTE: Will mutate this HyperFrameRecord and the pb it contains.  If you read from the disk or you
        read from the sqlite db, we will have already made a copy for you.  I.e., 'get_hframes' makes copies.

        NOTE: It is OK that the frames aren't copied.

        NOTE: This will push up external files and PBs.

        Copy top-level HFR, copy all Frames.
        If Frame is local FS -- copy_in to new directory
        If Frame is local FS and to_remote -- Make correct s3 paths, push files to s3
        If Frame is s3 -- Copy_in files to new s3 path and fix path (optimize in future)
        If Frame is db -- Do nothing

        Args:
            hfr:
            copy_to: 'local' or 'remote'

        Returns:
            (bool) : whether we modified this hyperframe, i.e., made a "copy"
        """

        # Currently we don't have a use for creating new copies of a bundle and assigning
        # branch new uuids.   This, though, could be useful in the future.
        raise Exception("Current Dead Code")

        # We may or may not make a new HFR, but if so make a new path / uuid
        # throw them away if we don't use them.
        # note that we make an s3 key

        #print "---------------COPYHFR WITH ROOT_HFR {}  ".format(hfr.pb.uuid)

        local_fs_managed_path, new_hfr_uuid, s3_managed_path = self._curr_context.make_managed_path(uuid=force_uuid)

        if copy_to == 'local':
            managed_path = local_fs_managed_path
        elif copy_to == 'remote':
            managed_path = s3_managed_path
        else:
            _logger.error("copy_hfr has unknown copy_to argument {}.  Should be 'local' or 'remote'".format(copy_to))

        frame_copies = []
        need_to_copy = False

        for fr in hfr.get_frames(self.get_curr_context()):

            if fr.is_hfr_frame():

                # CASE 1: A frame containing HFRs.   Descend recursively.
                # If we did need to copy, then make a new frame.
                hfr_copies = []

                for next_hfr in fr.get_hframes():
                    possible_hfr_copy = self._copy_hfr(next_hfr, copy_to=copy_to)
                    if possible_hfr_copy: # is not next_hfr:
                        need_to_copy = True
                    hfr_copies.append(next_hfr) # NOTE: Use existing, because we modify in place!

                if need_to_copy:
                    # Make a new FrameRecord because at least one hfr needed to be a copy.
                    new_frame = hyperframe.FrameRecord.make_hframe_frame(new_hfr_uuid, fr.pb.name, hfr_copies)
                else:
                    new_frame = fr

                frame_copies.append(new_frame)

            else:

                # CASE 2:  This is a different kind of frame
                # If it is a local fs or an s3 frame, then we have to copy
                # the files over.  It's a new frame.
                possible_fr_copy = self._copy_fr(fr, new_hfr_uuid, managed_path)

                if possible_fr_copy is not fr:
                    need_to_copy = True
                    # If we copy any of them

                frame_copies.append(possible_fr_copy)

        # We have iterated through all of our frames.  Some might have needed copies, if so, make a new
        # HFR and return it.  The new HFR uses the frames in frame_copies(), uses a new UUID, and a new creation
        # date and frame_cache().
        if need_to_copy or force_uuid is not None:
            # Modify in place!  By construction, this HFR is *already* a copy.
            # print "---------------ROOT_HFR {}  MAKING NEW HFR {}  LOCAL ".format(hfr.pb.uuid, new_hfr_uuid)
            # update UUID -- must do this before other mods (especially mod_frames)!

            # Some frames are new and there may be some old frames that are unmodified
            # But we need to make a new HFR.  That means, we need all new frames.
            # The new frames have the new hfr_uuid and they have new uuids
            # But the old frames point to the prior hfr and have old uuids.
            # Here we copy over all the frames, so all need new frame (not hyperframe) uuids.
            # This is a hack, we iterate over all, and give them all new uuids.  The new ones
            # already have new uuids, but no one has written them out yet.
            for fr in frame_copies:
                fr = fr.mod_hfr_uuid(new_hfr_uuid)

            hfr = hfr.mod_uuid(new_hfr_uuid)
            hfr = hfr.mod_frames(frame_copies)
            # Need to write this hyperframe locally (with all paths pointing at s3)
            self.get_curr_context().write_hframe(hfr)

        # TODO: someone needs to check if it's already there!
        # make sure all hyperframes are on remote
        # print "---------------ROOT_HFR {}  MAKING NEW HFR {}  REMOTE".format(hfr.pb.uuid, new_hfr_uuid)
        self.get_curr_context().write_hframe(hfr, to_remote=True)

        return need_to_copy

    def _copy_fr(self, fr, new_hfr_uuid, managed_path):
        """
        Given a non-HyperFrame frame, if a local fs or s3 frame, do the
        copy_in to the managed_path.

        Args:
            fr:  Frame to possibly copy_in files to managed_path
            new_hfr_uuid:  The new uuid of the new enclosing hframe
            managed_path: The s3 path where these files should go.

        Returns:
            (`hyperframe.FrameRecord`): Return either a fr copy with new paths or same fr
        """
        if fr.is_local_fs_link_frame() or fr.is_s3_link_frame():
            # Each frame has a list of bundle://<name> paths.
            # Everything in a frame is either local or it is remote
            # Ensure copy_in does not copy from a managed path to the same managed path.
            # We should make sure that luigi targets are not copied in.
            assert self._curr_context is not None
            src_paths = self._curr_context.actualize_link_urls(fr)
            new_paths = DataContext.copy_in_files(src_paths, managed_path)
            fr = hyperframe.FrameRecord.make_link_frame(new_hfr_uuid, fr.pb.name, new_paths, managed_path)
        return fr

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

        if data_context is None:
            if not self.in_context():
                _logger.warning('Not in a data context')
                raise UserWarning("Not in a data context")
            data_context = self.get_curr_context()

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
                    DataContext.copy_in_files(f, managed_path)

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

        MAX_WAIT = 12 * 60

        pool = Pool(processes = cpu_count()) # I/O bound, so let it use at least cpu_count()

        _logger.debug("Fast Pull Pool using {} processes.".format(cpu_count()))

        remote_s3_object_dir = data_context.get_remote_object_dir()
        s3_bucket, remote_obj_dir = aws_s3.split_s3_url(remote_s3_object_dir)
        all_objects = aws_s3.ls_s3_url_objects(remote_s3_object_dir)

        if not localize:
            all_objects = [obj for obj in all_objects if 'frame.pb' in obj.key]

        multiple_results = []
        for s3_obj in all_objects:
            obj_basename = os.path.basename(s3_obj.key)
            obj_suffix = s3_obj.key.replace(remote_obj_dir,'')
            obj_suffix_dir = os.path.dirname(obj_suffix).strip('/')  # remote_obj_dir won't have a trailing slash
            local_uuid_dir = os.path.join(data_context.get_object_dir(), obj_suffix_dir)
            local_object_path = os.path.join(local_uuid_dir, obj_basename)
            if not os.path.exists(local_object_path):
                multiple_results.append(pool.apply_async(aws_s3.get_s3_key,
                                                         (s3_bucket,s3_obj.key,local_object_path)))
        pool.close()

        results = [res.get(timeout=MAX_WAIT) for res in multiple_results]

        pool.join()

        _logger.info("Fast pull complete -- thread pool closed and joined.")

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

        if data_context is None:
            if not self.in_context():
                _logger.warning('Not in a data context')
                raise UserWarning("Not in a data context")
            data_context = self.get_curr_context()

        if data_context.remote_ctxt_url is None:
            print("Pull cannot execute.  Local context {} on remote {} not bound.".format(data_context.local_ctxt, data_context.remote_ctxt))
            raise UserWarning("Local context {} has no remote".format(data_context.local_ctxt))

        if human_name is None and uuid is None:
            # NOTE: This is fast and loose.  Another command might be editing the db.  Unit test.
            # NOTE: If we fail, we could have a partial DB.  Need to surface the rebuild command.
            self.fast_pull(data_context, localize)
            data_context.rebuild_db()
            return

        #start = time.time()
        possible_hframe_objects = aws_s3.ls_s3_url_objects(data_context.get_remote_object_dir())
        #print "List time {} seconds".format(time.time() - start)

        hframe_objects = [obj for obj in possible_hframe_objects if '_hframe.pb' in obj.key]

        for s3_hfr_obj in hframe_objects:
            hfr_basename = os.path.basename(s3_hfr_obj.key)
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
                    # Are we trying to localize a particular HyperFrame?  match name and uuid. 

                    obj = s3_hfr_obj.Object().get()
                    hfr_test = hyperframe.HyperFrameRecord.from_str_bytes(obj['Body'].read())
                    if human_name is not None:
                        if human_name != hfr_test.pb.human_name:
                            continue
                        else:
                            print("Found remote bundle with human name {}, uuid {} localizing ...".format(hfr_test.pb.human_name,
                                                                                                          hfr_test.pb.uuid))

                    # grab files for this hyperframe -- read the local HFR frames
                    DisdatFS._localize_hfr(local_hfr, s3_uuid, data_context)

            else:
                obj = s3_hfr_obj.Object().get()
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
                    local_fr_path = os.path.join(local_uuid_dir,fr_basename)
                    s3_fr_obj.Object().download_file(local_fr_path)

                data_context.write_hframe_db_only(hfr_test)

                if localize:
                    DisdatFS._localize_hfr(self.get_hframe_by_uuid(s3_uuid, data_context=data_context),
                                           s3_uuid, data_context)

    def remote_add(self, remote_context, s3_url, force, ):
        """
        Bind the context name to this s3path.   For all branches with context name, set remote to s3path.

        Args:
            remote_context (str):  the name of the remote context
            s3_url (str):   the s3 path to bind to this remote context
            force (bool):

        Returns:
            None
        """

        self.get_curr_context().bind_remote_ctxt(remote_context, s3_url, force=force)

    def status(self, human_name):
        """

        Args:
            human_name:

        Returns:

        """
        return_strings = []

        if not self.in_context():
            return_strings.append('[None]')
        else:
            return_strings.append("Disdat Context {}".format(self._curr_context.get_repo_name()))
            return_strings.append("On local context {}".format(self._curr_context.get_local_name()))
            if self._curr_context.get_remote_object_dir() is not None:
                return_strings.append("Remote @ {}".format(self._curr_context.get_remote_object_dir()))
            else:
                return_strings.append("No remote set.")
        if False:
            try:
                hfrs = self._curr_context.get_hframes(human_name=human_name)
                if len(hfrs) > 0:
                    return_strings.append("Most recent object with this name is:")
                    return_strings.extend(DisdatFS._pretty_print_hframe(hfrs[0]))
                    return_strings.append("Older versions of this object are:")
                    for hfr in hfrs[1:]:
                        return_strings.extend(DisdatFS._pretty_print_hframe(hfr))
            except KeyError:
                return_strings.append('No hyperframe with that name found')

        return return_strings


def _branch(fs, args):
    if args.delete:
        fs.delete_branch(args.context, args.remote, args.force)
    else:
        fs.branch(args.context)


def _add(fs, args):

    fs.add(args.bundle, args.path_name, tags=common.parse_args_tags(args.tag))


def _commit(fs, args):
    fs.commit(args.bundle, common.parse_args_tags(args.tag), uuid=args.uuid)


def _remote(fs, args):
    fs.remote_add(args.context, args.s3_url, args.force)


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
                   args.verbose,
                   args.print_args,
                   committed=committed,
                   before=before,
                   after=after,
                   maxbydate=args.latest_by_date,
                   tags=common.parse_args_tags(args.tag)):
        print(f)


def _cat(fs, args):

    result = fs.cat(args.bundle, uuid=args.uuid, tags=common.parse_args_tags(args.tag), file=args.file)

    if result is None:
        print("dsdt cat found no bundle with name {}".format(args.bundle))
    else:

        if isinstance(result, pd.DataFrame):
            # If df, make sure we print out all columns
            pd.set_option('display.max_colwidth', -1)
            print(result.to_string())
        else:
            # else default print the object
            print(result)

def _status(fs, args):
    for f in fs.status(args.bundle):
        print(f)


def init_fs_cl(subparsers):
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

    # add
    add_p = subparsers.add_parser('add', description='Create a bundle from a .csv, .tsv, or a directory of files.')
    add_p.add_argument('-t', '--tag', nargs=1, type=str, action='append',
                       help="Set one or more tags: 'dsdt add -t authoritative:True -t version:0.7.1'")
    add_p.add_argument('bundle', type=str, help='The destination bundle in the current context')
    add_p.add_argument('path_name', type=str, help='File or directory of files to add to the bundle', action='store')
    add_p.set_defaults(func=lambda args: _add(fs, args))

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
    rm_p.add_argument('bundle', nargs='?', type=str, default=None, help='The destination bundle in the current context')
    rm_p.add_argument('-f', '--force', action='store_true', default=False, help='Force remove of a committed bundle')
    rm_p.add_argument('-u', '--uuid', type=str, default=None, help='Bundle UUID to remove')
    rm_p.add_argument('-t', '--tag', nargs=1, type=str, action='append',
                      help="Having a specific tag: 'dsdt rm -t committed:True -t version:0.7.1'")
    rm_p.add_argument('--all', action='store_true',
                      help='Remove the current version and all history.  Otherwise just remove history')
    rm_p.add_argument('--old', action='store_true', default=False,
                      help='Remove everything except the most recent bundle.')
    rm_p.set_defaults(func=lambda args: _rm(fs, args))

    # ls
    ls_p = subparsers.add_parser('ls')
    ls_p.add_argument('bundle', nargs='*', type=str, help="Show all bundles 'dsdt ls' or explicit bundle 'dsdt ls <somebundle>' in current context")
    ls_p.add_argument('-a', '--print-args', action='store_true', help="Print the arguments (if any) used to create the bundle.")
    ls_p.add_argument('-p', '--print-tags', action='store_true', help="Print each bundle's tags.")
    ls_p.add_argument('-i', '--intermediates', action='store_true',
                      help="List all bundles, including intermediate outputs.")
    ls_p.add_argument('-c', '--committed', action='store_true',
                      help="List only committed bundles.")
    ls_p.add_argument('-u', '--uncommitted', action='store_true',
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

    # status
    status_p = subparsers.add_parser('status')
    status_p.add_argument('bundle', type=str, help='A bundle in the current context')
    status_p.set_defaults(func=lambda args: _status(fs, args))

    # remote add <name> <s3_url>
    remote_p = subparsers.add_parser('remote')
    remote_p.add_argument('context', type=str, help='Name of the remote context')
    remote_p.add_argument('s3_url', type=str, help="Remote context site, i.e, 's3://<bucket>/dsdt/'")
    remote_p.add_argument('-f', '--force', action='store_true',
                          help="Force re-binding of remote. Executes 'dsdt pull --localize' to resolve files, which might take awhile.")
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
