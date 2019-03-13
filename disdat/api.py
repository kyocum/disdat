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
API

A Disdat API for creating and publishing bundles.

These calls are note thread safe.  If they operate on a context, they
require the user to specify the context.   This is unlike the CLI that maintains state on disk
that keeps track of your current context between calls.   The API won't change the context you're in
in the CLI and vice versa.

To make this work we get a pointer to the singelton FS object, and we temporarily change the context (it will
automatically assume the context of the CLI) and perform our operation.

Author: Kenneth Yocum
"""
from __future__ import print_function

import os
import json

import luigi
from luigi.contrib import s3

import disdat.apply
import disdat.run
import disdat.fs
import disdat.common as common
from disdat.pipe_base import PipeBase, get_pipe_version
from disdat.db_link import DBLink
from disdat.pipe import PipeTask
from disdat.hyperframe import HyperFrameRecord, LineageRecord
from disdat.run import Backend, run_entry
from disdat.dockerize import dockerize_entry
from disdat import logger as _logger

disdat.fs.DisdatConfig.instance()

fs = disdat.fs.DisdatFS()


def set_aws_profile(aws_profile):
    os.environ['AWS_PROFILE'] = aws_profile


"""
Bundle Creation
1.) Create a bundle object (bind to data context on creation)
2.) Allocate files and directories wrt that object
3.) Call add to add this bundle to the context
"""


class BundleWrapperTask(PipeTask):
    """ This task allows one to create bundles that can be referred to
    in a Disdat pipeline through a self.add_external_dependency.
    1.) User makes a bundle outside of Luigi
    2.) Luigi pipeline wants to use bundle.
       a.) Refer to the bundle as an argument and reads it using API (outside of Luigi dependencies)
       b.) Refer to the bundle using a BundleWrapperTask.  Luigi Disdat pipeline uses the latest
       bundle with the processing_name.

    Two implementation options:
    1.) We add a "add_bundle_dependency()" call to Disdat.  This directly changes how we schedule.
    2.) We add a special Luigi task (as luigi has for outside files) and use that to produce the
    processing_name, when there isn't an actual task creating the data.

    Thus this task is mainly to provide a way that
    a.) A user can create a bundle and set the processing_name
    b.) The pipeline can refer to this bundle using the same processing_name

    The parameters in this task should allow one to sufficiently identify versions
    of this bundle.

    Note:  No processing_name and No UUID
    """
    name = luigi.Parameter(default=None)
    owner = luigi.Parameter(default=None)
    tags = luigi.DictParameter(default={})

    def bundle_inputs(self):
        """ Determine input bundles """
        raise NotImplementedError

    def bundle_outputs(self):
        """ Determine input bundles """
        raise NotImplementedError

    def pipeline_id(self):
        """ default is shortened version of pipe_id
        But here we want it to be the set name """
        return self.name


class Bundle(HyperFrameRecord):

    def __init__(self, local_context, name, owner=''):
        """ Given name and a local context, create a handle that users can
        work with to:
        a.) Create files / directories / dbtables (i.e., Bundle Links)
        b.) Add constants and files into bundle

        Create the bundle ahead of time, and add items to it.
        Or use a temp dir and copy things into the bundle when you're done.
        If #2 then, it's easy to use a bundle object and write it to multiple
        contexts.   We should close or destroy a bundle in case 2.


        Args:
            local_context (str): Where this bundle will be output or where it was sourced from.
            name (str): Human name for this bundle
        """

        try:
            self.data_context = fs.get_context(local_context)
        except Exception as e:
            _logger.error("Unable to allocate bundle in context: {} ".format(local_context, e))
            return

        super(Bundle, self).__init__(human_name=name, owner=owner)

        self.local_dir = None
        self.remote_dir = None

        self.open = False
        self.closed = False

        self.input_data = None  # The df, array, dictionary the user wants to store

        self.db_targets = []    # list of created db targets
        self.depends_on = []    # list of tuples (processing_name, uuid) of bundles on which this bundle depends
        self.data = None

    """ Convenience accessors """

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
    def tags(self):
        return self.tag_dict

    @property
    def params(self):
        """ Return the tags that were parameters """
        return {k.strip(common.BUNDLE_TAG_PARAMS_PREFIX):json.loads(v)
                 for k,v in self.tag_dict.items()
                 if common.BUNDLE_TAG_PARAMS_PREFIX in k}

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

        assert(not self.open and not self.closed)  # assume this is a brand-new bundle

        self.open = False
        self.closed = True

        self.pb = hfr.pb
        self.init_internal_state()

        self.depends_on = hfr.pb.lineage.depends_on
        self.data = self.data_context.present_hfr(hfr)

    """ Python Context Manager Interface """

    def __enter__(self):
        """ 'open'
        """
        return self._open()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """ 'close'
        If there has been an exception, let the user deal with the written created bundle.
        """
        self._close()

    def _open(self):
        """ Add the bundle to this local context

        Note, we don't need to checkout this context, we just need the relevant context object.

        Args:

        Returns:
            None
        """
        if self.open:
            _logger.warn("Bundle is already open.")
            return
        elif not self.closed:
            self.local_dir, self.pb.uuid, self.remote_dir = self.data_context.make_managed_path()
            self.open = True
        else:
            _logger.warn("Bundle is closed -- unable to re-open.")
        return self

    def _close(self):
        """ Write out this bundle as a hyperframe.

        Parse the data, set presentation, create lineage, and
        write to disk.

        This closes the bundle so it may not be re-used.

        Returns:
            None
        """

        try:

            presentation, frames = PipeBase.parse_return_val(self.uuid, self.data, self.data_context)

            self.add_frames(frames)

            self.pb.presentation = presentation

            cv = get_pipe_version(BundleWrapperTask)

            lr = LineageRecord(hframe_name=self._set_processing_name(), # <--- setting processing name
                               hframe_uuid=self.uuid,
                               code_repo=cv.url,
                               code_name='unknown',
                               code_semver=cv.semver,
                               code_hash=cv.hash,
                               code_branch=cv.branch,
                               depends_on=self.depends_on)

            self.add_lineage(lr)

            self.replace_tags(self.tags)

            self.data_context.write_hframe(self)

        except Exception as error:
            """ If we fail for any reason, remove bundle dir and raise """
            PipeBase.rm_bundle_dir(self.local_dir, self.uuid, self.db_targets)
            raise

        self.closed = True
        self.open = False

        return self

    """ Convenience Routines """

    def cat(self):
        """ Return the data in the bundle
        The data is already present in .data

        """
        assert(self.closed and not self.open)
        return self.data

    def add_data(self, data):
        """ Add data to a bundle.   The bundle must be open and not closed.
            One adds one data item to a bundle (dictionary, list, tuple, scalar, or dataframe).
            Calling this replaces the latest item -- only the latest will be included in the bundle on close.

        Args:
            data (list|tuple|dict|scalar|`pandas.DataFrame`):

        Returns:
            self
        """
        assert(self.open and not self.closed)
        if self.data is not None:
            _logger.warning("Disdat API add_data replacing existing data on bundle")
        self.data = data
        return self

    def rm(self):
        """ Remove bundle from the current context associated with this bundle object
        Only remove this bundle with this uuid.
        This only makes sense if the bundle is closed.
        """
        assert(self.closed and not self.open)
        fs.rm(uuid=self.uuid, data_context=self.data_context)
        return self

    def commit(self):
        """ Shortcut version of api.commit(uuid=bundle.uuid)

        Returns:
            self
        """
        fs.commit(None, None, uuid=self.uuid, data_context=self.data_context)
        return self

    def push(self):
        """ Shortcut version of api.push(uuid=bundle.uuid)

        Returns:
            (`disdat.api.Bundle`): this bundle

        """

        assert(self.closed and not self.open)

        if self.data_context.get_remote_object_dir() is None:
            raise RuntimeError("Not pushing: Current branch '{}/{}' has no remote".format(self.data_context.get_repo_name(),
                                                                                          self.data_context.get_local_name()))

        fs.push(uuid=self.uuid, data_context=self.data_context)

        return self

    def pull(self, localize=False):
        """ Shortcut version of api.pull()

        Note if localizing, then we update this bundle to reflect the possibility of new, local links

        """
        assert( (not self.open and not self.closed) or (not self.open and self.closed) )
        fs.pull(uuid=self.uuid, localize=localize, data_context=self.data_context)
        if localize:
            assert self.is_presentable()
            self.data = self.data_context.present_hfr(self) # actualize link urls
        return self

    def add_tags(self, tags):
        """ Add tag to our set of input tags

        Args:
            k,v (dict): string:string dictionary

        Returns:
            self
        """
        if not self.open:
            _logger.warn("Open the bundle to modify tags.")
            return
        if self.closed:
            _logger.warn("Unable to modify tags in a closed bundle.")
            return
        super(Bundle, self).add_tags(tags)
        return self

    def db_table(self, dsn, table_name, schema_name):
        """

        Args:
            dsn (unicode):
            table_name (unicode):
            schema_name (unicode):

        Returns:
            `disdat.db_link.DBLink`

        """
        assert (self.open and not self.closed)

        db_target = DBLink(None, dsn, table_name, schema_name, context=self.data_context)

        self.db_targets.append(db_target)

        return db_target

    def make_directory(self, dir_name):
        """ Returns path `<disdat-managed-directory>/<dir_name>`.  This is used if you need
        to hand a process an output directory and you do not have control of what it writes in
        that directory.

        Add this path as you would add file paths to your output bundle.  Disdat will incorporate
        all the data found in this directory into the bundle.

        See Pipe.create_output_dir()

        Arguments:
            dir_name (str): The human readable name to a file reference

        Returns:
            str: A directory path managed by disdat
        """
        assert (self.open and not self.closed)

        fqp = os.path.join(self.local_dir, dir_name)
        try:
            os.makedirs(fqp)
        except IOError as why:
            _logger.error("Creating directory in bundle directory failed:".format(why))

        return fqp

    def make_file(self, filename):
        """
        Create a file-like object to write to called 'filename'

        The file will be placed in the output bundle directory.  However it won't be
        recorded as part of the bundle unless the path or this target is placed
        in the output.  I.e., `bundle.add_data(bundle.make_file("my_file"))`

        Arguments:
            filename (str,list,dict): filename to create in the bundle

        Returns:
            `luigi.LocalTarget` or `luigi.s3.S3Target`
        """
        assert (self.open and not self.closed)

        return PipeBase.filename_to_luigi_targets(self.local_dir, filename)

    def add_file(self, filename):
        """
        Create a file-like object to write to called 'filename' and automatically add
        it to the output bundle.  This is useful if your bundle only contains output files
        and you don't wish to include any other information in your bundle.

        The file will be placed in the output bundle directory.

        Note: if you call `bundle.add_data()` it will overwrite any previous file adds.

        Arguments:
            filename (str,list,dict): filename to create in the bundle

        Returns:
            `luigi.LocalTarget` or `luigi.contrib.s3.S3Target`
        """
        assert (self.open and not self.closed)

        target = PipeBase.filename_to_luigi_targets(self.local_dir, filename)

        if self.data is None:
            self.data = target
        elif isinstance(self.data, list):
            self.data.append(target)
        else:
            assert(isinstance(self.data, (luigi.LocalTarget, s3.S3Target)))
            self.data = [self.data, target]

        return target

    def add_dependency(self, bundle):
        """ Add an upstream bundle as a dependency

        Args:
            bundle(`api.Bundle`): Another bundle that may have been used to produce this one
        """
        self.depends_on.append((bundle.processing_name, bundle.uuid))

    def _set_processing_name(self):
        """ Set a processing name that may be used to identify bundles that
        were created in the same way -- they used the same task and task paramaters.
        In cases where Luigi tasks create bundles, this is the luigi.Task.taskid()
        Here we use a wrapper luigi task to do the same.
        Note that we assume you have placed your parameters as tags.

        Returns:
            processing_name(str)

        """
        wrapper_task = BundleWrapperTask(name=self.name,
                                         owner=self.owner,
                                         tags=self.tags)

        self.pb.processing_name = wrapper_task.pipe_id()

        return self.pb.processing_name


def _get_context(context_name):
    """Retrieve data context given name.   Raise exception if not found.

    Args:
        context_name(str): <remote context>/<local context> or <local context>

    Returns:
        (`disdat.data_context.DataContext`)
    """

    data_context = fs.get_context(context_name)

    if data_context is None:
        error_msg = "Unable to perform operation: could not find context {}".format(context_name)
        _logger.error(error_msg)
        raise RuntimeError(error_msg)

    return data_context


def current_context():
    """ Return the current context name (not object) """

    try:
        return fs.get_curr_context().get_local_name()
    except Exception as se:
        print(("Current context failed due to error: {}".format(se)))
        return None


def ls_contexts():
    """ Return list of contexts and their remotes

    Returns:
        List[Tuple(str,str)]: Return a list of tuples containing <local context>, <remote context>@<remote string>

    """
    # TODO: have the fs object provide a wrapper function
    return list(fs._all_contexts.keys())


def context(context_name):
    """ Create a new context

    Args:
        context_name(str): <remote context>/<local context> or <local context>

    Returns:
        (int) : 0 if successfully created branch, 1 if branch exists
    """

    return fs.branch(context_name)


def delete_context(context_name, remote=False, force=False):
    """ Delete a context

    Args:
        context_name (str): <remote context>/<local context> or <local context>
        remote (bool): Delete remote context as well, force must be True
        force (bool): Force deletion if dirty

    Returns:
        None
    """
    fs.delete_branch(context_name, remote=remote, force=force)


def switch(context_name):
    """ Stateful switch to a different context.
    This changes the current default context used by the CLI commands.

    Args:
        context_name(str): <remote context>/<local context> or <local context>

    Returns:
        None
    """
    fs.switch(context_name)


def remote(local_context, remote_context, remote_url, force=False):
    """ Add a remote to local_context

    Args:
        local_context (str): <remote context>/<local context> or <local context>
        remote_context (str):
        remote_url (str):
        force (bool):

    Returns:
        None

    """
    _logger.debug("Adding remote context {} at URL {} on local context '{}'".format(remote_context,
                                                                                    remote_url,
                                                                                    local_context))

    # Be generous and fix up S3 URLs to end on a directory.
    remote_url = '{}/'.format(remote_url.rstrip('/'))

    data_context = _get_context(local_context)

    data_context.bind_remote_ctxt(remote_context, remote_url, force=force)


def search(local_context, search_name=None, search_tags=None,
           is_committed=None, find_intermediates=False, find_roots=False,
           before=None, after=None):
    """ Search for bundle in a context.
    Allow for searching by human name, is_committed, is intermediate or is root task output, and tags.

    At this time the SQL interface in disdat.fs does not allow searching for entries without particular tags.

    So we loop through results to ensure we get the right results.

    Args:
        local_context (str): <remote context>/<local context> or <local context>
        search_name: May be None.  Interpret as a simple regex (one kleene star)
        search_tags (bool): A set of key:values the bundles must have
        is_committed (bool): If None (default): ignore committed, If True return committed, If False return uncommitted
        find_intermediates (bool):  Results must be intermediates
        find_roots (bool): Results must be final outputs
        before (str): bundles <= "12-1-2009" or "12-1-2009 12:13:42"
        after (str): bundles >= "12-1-2009" or "12-1-2009 12:13:42"

    Returns:
        [](Bundle): List of API bundle objects
    """

    results = []

    data_context = _get_context(local_context)

    if search_tags is None and (find_roots or find_intermediates):
        search_tags = {}

    if find_roots:
        search_tags.update({'root_task': True})

    if before is not None:
        before = disdat.fs._parse_date(before, throw=True)

    if after is not None:
        after = disdat.fs._parse_date(after, throw=True)

    for i, r in enumerate(data_context.get_hframes(human_name=search_name, tags=search_tags, before=before, after=after)):

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

        # We have filtered out
        bundle = Bundle(local_context, 'unknown')
        bundle.fill_from_hfr(r)
        results.append(bundle)

    return results


def get(local_context, bundle_name, uuid=None, tags=None):
    """ Get bundle from local context, with name, uuid, and tags.
    Return a Bundle object that contains both the hyperframe as well
    as a reference to a dataframe representation.

    This is a deconstructed fs.cat() call.

    Args:
        local_context: <remote context>/<local context> or <local context>
        bundle_name:
        uuid:
        tags:

    Returns:
        `api.Bundle`

    """

    data_context = _get_context(local_context)

    if uuid is None:
        hfr = fs.get_latest_hframe(bundle_name, tags=tags, data_context=data_context)
    else:
        hfr = fs.get_hframe_by_uuid(uuid, tags=tags, data_context=data_context)

    if hfr is None:
        return None

    b = Bundle(local_context, 'unknown')
    b.fill_from_hfr(hfr)

    return b


def rm(local_context, bundle_name=None, uuid=None, tags=None, rm_all=False, rm_old_only=False, force=False):
    """ Delete a bundle

    Args:
        local_context (str): Local context name
        bundle_name (str): Optional human-given name for the bundle
        uuid (str): Optional UUID for the bundle to remove.  Trumps bundle_name argument if both given
        tags (dict(str:str)): Optional dictionary of tags that must be present on bundle to remove
        rm_all (bool): Remove latest and all historical if given bundle_name
        rm_old_only (bool): remove everything but latest if given bundle_name
        force (bool): If a db-link exists and it is the latest on the remote DB, force remove. Default False.

    Returns:

    """

    data_context = _get_context(local_context)

    fs.rm(human_name=bundle_name, rm_all=rm_all, rm_old_only=rm_old_only,
          uuid=uuid, tags=tags, force=force, data_context=data_context)


def cat(local_context, bundle_name):
    """Get dataframe representation of bundle

    Args:
        local_context: <remote context>/<local context> or <local context>
        bundle_name: The human name of bundle to get df from

    Returns:
        (`pandas.DataFrame`)

    """

    data_context = _get_context(local_context)

    return fs.cat(bundle_name, data_context=data_context)


def commit(local_context, bundle_name, tags=None, uuid=None):
    """ Commit bundle in this local context.  Call commit on the bundle
    object after it has been closed / saved.

    Args:
        local_context (str): <remote context>/<local context> or <local context>
        bundle_name (str): The human name of the bundle to commit
        tags (dict(str:str)): Optional dictionary of tags with which to find bundle.
        uuid (str): UUID of the bundle to commit.
    """

    data_context = _get_context(local_context)

    if tags is None:
        tags = {}

    fs.commit(bundle_name, tags, uuid=uuid, data_context=data_context)


def push(local_context, bundle_name, tags=None, uuid=None):
    """ Push a bundle to a remote repository.

    Args:
        local_context (str): <remote context>/<local context> or <local context>
        bundle_name (str): human name of the bundle to push or None (if using uuid)
        tags (dict): Tags that bundle should have
        uuid (str): Optional UUID of the bundle to push.  If specified with bundle_name, UUID takes precedence.

    Returns:
        None

    """
    _logger.debug('disdat.api.push on bundle \'{}\''.format(bundle_name))

    data_context = _get_context(local_context)

    if data_context.get_remote_object_dir() is None:
        raise RuntimeError("Not pushing: Current branch '{}/{}' has no remote".format(data_context.get_repo_name(),
                                                                                      data_context.get_local_name()))

    fs.push(human_name=bundle_name, uuid=uuid, tags=tags, data_context=data_context)


def pull(local_context, bundle_name=None, uuid=None, localize=False):
    """ Pull bundles from the remote context into this local context.
    If there is no remote context associated with this context, then this is
    a no-op.  fs.pull will raise UserWarning if there is no local or remote context.

    Args:
        local_context (str): <remote context>/<local context> or <local context>
        bundle_name (str):
        uuid (str):
        localize (bool): Whether to bring linked files directly into bundle directory

    Returns:
        Bundle:
    """

    data_context = _get_context(local_context)

    fs.pull(human_name=bundle_name, uuid=uuid, localize=localize, data_context=data_context)


def apply(local_context, output_bundle, transform,
          input_tags=None, output_tags=None, force=False, params=None,
          output_bundle_uuid=None, central_scheduler=False, workers=1,
          incremental_push=False, incremental_pull=False):
    """ Execute a pipeline locally

    Args:
        local_context:
        output_bundle:
        transform:
        input_tags: optional tags dictionary for selecting input bundle
        output_tags: optional tags dictionary to tag output bundle
        force: Force re-running this transform, default False
        params: optional parameters dictionary
        output_bundle_uuid: Force UUID of output bundle
        central_scheduler (bool): Use a central scheduler, default False, i.e., use local scheduler
        workers (int): Number of workers, default 1.
        incremental_push (bool): commit and push task bundles as they complete
        incremental_pull (bool): localize bundles from remote as they are required by downstream tasks

    Returns:

    """

    data_context = _get_context(local_context)

    if input_tags is None:
        input_tags = {}

    if output_tags is None:
        output_tags = {}

    if params is None:
        params = {}

    task_params = json.dumps(params)

    # IF apply raises, let it go up.
    # If API, caller can catch.
    # If CLI, python will exit 1
    result = disdat.apply.apply(output_bundle, task_params, transform,
                                input_tags, output_tags, force,
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


def run(local_context,
        remote_context,
        output_bundle,
        pipe_cls,
        pipeline_args,
        remote=None,
        backend=Backend.default(),
        input_tags=None,
        output_tags=None,
        force=False,
        no_pull=False,
        no_push=False,
        no_push_int=False,
        vcpus=2,
        memory=4000,
        workers=1,
        no_submit=False,
        aws_session_token_duration=42300,
        job_role_arn=None):
    """ Execute a pipeline in a container.  Run locally, on AWS Batch, or AWS Sagemaker
    _run() finds out whether we have a

    Args:
        local_context (str):  The local context in which the pipeline will run in the container
        remote_context (str): The remote context to pull / push bundles during execution
        output_bundle (str): human name of output bundle
        pipe_cls (str): The pkg.module.class of the root of the pipeline DAG
        pipeline_args (dict): Dictionary of the parameters of the root task
        remote (str): The remote's S3 path
        input_tags (dict): str:str dictionary of tags required of the input bundle
        output_tags (dict): str:str dictionary of tags placed on all output bundles (including intermediates)
        force (bool):  Currently not respected.  But should re-run the entire pipeline no matter prior outputs
        no_pull (bool): Do not pull before execution
        no_push (bool): Do not push any output bundles after task execution
        no_push_int (bool):  Do not push intermediate task bundles after execution
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

    context = "{}/{}".format(remote_context, local_context) # remote_name/local_name

    retval = run_entry(output_bundle=output_bundle,
                       pipeline_args=pipeline_arg_list,
                       pipe_cls=pipe_cls,
                       backend=backend,
                       input_tags=input_tags,
                       output_tags=output_tags,
                       force=force,
                       context=context,
                       remote=remote,
                       no_pull=no_pull,
                       no_push=no_push,
                       no_push_int=no_push_int,
                       vcpus=vcpus,
                       memory=memory,
                       workers=workers,
                       no_submit=no_submit,
                       job_role_arn=job_role_arn,
                       aws_session_token_duration=aws_session_token_duration)

    return retval


def dockerize(setup_dir,
              pipe_cls,
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
        pipe_cls (str): The package.module.class string of the pipeline root.
        config_dir (str): The directory containing the configuration of .deb packages
        build (bool): If False, just copy files into the Docker build context without building image.
        push (bool):  Push the container to the repository
        sagemaker (bool): Create a Docker image executable as a SageMaker container (instead of a local / AWSBatch container).

    Returns:
        (int): 0 if success, 1 on failure

    """

    retval = dockerize_entry(pipe_root=setup_dir,
                             pipe_cls=pipe_cls,
                             config_dir=config_dir,
                             os_type=None,
                             os_version=None,
                             build=build,
                             push=push,
                             sagemaker=sagemaker)

    return retval


if __name__ == '__main__':
    b = get('careops5','tto_model_discovery')
