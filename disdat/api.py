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
api

A Disdat API for creating and publishing bundles.

These calls are note thread safe.  If they operate on a context, they
require the user to specify the context.   This is unlike the CLI that maintains state on disk
that keeps track of your current context between calls.   The API won't change the context you're in
in the CLI and vice versa.

To make this work we get a pointer to the singelton FS object, and we temporarily change the context (it will
automatically assume the context of the CLI) and perform our operation.


import disdat as dsdt

dsdt.contexts()
dsdt.bundle(data=None, tags=None)
dsdt.remote(remote_ctxt, s3_url)
dsdt.add(ctxt, bndl)
dsdt.ls(ctxt, bndl)
dsdt.commit(ctxt, bndl, tags=None)
dsdt.bundle_to_df(bndl)
dsdt.pull(remote, bndl, uuid, tags)
dsdt.push(remote, bndl, uuid, tags)
dsdt.apply(in, out, transform, in_tags, out_tags, force, **kwargs)
dsdt.run(in, out, transform, in_tags, out_tags, force, **kwargs)

Author: Kenneth Yocum
"""


import logging
import os
import json
import luigi

import disdat.apply
import disdat.run
import disdat.fs
import disdat.common as common
from disdat.pipe_base import PipeBase
from disdat.db_target import DBTarget
from disdat.pipe import PipeTask

_logger = logging.getLogger(__name__)

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
    creation_date = luigi.Parameter(default=None)

    def bundle_inputs(self):
        """ Determine input bundles

        Returns
          [(processing_name, uuid), ... ]

        """
        return []


class Bundle(object):

    def __init__(self, local_context, name):
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

        self.name = name
        self.processing_name = None
        self.owner = None
        self.creation_date = None

        self.local_dir = None
        self.remote_dir = None

        self.open = False
        self.closed = False

        self.input_data = None  # The df, array, dictionary the user wants to store
        self.tags = {}    # The dictionary of (str):(str) tags the user wants to attach
        self.db_targets = []    # list of created db targets
        self.data = None

        self.uuid = None        # Internal uuid of this bundle.
        self.presentation = None
        self._hfr = None        # debating whether to include this

    def fill_from_hfr(self, hfr):
        """ Given an internal hyperframe, copy out the information to this user-side Bundle object.

        Args:
            hfr:

        Returns:
            None

        """

        self.open = False
        self.closed = True

        self.name = hfr.pb.human_name
        self.processing_name = hfr.pb.processing_name
        self.owner = hfr.pb.owner
        self.creation_date = hfr.pb.lineage.creation_date
        self.uuid = hfr.pb.uuid
        self.presentation = hfr.pb.presentation

        self.data = self.data_context.convert_hfr2df(hfr)
        self.tags = hfr.tag_dict

        self._hfr = hfr

    def __enter__(self):
        """ 'open'
        """
        return self.open()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """ 'close'
        If there has been an exception, let the user deal with the written created bundle.
        """
        self.close()

    def open(self):
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
            self.local_dir, self.uuid, self.remote_dir = self.data_context.make_managed_path()
            self.open = True
        else:
            _logger.warn("Bundle is closed -- unable to re-open.")
        return self

    def close(self):
        """

        Returns:
            None
        """

        try:

            # only sets it in the object
            self._set_processing_name()

            def parse_api_return_val(hfid, val, data_context, bundle_inputs, bundle_name, bundle_processing_name, pipe):

            hfr = PipeBase.parse_api_return_val(self.uuid,
                                                self.data,
                                                self.data_context,
                                                self.bundle_inputs(),
                                                self.name,
                                                self.processing_name,
                                                )

            hfr.replace_tags(self.tags)

            self.data_context.write_hframe(hfr)

            self._hfr = hfr

            self.creation_date = hfr.pb.lineage.creation_date

        except Exception as error:
            """ If we fail for any reason, remove bundle dir and raise """
            PipeBase.rm_bundle_dir(self.local_dir, self.uuid, self.db_targets)
            raise

        self.open = False
        self.closed = True

        return self

    def add_data(self, data):
        """ Add data to a bundle.   The bundle must be open and not closed.
            One adds one data item to a bundle (dictionary, list, tuple, scalar, or dataframe).
            Calling this replaces the latest item -- only the latest will be included in the bundle on close.

        Args:
            data (list|tuple|dict|scalar|`pandas.DataFrame`):

        Returns:
            self
        """
        self.data = data
        return self

    def to_df(self):
        if self.closed:
            return self.df
        else:
            _logger.warn("Close bundle to convert to dataframe")
            return None

    def rm(self):
        """ Remove bundle from the current context associated with this bundle object
        Only remove this bundle with this uuid.
        This only makes sense if the bundle is closed.
        """
        assert(self.closed and not self.open)
        fs.rm(uuid=self.uuid, data_context=self.data_context)
        self._hfr = None
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

        if self.data_context.get_remote_object_dir() is None:
            raise RuntimeError("Not pushing: Current branch '{}/{}' has no remote".format(self.data_context.get_repo_name(),
                                                                                          self.data_context.get_local_name()))

        fs.push(uuid=self.uuid, data_context=self.data_context)

        return self

    def pull(self, localize=False):
        """ Shortcut version of api.pull()
        """
        assert( (not self.open and not self.closed) or (not self.open and self.closed) )
        fs.pull(uuid=self.uuid, localize=localize, data_context=self.data_context)
        return self

    def tag(self, key, value):
        """ Add tag to our set of input tags

        Args:
            key (str): The tag key
            value (str): The tag value

        Returns:
            self
        """
        if not self.open:
            _logger.warn("Open the bundle to modify tags.")
            return
        if self.closed:
            _logger.warn("Unable to modify tags in a closed bundle.")
            return
        self.tags[key] = value
        return self

    def db_table(self, dsn, table_name, schema_name):
        """

        Args:
            dsn (unicode):
            table_name (unicode):
            schema_name (unicode):

        Returns:
            `disdat.db_target.DBTarget`

        """
        assert (self.open and not self.closed)

        db_target = DBTarget(None, dsn, table_name, schema_name, context=self.data_context)

        self.db_targets.append(db_target)

        return db_target

    def make_directory(self, dir_name):
        """
        Returns a path to a disdat managed directory.

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
        in the output.

        Arguments:
            filename (str,list,dict): filename to create in the bundle

        Returns:
            single, list, or dictionary of `luigi.LocalTarget` or `luigi.s3.S3Target`
        """
        assert (self.open and not self.closed)

        return PipeBase.filename_to_luigi_targets(self.local_dir, filename)

    def _set_processing_name(self):
        """ Set a processing name that may be used to identify bundles that
        were created in the same way -- they used the same task and task paramaters.
        In cases where Luigi tasks create bundles, this is the luigi.Task.taskid()
        Here we use a wrapper luigi task to do the same.
        Note that we assume you have placed your parameters as tags.
        """
        wrapper_task = BundleWrapperTask(name=self.name,
                                         owner=self.owner,
                                         tags=self.tags,
                                         creation_date=self.creation_date)

        self.processing_name = wrapper_task.pipe_id()


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
    except StandardError as se:
        print ("Current context failed due to error: {}".format(se))
        return None


def ls_contexts():
    """ Return list of contexts and their remotes

    Returns:
        List[Tuple(str,str)]: Return a list of tuples containing <local context>, <remote context>@<remote string>

    """
    # TODO: have the fs object provide a wrapper function
    return [ctxt for ctxt in fs._all_contexts.keys()]


def context(context_name):
    """ Create a new context

    Args:
        context_name(str): <remote context>/<local context> or <local context>

    Returns:
        None
    """

    fs.branch(context_name)


def delete_branch(context_name, force=False):
    """ Delete a context

    Args:
        context_name (str): <remote context>/<local context> or <local context>
        force (bool): Force deletion if dirty

    Returns:
        None
    """
    fs.delete_branch(context_name, force=force)


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


def search(local_context, search_name=None, search_tags=None, is_committed=None, find_intermediates=False, find_roots=False):
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

    Returns:
        [](Bundle): List of API bundle objects
    """

    results = []

    data_context = _get_context(local_context)

    if search_tags is None and (find_roots or find_intermediates):
        search_tags = {}

    if find_roots:
        search_tags.update({'root_task': True})

    for i, r in enumerate(data_context.get_hframes(human_name=search_name, tags=search_tags)):

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
        bundle = Bundle(local_context)
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

    b = Bundle(local_context)
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


def apply(local_context, input_bundle, output_bundle, transform,
          input_tags=None, output_tags=None, force=False, params=None,
          output_bundle_uuid=None, central_scheduler=False, workers=1,
          incremental_push=False, incremental_pull=False):
    """
    Similar to apply.main() but we create our inputs and supply the context
    directly.

    Args:
        local_context:
        input_bundle:
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

    #result = {'success': False, 'did_work': False}

    dynamic_params = json.dumps(params)

    # IF apply raises, let it go up.
    # If API, caller can catch.
    # If CLI, python will exit 1
    result = disdat.apply.apply(input_bundle, output_bundle, dynamic_params, transform,
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


def run(local_context, input_bundle, output_bundle, transform, input_tags, output_tags, force=False, **kwargs):
    """

    Args:
        local_context:
        input_bundle:
        output_bundle:
        transform:
        input_tags:
        output_tags:
        force:
        **kwargs:

    Returns:

    """

    raise NotImplementedError


def _no_op():
    # pyinstaller hack for including api in single-image binary
    pass


if __name__ == '__main__':
    b = get('careops5','tto_model_discovery')
