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

import disdat.apply
import disdat.run
import disdat.fs
import disdat.common
from disdat.pipe_base import PipeBase
from disdat.db_target import DBTarget

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


class Bundle(object):

    def __init__(self, local_context):
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
        """

        try:
            self.data_context = fs.get_context(local_context)
        except Exception as e:
            _logger.error("Unable to allocate bundle in context: {} ".format(local_context, e))
            return

        self.name = None
        self.processing_name = None
        self.owner = None

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
        self.uuid = hfr.pb.uuid
        self.presentation = hfr.pb.presentation

        self.data = self.data_context.convert_hfr2df(hfr)
        self.tags = hfr.tag_dict

        self._hfr = hfr

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
        return

    def close(self):
        """

        Returns:
            None
        """

        try:

            hfr = PipeBase.parse_pipe_return_val(self.uuid, self.data, self.data_context, human_name=self.name)

            self.tags.update({'APIBundle': 'True'})

            hfr.replace_tags(self.tags)

            self.data_context.write_hframe(hfr)

        except Exception as error:
            """ If we fail for any reason, remove bundle dir and raise """
            PipeBase.rm_bundle_dir(self.local_dir, self.uuid, self.db_targets)
            raise

        self.open = False
        self.closed = True

        return hfr

    def add(self, data):
        """ Add data to a bundle.   The bundle must be open and not closed.
            One adds one data item to a bundle (dictionary, list, tuple, scalar, or dataframe).
            Calling this replaces the latest item -- only the latest will be included in the bundle on close.

        Args:
            data (list|tuple|scalar|`pandas.DataFrame`):

        Returns:
            None
        """
        self.data = data

    def to_df(self):
        if self.closed:
            return self.df
        else:
            _logger.warn("Close bundle to convert to dataframe")
            return None

    def commit(self):
        """
        TODO: should do this or use the api below?  If we have the HFR we don't have to find it.

        Returns:

        """
        raise NotImplementedError

    def push(self):
        """
        TODO: should do this or use the api below?  If we have the HFR we don't have to find it.

        Args:
            remote:
            bndl:
            uuid:
            tags:

        Returns:
            (`disdat.api.Bundle`): this bundle

        """
        raise NotImplementedError

    def tag(self, key, value):
        """ Add tag to our set of input tags

        Args:
            key (str): The tag key
            value (str): The tag value

        Returns:

        """
        if not self.open:
            _logger.warn("Open the bundle to modify tags.")
            return
        if self.closed:
            _logger.warn("Unable to modify tags in a closed bundle.")
            return
        self.tags[key] = value

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

    def make_file(self, name):
        """
        Returns a path to a disdat managed file.

        See Pipe.create_output_file()

        Arguments:
            name (str): The human readable name to a file reference

        Returns:
            str: A file path managed by disdat
        """
        assert (self.open and not self.closed)

        return self.make_luigi_targets_from_basename(filename)


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

    if hfr is not None:
        df = data_context.convert_hfr2df(hfr)
    else:
        df = None

    b = Bundle(bundle_name)
    b.df = df
    b.uuid = hfr.pb.uuid
    b.hfr = hfr
    b.data_context = data_context
    b.closed = True

    return b


def rm(local_context, bundle_name, uuid=None):
    """ Delete a bundle

    Args:
        local_context:
        bundle_name:
        uuid:

    Returns:

    """



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
        bundle_name (str): human name of the bundle to push
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
          input_tags=None, output_tags=None, force=False, params=None, output_bundle_uuid=None,
          central_scheduler=False, workers=1):
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

    Returns:

    """

    data_context = _get_context(local_context)

    if input_tags is None:
        input_tags = {}

    if output_tags is None:
        output_tags = {}

    if params is None:
        params = {}

    try:
        dynamic_params = json.dumps(params)
        disdat.apply.apply(input_bundle, output_bundle, dynamic_params, transform,
                           input_tags, output_tags, force, output_bundle_uuid=output_bundle_uuid,
                           sysexit=False, central_scheduler=central_scheduler,
                           workers=workers, data_context=data_context)

    except SystemExit as se:
        print "SystemExit caught: {}".format(se)


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
    apply('tester', 'demo', 'treeout', 'pipelines.simple_tree.SimpleTree', force=True)
    apply('tester', 'demo', 'treeout', 'pipelines.simple_tree.SimpleTree', force=True)
