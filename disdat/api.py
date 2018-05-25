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
            name (str): Human name for this output dataset
        """

        try:
            self.data_context = fs.get_context(local_context)
        except Exception as e:
            _logger.error("Unable to allocate bundle in context: {} ".format(local_context, e))
            return

        self.name = name
        self.local_dir = None
        self.remote_dir = None

        self.open = False
        self.closed = False

        self.input_data = None  # The df, array, dictionary the user wants to store
        self.input_tags = {}    # The dictionary of (str):(str) tags the user wants to attach
        self.db_targets = []    # list of created db targets
        self.data = None

        self.uuid = None        # Internal uuid of this bundle.
        self.hfr = None

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

            self.input_tags.update({'MadeByTask': 'False'})

            hfr.replace_tags(self.input_tags)

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

    def push(self):
        """

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
        self.input_tags[key] = value

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

def contexts():
    """ Return list of contexts and their remotes

    Returns:
        List[Tuple(str,str)]: Return a list of tuples containing <local context>, <remote context>@<remote string>

    """

    # TODO: have the fs object provide a wrapper function
    return [ctxt for ctxt in fs._all_contexts.keys()]


def remote(local_context, remote_context, s3_url, force=False):
    """ Add a remote to a context

    Args:
        local_context:
        remote_context:
        s3_url:

    Returns:
        bool: True on success, False otherwise

    """
    raise NotImplementedError


def ls(local_context, search_name=None, print_tags=False, print_intermediates=False, print_long=False, tags=None):
    """ Search for bundle in a context.

    Args:
        local_context (str): Context in which to perform ls
        search_name: May be None.  Interpret as a simple regex (one kleene star)
        print_tags (bool): Whether to print the bundle tags
        print_intermediates (bool): Whether to show intermediate bundles
        tags: Optional. A dictionary of tags to search for.

    Returns:

    """

    data_context = fs.get_context(local_context)

    if data_context is None:
        _logger.error("Unable to perform ls: couldn't find context {}".format(local_context))
        return

    results = fs.ls(search_name, print_tags, print_intermediates, print_long, tags=tags)

    return results


def get(local_context, bundle_name, uuid=None, tags=None):
    """ Get bundle from local context, with name, uuid, and tags.
    Return a Bundle object that contains both the hyperframe as well
    as a reference to a dataframe representation.

    This is a deconstructed fs.cat() call.

    Args:
        local_context:
        bundle_name:
        uuid:
        tags:

    Returns:
        `api.Bundle`

    """

    data_context = fs.get_context(local_context)

    if data_context is None:
        _logger.error("Unable to perform bundle_to_df: couldn't find context {}".format(local_context))
        return

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


def commit(local_context, bundle_name,  tags=None):
    """ Commit bundle in this local context.  Call commit on the bundle
    object after it has been closed / saved.

    Args:
        bundle_name (str): The human name of the bundle to commit
        local_context (str): The local context to use on add.
        tags (dict(str:str)): Optional dictionary of tags with which to find bundle.

    """

    data_context = fs.get_context(local_context)

    if data_context is None:
        _logger.error("Unable to perform commit: {} ".format(e))
        return

    if tags is None:
        tags = {}

    fs.commit(bundle_name, tags, data_context=data_context)


def pull(local_context, bundle_name=None, uuid=None, localize=False):
    """

    Args:
        local_context (str):
        bundle_name (str):
        uuid:
        localize (bool): Whether to bring linked files directly into bundle directory

    Returns:
        Bundle:
    """

    data_context = fs.get_context(local_context)

    if data_context is None:
        _logger.error("Unable to perform commit: {} ".format(e))
        return

    fs.pull(human_name=bundle_name, uuid=uuid, localize=localize, data_context=data_context)


def apply(local_context, input_bundle, output_bundle, transform,
          input_tags=None, output_tags=None, force=False, params=None,
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
        central_scheduler (bool): Use a central scheduler, default False, i.e., use local scheduler
        workers (int): Number of workers, default 1.

    Returns:

    """

    data_context = fs.get_context(local_context)

    if data_context is None:
        _logger.error("Unable to perform apply: {} ".format(local_context))
        return

    if input_tags is None:
        input_tags = {}

    if output_tags is None:
        output_tags = {}

    try:

        if params is None:
            params = {}

        dynamic_params = json.dumps(params)
        disdat.apply.apply(input_bundle, output_bundle, dynamic_params, transform,
                           input_tags, output_tags, force,
                           sysexit=False, central_scheduler=central_scheduler,
                           workers=workers, data_context=data_context)

    except SystemExit as se:
        print "SystemExist caught: {}".format(se)

    except Exception as e:
        print "Exception in apply: {}".format(e)


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
