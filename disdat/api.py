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

_logger = logging.getLogger(__name__)

disdat.fs.DisdatConfig.instance()

fs = disdat.fs.DisdatFS()


def set_aws_profile(aws_profile):
    os.environ['AWS_PROFILE'] = aws_profile


def contexts():
    """

    Returns:

    """

    # TODO: have the fs object provide a wrapper function
    return [ ctxt for ctxt in fs._all_contexts.keys()]


def bundle(data=None, tags=None):
    """

    Args:
        data:
        tags:

    Returns:

    """
    raise NotImplementedError


def remote(remote_ctxt, s3_url):
    """

    Args:
        remote_ctxt:
        s3_url:

    Returns:

    """
    raise NotImplementedError


def add(ctxt, bndl):
    """

    Args:
        ctxt:
        bndl:

    Returns:

    """
    raise NotImplementedError


def ls(ctxt):
    """

    Args:
        ctxt:
        bndl:

    Returns:

    """

    try:
        prior_context_name = fs.checkout(ctxt, save=False)
    except Exception as e:
        _logger.error("Unable to perform apply: {} ".format(e))
        return

    try:
        results = fs.ls(None, False)
    finally:
        # Restore context.  Not strictly necessary but keeps consistent with state on disk.
        _ = fs.checkout(prior_context_name, save=False)

    return results


def commit(ctxt, bndl, tags=None):
    """

    Args:
        ctxt:
        bndl:
        tags:

    Returns:

    """
    raise NotImplementedError


def bundle_to_df(bndl):
    """

    Args:
        bndl:

    Returns:

    """
    raise NotImplementedError


def pull(remote, bndl, uuid, tags):
    """

    Args:
        remote:
        bndl:
        uuid:
        tags:

    Returns:

    """
    raise NotImplementedError

def push(remote, bndl, uuid, tags):
    """

    Args:
        remote:
        bndl:
        uuid:
        tags:

    Returns:

    """
    raise NotImplementedError


def apply(ctxt, input_bundle, output_bundle, transform, input_tags=None, output_tags=None, force=False, params=None):
    """
    Similar to apply.main() but we create our inputs and supply the context
    directly.

    Args:
        ctxt:
        input_bundle:
        output_bundle:
        transform:
        input_tags: optional tags dictionary for selecting input bundle
        output_tags: optional tags dictionary to tag output bundle
        force: Force re-running this transform, default False
        params: optional parameters dictionary

    Returns:

    """
    try:
        prior_context_name = fs.checkout(ctxt, save=False)
    except Exception as e:
        _logger.error("Unable to perform apply: {} ".format(e))
        return

    if input_tags is None:
        input_tags = {}
    if output_tags is None:
        output_tags = {}

    try:
        if params is None:
            params = {}

        dynamic_params = json.dumps(params)
        disdat.apply.apply(input_bundle, output_bundle, dynamic_params, transform, input_tags, output_tags, force,
                           sysexit=False)
    except SystemExit as se:
        print "SystemExist caught: {}".format(se)
    except Exception as e:
        print "Exception in apply: {}".format(e)
    finally:
        # Restore context.
        _ = fs.checkout(prior_context_name, save=False)


def run(ctxt, input_bundle, output_bundle, transform, input_tags, output_tags, force=False, **kwargs):
    """

    Args:
        ctxt:
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
