#!/usr/bin/env python
"""
Entry point for pipelines run within Docker images.

@author: twong / kyocum
@copyright: Human Longevity, Inc. 2017
@license: Apache 2.0
"""

import argparse
import disdat.apply
import disdat.common
import disdat.fs
import json
import logging
import os
import pandas as pd
import sys
import tempfile

from multiprocessing import Process


_PIPELINE_CLASS_ENVVAR = 'PIPELINE_CLASS'


_HELP = """ Run a Disdat pipeline. This script wraps up several of the
steps required to run a pipeline, including: creating a working branch
inside a Disdat context, applying a pipeline class to an input bundle to
generate an output bundle, and pushing an output bundle to a Disdat remote.
"""

_logger = logging.getLogger(__name__)


def _add(fs, bundle_name, input_path):
    """Add a file as a bundle to a Disdat branch.

    :param fs: A Disdat file system handle.
    :param bundle_name: The name of the bundle
    :param input_path: The path to the file
    """
    _logger.debug("Adding file '{}' as bundle '{}'".format(input_path, bundle_name))
    if not os.path.exists(input_path):
        _logger.error('Failed to find input file {} for bundle {}: Not copied into shared volume?'.format(input_file, bundle_name))
        raise RuntimeError()
    # We have to spawn the add task as a child process otherwise the entire
    # run process will exit after we complete adding the bundle.
    def _inner():
        fs.add(bundle_name, input_path)
        fs._context = disdat.fs.DataContext.load(fs.disdat_config.get_meta_dir())
    p = Process(target=_inner)
    p.start()
    p.join()
    return True


def _apply(input_bundle_name, output_bundle_name, pipeline_class_name, pipeline_args,
           input_tags, output_tags, output_bundle_uuid=None, force=False):
    """Apply a pipeline to an input bundle, and save the results in an
    output bundle.

    Args:
        input_bundle_name: The human name of the input bundle
        output_bundle_name: The human name of the output bundle
        pipeline_class_name: Name of the pipeline class to run
        pipeline_args: Optional arguments to pass to the pipeline class
        pipeline_args: list
        input_tags: Set of tags to find input bundle
        output_tags: Set of tags to give the output bundle
        output_bundle_uuid: A UUID specifying the version to save within
        the output bundle; default `None`
        force: If `True` force recomputation of all upstream pipe requirements
    """
    _logger.debug("Applying '{}' to '{}' to get '{}'".format(pipeline_class_name, input_bundle_name, output_bundle_name))

    apply_kwargs = {
        'input_bundle': input_bundle_name,
        'output_bundle': output_bundle_name,
        'output_bundle_uuid': output_bundle_uuid,
        'pipe_params': json.dumps(disdat.common.parse_params(pipeline_args)),
        'pipe_cls': pipeline_class_name,
        'input_tags': input_tags,
        'output_tags': output_tags,
        'force': force
    }
    p = Process(target=disdat.apply.apply, kwargs=apply_kwargs)
    p.start()
    p.join()
    return p.exitcode == 0


def _branch_and_checkout(fs, branch_name=None):
    """Create and check out a new Disdat branch.

    :param fs: A Disdat file system handle.
    :param branch_name: A fully-qualified branch name.
    """
    if branch_name is None:
        _logger.error("Got an invalid branch name '{}'".format(branch_name))
        return False
    if len(branch_name.split('/')) <= 1:
        _logger.error("Got a partial branch name: Expected <context>/<branch-name>, got branch name '{}' with no context".format(branch_name))
        return False
    # These operations are idempotent, so if the context and branch already
    # exist these become no-ops.
    fs.branch(branch_name)
    fs.checkout(branch_name)
    return True


def _cat(fs, bundle_name):
    """Dump the contents of a bundle

    :param fs: A Disdat file system handle.
    :param bundle_name: The name of the bundle
    :return: the presentable contents of the bundle, if such contents exists
    """
    return fs.cat(bundle_name)


def _commit(fs, bundle_name, input_tags):
    """

    Args:
        fs:
        bundle_name (str):
        input_tags (dict): Tags the committed bundle must have.

    Returns:

    """
    _logger.debug("Committing '{}'".format(bundle_name))

    # We have to spawn the commit task as a child process otherwise the
    # entire run process will exit after we complete committing the bundle.

    def _inner():
        fs.commit(bundle_name, input_tags)

    p = Process(target=_inner)
    p.start()
    p.join()
    return True


def _pull(fs, bundle_name):
    _logger.debug("Pulling '{}'".format(bundle_name))
    context = fs.get_curr_context()
    if context is None:
        _logger.error('Not pulling: No current context')
        return False
    if context.get_remote_object_dir() is None:
        _logger.error("Not pulling: Current branch '{}/{}' has no remote".format(context.get_repo_name(), context.get_local_name()))
        return False
    fs.pull(human_name=bundle_name, localize=True)
    return True


def _push(fs, bundle_name, force_uuid=None):
    """Push a bundle to a remote repository.
    """
    _logger.debug('Pushing \'{}\''.format(bundle_name))
    context = fs.get_curr_context()
    if context is None:
        _logger.error('Not pushing: No current context')
        return False
    if context.get_remote_object_dir() is None:
        _logger.error("Not pushing: Current branch '{}/{}' has no remote".format(context.get_repo_name(), context.get_local_name()))
        return False
    fs.push(human_name=bundle_name, force_uuid=force_uuid)
    return True


def _remote(fs, remote_url, context_or_branch_name=None):
    _logger.debug("Adding remote at URL {} for branch '{}'".format(remote_url, context_or_branch_name))
    if context_or_branch_name is None:
        context = fs.get_curr_context()
        if context is None:
            _logger.error('Not pushing: No current context')
            return False
        context_name = context.get_repo_name()
    else:
        context_name = context_or_branch_name.split('/')[0]
    if remote_url is None:
        _logger.error("Got an invalid URL {}".format(remote_url))
        return False
    else:
        # Be generous and fix up S3 URLs to end on a directory.
        remote_url = '{}/'.format(remote_url.rstrip('/'))
    try:
        fs.remote_add(context_name, remote_url, force=True)
    except Exception:
        return False
    return True


def _remove(fs, bundle_name):
    """Remove all versions of a bundle from a Disdat branch.

    :param fs: A Disdat file system handle.
    :param bundle_name: The name of the bundle
    :return: a list of all removed bundle versions
    :rtype: list
    """
    return fs.rm(bundle_name, rm_all=True)


def add_argument_help_string(help_string, default=None):
    if default is None:
        return '{}'.format(help_string)
    else:
        return "{} (default '{}')".format(help_string, default)


if __name__ == '__main__':
    # To simplify configuring and building pipeline images, we keep all
    # of the various defaults parameter values in the Docker image makefile,
    # and pass them on as Docker ENV variables.
    _pipeline_class_default = os.environ[_PIPELINE_CLASS_ENVVAR] if _PIPELINE_CLASS_ENVVAR in os.environ else None

    parser = argparse.ArgumentParser(
        description=_HELP,
    )

    parser.add_argument(
        '--input-json',
        default=None,
        type=str,
        help='JSON-encoded data to load as the input bundle',
    )
    parser.add_argument(
        '--dump-output',
        help='Dump the output to standard output',
        action='store_true',
    )
    parser.add_argument(
        '--debug-level',
        default=logging.INFO,
        help='The debug logging level (default {})'.format(logging.getLevelName(logging.WARNING))
    )

    disdat_parser = parser.add_argument_group('remote repository arguments')
    disdat_parser.add_argument(
        '--remote',
        type=str,
        required=True,
        help='The s3 bucket from/to which to pull/push data',
    )
    disdat_parser.add_argument(
        '--no-pull',
        action='store_true',
        help='Do not pull the input bundle from the remote repository (default is to pull)',
    )
    disdat_parser.add_argument(
        '--no-push',
        action='store_true',
        help='Do not push the output bundle to the remote repository (default is to push)',
    )

    pipeline_parser = parser.add_argument_group('pipe arguments')
    pipeline_parser.add_argument(
        '--pipeline',
        default=_pipeline_class_default,
        type=str,
        required=(_pipeline_class_default is None),
        help=add_argument_help_string('Name of the pipeline class to run', _pipeline_class_default),
    )
    pipeline_parser.add_argument(
        '--branch',
        type=str,
        required=True,
        help='The fully-qualified Disdat branch to use when running',
    )
    pipeline_parser.add_argument(
        '--input-tags',
        type=str,
        help='A JSON-encoded dictionary of tags to choose input bundle',
    )
    pipeline_parser.add_argument(
        '--output-tags',
        type=str,
        help='A JSON-encoded dictionary of tags to attach to the output bundle',
    )
    pipeline_parser.add_argument(
        '--output-bundle-uuid',
        default=None,
        type=str,
        help='UUID for the output bundle (default is for apply to generate a UUID)',
    )
    pipeline_parser.add_argument(
        '--force',
        action='store_true',
        help='Force recomputation of all pipe dependencies (default is to recompute dependencies with changed inputs or code)',
    )
    pipeline_parser.add_argument(
        'input_bundle',
        type=str,
        help='Name of the input bundle',
    )
    pipeline_parser.add_argument(
        'output_bundle',
        type=str,
        help='Name for the output bundle',
    )
    pipeline_parser.add_argument(
        "pipeline_args",
        nargs=argparse.REMAINDER,
        type=str,
        help='One or more optional arguments to pass on to the pipeline class, of the form \'--param-name param-value\'; note that parameter values are NOT optional!',
    )
    args = parser.parse_args()

    logging.basicConfig(level=args.debug_level)
    _logger.setLevel(args.debug_level)

    # Check to make sure that we have initialized the Disdat environment
    if not os.path.exists(os.path.join(os.environ['HOME'], '.config', 'disdat')):
        _logger.warning('Disdat environment possibly uninitialized?')
    # Get a Disdat file system handle and create the branch if necessary.
    fs = disdat.fs.DisdatFS()
    if not _branch_and_checkout(fs, args.branch):
        _logger.error('Failed to branch and check out \'{}\''.format(args.branch))
        sys.exit(os.EX_IOERR)
    # If we received JSON, convert it into a temporary tab-separated file,
    # and use that as the input bundle.
    if args.input_json is not None:
        with tempfile.NamedTemporaryFile(suffix='.tsv') as input_file:
            # To be nice, strip newlines and any single-quotes left over by
            # the shell
            input_json = args.input_json.strip('\'\n')
            _logger.debug('Adding JSON {}'.format(input_json))
            input_path = input_file.name
            _logger.debug('Saving JSON data to temporary file {}'.format(input_file.name))
            pd.read_json(input_json).to_csv(input_path, sep='\t')
            if not _add(fs, bundle_name=args.input_bundle, input_path=input_file.name):
                _logger.error('Failed to add JSON to input bundle \'{}\''.format(args.input))
                sys.exit(os.EX_IOERR)

    # If specified, decode the JSON-encoded tags.
    input_tags = {}
    if args.input_tags is not None:
        input_tags = json.loads(args.input_tags)
    output_tags = {}
    if args.output_tags is not None:
        output_tags = json.loads(args.output_tags)

    if False:
        print "Container Running with command (output uuid {}, input_tags {}, output_tags {}):".format(args.output_bundle_uuid,
                                                                                                   input_tags, output_tags)
        print "\t dsdt apply {} {} {} {} ".format(args.input_bundle, args.output_bundle, args.pipeline, args.pipeline_args)

    # Let it rip!
    if (
        ((args.no_pull and args.no_push) or (args.remote is None) or _remote(fs, args.remote)) and
        (args.no_pull or (args.input_json is not None) or _pull(fs, bundle_name=args.input_bundle)) and
        _apply(
            input_bundle_name=args.input_bundle,
            output_bundle_name=args.output_bundle,
            pipeline_class_name=args.pipeline,
            pipeline_args=args.pipeline_args,
            input_tags=input_tags,
            output_tags=output_tags,
            output_bundle_uuid=args.output_bundle_uuid,
            force=args.force,
        ) and
        _commit(fs, args.output_bundle, output_tags) and
        (args.no_push or _push(fs, args.output_bundle))
    ):
        if args.dump_output:
            print(_cat(fs, args.output_bundle))
        sys.exit(os.EX_OK)
    else:
        _logger.error('Failed to run pipeline')
        sys.exit(os.EX_IOERR)
