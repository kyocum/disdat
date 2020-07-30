#!/usr/bin/env python
"""
Entry point for pipelines run within Docker images.

@author: twong / kyocum
@copyright: Human Longevity, Inc. 2017
@license: Apache 2.0
"""
from __future__ import print_function

import logging
import os
import sys

import argparse
import subprocess
import disdat.common
import disdat.fs
import disdat.api
from disdat import log

import boto3
from botocore.exceptions import ClientError

_HELP = """ Run a Disdat pipeline. This script wraps up several of the
steps required to run a pipeline, including: creating a working context, 
running a pipeline class to generate an output bundle, and pushing an 
output bundle to a Disdat remote.
"""

_logger = logging.getLogger(__name__)


def _context_and_remote(context_name, remote=None):
    """Create a new Disdat context and bind remote if not None.

    Check environment for 'LOCAL_EXECUTION', which should exist and be True if we are running
    a container in an existing .disdat environment (e.g., on someone's laptop).

    If so, do not take any actions that would change the state of the users CLI.  That is, do not
    switch contexts.

    Args:
        context_name (str): A fully-qualified context name. remote-context/local-context or local-context
        remote (str): S3 remote name.
    """

    retval = disdat.api.context(context_name)

    if retval == 1: # branch exists
        _logger.warn("Entrypoint found existing local context {} ".format(context_name))
        _logger.warn("Entrypoint not switching and ignoring directive to change to remote context {}".format(remote))
    elif retval == 0: # just made a new branch
        if remote is not None:
            _logger.info("Entrypoint made a new context {}, attaching remote {}".format(context_name, remote))
            _remote(context_name, remote)
    else:
        _logger.error("Entrypoint got non standard retval {} from api.context({}) command.".format(retval, context_name))
        return False

    if disdat.common.LOCAL_EXECUTION not in os.environ:
        disdat.api.switch(context_name)
    else:
        _logger.info("Container running locally (not in a cloud provider, aka AWS).  Not switching contexts")

    return True


def _remote(context_arg, remote_url):
    """ Add remote to our context.

    Args:
        context_arg:  <remote context>/<local context> or <local context> to use in this container
        remote_url: The remote to add to this local context

    Returns:
        None
    """
    _logger.debug("Adding remote at URL {} for branch '{}'".format(remote_url, context_arg))

    contexts = context_arg.split('/')

    if len(contexts) > 1:
        remote_context = contexts[0]
        local_context = contexts[1]
    else:
        local_context = contexts[0]
        remote_context = local_context

    if remote_url is None:
        _logger.error("Got an invalid URL {}".format(remote_url))
        return False

    try:
        disdat.api.remote(local_context, remote_context, remote_url)
    except Exception:
        return False
    return True


def retrieve_secret(secret_name):
    """ Placeholder for ability to retrieve secrets needed by image

    Returns:

    """

    raise NotImplementedError

    # Modify these to get them from the current environment
    endpoint_url = "https://secretsmanager.us-west-2.amazonaws.com"
    region_name = "us-west-2"

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name,
        endpoint_url=endpoint_url
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print("The requested secret " + secret_name + " was not found")
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            print(("The request was invalid due to:", e))
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            print(("The request had invalid params:", e))
    else:
        # Decrypted secret using the associated KMS CMK
        # Depending on whether the secret was a string or binary, one of these fields will be populated
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            binary_secret_data = get_secret_value_response['SecretBinary']

        print ("Found the secret string as ")
        print(secret)


def add_argument_help_string(help_string, default=None):
    if default is None:
        return '{}'.format(help_string)
    else:
        return "{} (default '{}')".format(help_string, default)


def _commit_and_push(b):
    """ commit and push bundle b if not transient """
    if disdat.common.BUNDLE_TAG_TRANSIENT not in b.tags:
        b.commit()
        b.push()


def run_disdat_container(args):
    """ Execute Disdat inside of container

    Args:
        args: input arguments

    Returns:
        None

    """
    print("Entrypoint running with args: {}".format(args))

    if args.remote is not None:
        response = boto3.client('sts').get_caller_identity()
        _logger.info("boto3 caller identity {}".format(response))
        incremental_pull = True   # running with a remote
    else:
        incremental_pull = False  # running without a remote

    print ("Entrypoint running with incremental_pull=={}".format(incremental_pull))

    # Check to make sure that we have initialized the Disdat environment
    if not os.path.exists(os.path.join(os.environ['HOME'], '.config', 'disdat')):
        _logger.warning("Disdat environment possibly uninitialized?")

    # Create context, add remote, and switch to it
    if not _context_and_remote(args.branch, args.remote):
        _logger.error("Failed to branch to \'{}\' and optionally bind  to \'{}\'".format(args.branch,
                                                                                         args.remote))
        sys.exit(os.EX_IOERR)

    # Pull the remote branch into the local branch or download individual items
    try:
        if not args.no_pull:
            disdat.api.pull(args.branch, localize=not incremental_pull)
    except Exception as e:
        _logger.error("Failed to pull and localize all bundles from context {} due to {}".format(args.branch, e))
        sys.exit(os.EX_IOERR)

    # If specified, decode the ordinary 'key:value' strings into a dictionary of tags.
    input_tags = disdat.common.parse_args_tags(args.input_tag)
    output_tags = disdat.common.parse_args_tags(args.output_tag)

    # Convert string of pipeline args into dictionary for api.apply
    deser_user_params = disdat.common.parse_params(args.pipe_cls, args.pipeline_args)

    # If the user wants final and intermediate, then inc push.
    if not args.no_push and not args.no_push_intermediates:
        incremental_push = True
    else:
        incremental_push = False

    try:
        result = disdat.api.apply(args.branch,
                                  args.pipe_cls,
                                  output_bundle=args.output_bundle,
                                  input_tags=input_tags,
                                  output_tags=output_tags,
                                  params=deser_user_params,
                                  output_bundle_uuid=args.output_bundle_uuid,
                                  force=args.force,
                                  force_all=args.force_all,
                                  workers=args.workers,
                                  incremental_push=incremental_push,
                                  incremental_pull=incremental_pull)

        if not incremental_push:
            if not args.no_push:
                if not args.no_push_intermediates:
                    to_push = disdat.api.search(args.branch, is_committed=False, find_intermediates=True)
                    for b in to_push:
                        _commit_and_push(b)
                if result['did_work']:
                    _logger.info("Pipeline ran.  Committing and pushing output bundle UUID {}.".format(args.output_bundle_uuid))
                    b = disdat.api.get(None, uuid=args.output_bundle_uuid)
                    assert(b is not None)
                    _commit_and_push(b)
                else:
                    _logger.info("Pipeline ran but did no useful work (output bundle exists).")
            else:
                _logger.info("Pipeline ran but user specified not to push any bundles to remote context.")
        else:
            _logger.info("Pipeline ran using incremental pushing.")

    except RuntimeError as re:
        _logger.error('Failed to run pipeline: RuntimeError {}'.format(re))
        sys.exit(os.EX_IOERR)

    except disdat.common.ApplyError as ae:
        _logger.error('Failed to run pipeline: ApplyException {}'.format(ae))
        sys.exit(os.EX_IOERR)

    if args.dump_output:
        print(disdat.api.cat(args.branch, args.output_bundle))

    sys.exit(os.EX_OK)


def main(input_args):

    # To simplify configuring and building pipeline images, we can keep
    # various default parameter values in the Docker image makefile,
    # and pass them on as Docker ENV variables.   At the moment, we set
    # the default params below to handle most cases.  This is an example
    # of how you might do this in the future if needed.
    # some_default = os.environ[ENVVAR] if ENVVAR in os.environ else None

    parser = argparse.ArgumentParser(
        description=_HELP,
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
        default=None,
        help='The s3 bucket from/to which to pull/push data',
    )
    disdat_parser.add_argument(
        '--no-pull',
        action='store_true',
        help='Do not pull (synchronize) remote repository with local repo - may cause entire pipeline to re-run.',
    )
    disdat_parser.add_argument(
        '--no-push',
        action='store_true',
        help='Do not push output bundles (including intermediates) to the remote repository (default is to push)',
    )
    disdat_parser.add_argument(
        '--no-push-intermediates',
        action='store_true',
        help='Do not push the intermediate bundles to the remote repository (default is to push)',
    )

    pipeline_parser = parser.add_argument_group('pipe arguments')

    pipeline_parser.add_argument(
        '--branch',
        type=str,
        required=True,
        help='The fully-qualified Disdat branch to use when running',
    )

    pipeline_parser.add_argument(
        '--workers',
        type=int,
        default=2,
        help="The number of Luigi workers to spawn.  Default is 2."
    )

    pipeline_parser.add_argument(
        '-it', '--input-tag',
        nargs=1, type=str, action='append',
        help="Input bundle tags: '-it authoritative:True -it version:0.7.1'")

    pipeline_parser.add_argument(
        '-ot', '--output-tag',
        nargs=1, type=str, action='append',
        help="Output bundle tags: '-ot authoritative:True -ot version:0.7.1'")

    pipeline_parser.add_argument(
        '--output-bundle-uuid',
        default=None,
        type=str,
        help='UUID for the output bundle (default is for apply to generate a UUID)',
    )

    pipeline_parser.add_argument(
        '-o',
        '--output-bundle',
        type=str,
        default='-',
        help="Name output bundle: '-o my.output.bundle'.  Default name is '<TaskName>_<param_hash>'"
    )

    pipeline_parser.add_argument(
        '--force',
        action='store_true',
        help='Force recomputation of the last task.',
    )

    pipeline_parser.add_argument(
        '--force-all',
        action='store_true',
        help='Force recomputation of all upstream tasks.',
    )

    pipeline_parser.add_argument(
        'pipe_cls',
        default=None,
        type=disdat.common.load_class,
        help=add_argument_help_string("Name of the pipeline class to run, e.g., 'package.module.ClassName'"),
    )

    pipeline_parser.add_argument(
        "pipeline_args",
        nargs=argparse.REMAINDER,
        type=str,
        help="Optional set of parameters for this pipe '--parameter value'"
    )

    args = parser.parse_args(input_args)

    log_level = logging.INFO

    log.enable(level=log_level)  # TODO: Add configurable verbosity

    run_disdat_container(args)


if __name__ == '__main__':
    main(sys.argv[1:])


