#! /usr/bin/env python
#
# Copyright 2015, 2016  Human Longevity, Inc.
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
dsdt

Distributed data (dsdt) command line utility for working with data science pipelines.

"""

import argparse
import logging
import sys
import os

from disdat import apply  # @ReservedAssignment
from disdat import dockerize
from disdat import run
from disdat.fs import init_fs_cl
from disdat.common import DisdatConfig

_logger = logging.getLogger(__name__)

_pipes_fs = None

DISDAT_PATH = os.environ.get("PATH", None)
DISDAT_PYTHONPATH = os.environ.get("PYTHONPATH", None)


def _apply(args):
    """

    :param args:
    :return:
    """
    apply.main(DisdatConfig.instance(), args)


def _dockerize(args):
    """

    :param args:
    :return:
    """
    dockerize.main(DisdatConfig.instance(), args)


def _run(args):
    """

    :param args:
    :return:
    """
    run.main(DisdatConfig.instance, args)


def main():
    """
    Main as a function for testing convenience and as a package entry point.

    :return: (shape of input df, shape of pushed df)
    """

    if getattr( sys, 'frozen', False ) :
        here = sys._MEIPASS
    else:
        here = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..')

    with open(os.path.join(here, 'VERSION')) as version_file:
        __version__ = version_file.read().strip()

    args = sys.argv[1:]

    # General options
    parser = argparse.ArgumentParser(prog='dsdt', description='DisDat (dsdt) -- distributed data science management')
    parser.add_argument(
        '--profile',
        type=str,
        default=None,
        help="An AWS credential profile to use when performing AWS operations (default is to use the \'default\' profile)",
        dest='aws_profile'
    )
    parser.add_argument("--verbose", action='store_true', help='Be verbose: Show extra debugging information')
    parser.add_argument("--version", action='version', version='Running Disdat version {}'.format(__version__))
    subparsers = parser.add_subparsers()

    ls_p = subparsers.add_parser('init')
    ls_p.set_defaults(func=lambda args: DisdatConfig.init())

    # autodock
    dockerize_p = subparsers.add_parser('dockerize', description="Dockerizer a particular transform.")
    dockerize_p.add_argument(
        '--config-dir',
        type=str,
        default=None,
        help="A directory containing configuration files for the operating system within the Docker image",
    )
    dockerize_p.add_argument('--os-type', type=str, default=None, help='The base operating system type for the Docker image')
    dockerize_p.add_argument('--os-version', type=str, default=None, help='The base operating system version for the Docker image')
    dockerize_p.add_argument(
        '--push',
        action='store_true',
        help="Push the image to a remote Docker registry (default is to not push; must set 'docker_registry' in Disdat config)",
    )
    dockerize_p.add_argument(
        '--no-build',
        action='store_false',
        help='Do not build an image (only copy files into the Docker build context)',
        dest='build',
    )
    dockerize_p.add_argument(
        "pipe_root",
        type=str,
        help="Root of the Python source tree containing the user-defined transform; must have a setuptools-style setup.py file"
    )
    dockerize_p.add_argument("pipe_cls", type=str, help="User-defined transform, e.g., module.PipeClass")
    dockerize_p.set_defaults(func=lambda args: _dockerize(args))

    # run
    run_p = subparsers.add_parser('run', description="Run containerized version of transform.")
    run_p.add_argument('--backend', default=run.Backend.default(), type=str, choices=run.Backend.options(), help='An optional batch execution back-end to use')
    run_p.add_argument("--force", action='store_true', help="If there are dependencies, force re-computation.")
    run_p.add_argument("--no-push-input", action='store_false', help="Do not push the current committed input bundle before execution (default is to push)", dest='push_input_bundle')
    run_p.add_argument('-it', '--input-tag', nargs=1, type=str, action='append',
                       help="Input bundle tags: '-it authoritative:True -it version:0.7.1'")
    run_p.add_argument('-ot', '--output-tag', nargs=1, type=str, action='append',
                       help="Output bundle tags: '-ot authoritative:True -ot version:0.7.1'")
    run_p.add_argument("input_bundle", type=str, help="Name of source data bundle.  '-' means no input bundle.")
    run_p.add_argument("output_bundle", type=str, help="Name of destination bundle.  '-' means default output bundle.")
    run_p.add_argument("pipe_cls", type=str, help="User-defined transform, e.g., module.PipeClass")
    run_p.add_argument("pipeline_args", type=str,  nargs=argparse.REMAINDER, help="Optional set of parameters for this pipe '--parameter value'")
    run_p.set_defaults(func=lambda args: _run(args))

    # apply
    apply_p = subparsers.add_parser('apply', description="Apply a transform to an input bundle to produce an output bundle.")
    apply_p.add_argument('-it', '--input-tag', nargs=1, type=str, action='append',
                         help="Input bundle tags: '-it authoritative:True -it version:0.7.1'")
    apply_p.add_argument('-ot', '--output-tag', nargs=1, type=str, action='append',
                         help="Output bundle tags: '-ot authoritative:True -ot version:0.7.1'")
    apply_p.add_argument("input_bundle", type=str, help="Name of source data bundle.  '-' means no input bundle.")
    apply_p.add_argument("output_bundle", type=str, help="Name of destination bundle.  '-' means default output bundle.")
    apply_p.add_argument("pipe_cls", type=str, help="User-defined transform, e.g., module.PipeClass")
    apply_p.add_argument("--local", action='store_true', help="Run the class locally (even if dockered)")
    apply_p.add_argument("--force", action='store_true', help="If there are dependencies, force re-computation.")
    apply_p.add_argument("params", type=str,  nargs=argparse.REMAINDER,
                         help="Optional set of parameters for this pipe '--parameter value'")
    apply_p.set_defaults(func=lambda args: _apply(args))

    # File system operations
    init_fs_cl(subparsers)

    args = parser.parse_args(args)

    log_level = logging.WARN
    if args.verbose:
        log_level = logging.DEBUG
    logging.basicConfig(level=log_level)

    if args.aws_profile is not None:
        os.environ['AWS_PROFILE'] = args.aws_profile

    args.func(args)


if __name__ == "__main__":
    main()
