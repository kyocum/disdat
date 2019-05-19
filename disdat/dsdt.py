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
Disdat

Distributed data (dsdt) command line utility for working with data science pipelines.

"""

import argparse
import logging
import sys
import os

from disdat import apply
from disdat import dockerize
from disdat import run
from disdat.fs import init_fs_cl
from disdat.add import init_add_cl
from disdat.common import DisdatConfig
from disdat import log


_pipes_fs = None

DISDAT_PATH = os.environ.get("PATH", None)
DISDAT_PYTHONPATH = os.environ.get("PYTHONPATH", None)


def _apply(args):
    """

    :param args:
    :return:
    """
    apply.main(args)


def main():
    """
    Main as a function for testing convenience and as a package entry point.

    :return: (shape of input df, shape of pushed df)
    """

    if getattr(sys, 'frozen', False):
        here = os.path.join(sys._MEIPASS, 'disdat')
    else:
        here = os.path.abspath(os.path.dirname(__file__))

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

    # dockerize
    subparsers = dockerize.add_arg_parser(subparsers)

    # run
    subparsers = run.add_arg_parser(subparsers)

    # apply
    apply_p = subparsers.add_parser('apply', description="Apply a transform to an input bundle to produce an output bundle.")
    apply_p.add_argument('-cs', '--central-scheduler', action='store_true', default=False, help="Use a central Luigi scheduler (defaults to local scheduler)")
    apply_p.add_argument('-w', '--workers', type=int, default=1, help="Number of Luigi workers on this node")
    apply_p.add_argument('-it', '--input-tag', nargs=1, type=str, action='append',
                         help="Input bundle tags: '-it authoritative:True -it version:0.7.1'")
    apply_p.add_argument('-ot', '--output-tag', nargs=1, type=str, action='append',
                         help="Output bundle tags: '-ot authoritative:True -ot version:0.7.1'")
    apply_p.add_argument('-o', '--output-bundle', type=str, default='-',
                         help="Name output bundle: '-o my.output.bundle'.  Default name is '<TaskName>_<param_hash>'")
    apply_p.add_argument('--local', action='store_true', help="Run the class locally (even if dockered)")
    apply_p.add_argument('-f', '--force', action='store_true', help="If there are dependencies, force re-computation.")
    apply_p.add_argument('--incremental-push', action='store_true', help="Commit and push each task's bundle as it is produced to the remote.")
    apply_p.add_argument('--incremental-pull', action='store_true', help="Localize bundles as they are needed by downstream tasks from the remote.")
    apply_p.add_argument('pipe_cls', type=str, help="User-defined transform, e.g., module.PipeClass")
    apply_p.add_argument('params', type=str,  nargs=argparse.REMAINDER,
                         help="Optional set of parameters for this pipe '--parameter value'")
    apply_p.set_defaults(func=lambda args: _apply(args))

    # File system operations
    init_fs_cl(subparsers)

    # add
    init_add_cl(subparsers)

    args = parser.parse_args(args)

    log_level = logging.INFO
    if args.verbose:
        log_level = logging.DEBUG

    log.enable(level=log_level)  # TODO: Add configurable verbosity

    if args.aws_profile is not None:
        os.environ['AWS_PROFILE'] = args.aws_profile

    args.func(args)


if __name__ == "__main__":
    main()
