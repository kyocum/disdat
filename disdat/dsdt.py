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

from disdat import apply, dockerize, run, fs, add, lineage
from disdat.common import DisdatConfig
from disdat import log

_pipes_fs = None

DISDAT_PATH = os.environ.get("PATH", None)
DISDAT_PYTHONPATH = os.environ.get("PYTHONPATH", None)


def main():
    """
    Main is the package entry point.
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

    # Add additional subparsers
    dockerize.add_arg_parser(subparsers)
    run.add_arg_parser(subparsers)
    apply.add_arg_parser(subparsers)
    fs.add_arg_parser(subparsers)
    add.add_arg_parser(subparsers)
    lineage.add_arg_parser(subparsers)

    args = parser.parse_args(args)

    log_level = logging.INFO
    if args.verbose:
        log_level = logging.DEBUG

    log.enable(level=log_level)  # TODO: Add configurable verbosity

    if args.aws_profile is not None:
        os.environ['AWS_PROFILE'] = args.aws_profile

    if hasattr(args,'func'):
        args.func(args)
    else:
        print("dsdt requires arguments, see `dsdt -h` for usage")


if __name__ == "__main__":
    main()
