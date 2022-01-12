#! /usr/bin/env python
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

Command line utility
This comes with disdat (disdat-core).  It looks for other installed packages, such as disdat-luigi,
that may define additional commands.

"""

import argparse
import logging
import sys
import os
import importlib.util

from disdat import fs, add, lineage
from disdat.common import DisdatConfig
from disdat import log, __version__

_pipes_fs = None

DISDAT_PATH = os.environ.get("PATH", None)
DISDAT_PYTHONPATH = os.environ.get("PYTHONPATH", None)
DISDAT_CLI_EXTRAS = ["disdatluigi"]
EXTENSION_MODULE = "cli_extension"
EXTENSION_METHOD = "add_arg_parser"

def resolve_cli_extras(subparsers):
    """
    For each additional package that might extend the CLI check to see if the module
    is loaded, create a reference to it, and call the "add_arg_parser" function.
    We expect two things from the high-level package:
    1.) a top-level module "cli_extension"
    2.) method called "add_arg_parser"

    Returns:
        None
    """
    for module in DISDAT_CLI_EXTRAS:
        spec = importlib.util.find_spec(module)
        if spec is None:
            pass
            #print(f"Dynamic CLI extension: {module} is not installed")
        else:
            #print(f"Dynamic CLI extension: {module} found, attempting to extend CLI . . . ")
            module_handle = importlib.import_module(module+f".{EXTENSION_MODULE}")
            try:
                add_cli_arg_parser = getattr(module_handle, EXTENSION_METHOD)
                add_cli_arg_parser(subparsers)
            except AttributeError as ae:
                print(f"Disdat CLI unable to add commands from loaded extension [{module}], error {ae}")

def main():
    """
    Main is the package entry point.
    """

    if getattr(sys, 'frozen', False):
        here = os.path.join(sys._MEIPASS, 'disdat')
    else:
        here = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))

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

    # Add disdat core subparsers
    fs.add_arg_parser(subparsers)
    add.add_arg_parser(subparsers)
    lineage.add_arg_parser(subparsers)

    # Add additional parsers if we are imported
    resolve_cli_extras(subparsers)

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
