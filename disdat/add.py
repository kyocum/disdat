#
# Copyright 2015, 2016, 2017  Human Longevity, Inc.
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

from __future__ import print_function

import disdat.common as common
import disdat.api as api
import disdat.fs
from disdat import logger as _logger


def _add(args):
    """Invoke the api.add() call from the CLI to create a bundle.

    Args:
        args: command line args.

    Returns:
        None

    """

    fs = disdat.fs.DisdatFS()

    if not fs.in_context():
        _logger.warning('Not in a data context')
        return

    _ = api.add(fs._curr_context.get_local_name(),
                args.bundle,
                args.path_name,
                tags=common.parse_args_tags(args.tag))

    return


def add_arg_parser(subparsers):
    """Initialize a command line set of subparsers with the add command.

    Args:
        subparsers: A collection of subparsers as defined by `argsparse`.
    """
    # add
    add_p = subparsers.add_parser('add', description='Create a bundle from a .csv, .tsv, or a directory of files.')
    add_p.add_argument('-t', '--tag', nargs=1, type=str, action='append',
                       help="Set one or more tags: 'dsdt add -t authoritative:True -t version:0.7.1'")
    add_p.add_argument('bundle', type=str, help='The destination bundle in the current context')
    add_p.add_argument('path_name', type=str, help='File or directory of files to add to the bundle', action='store')
    add_p.set_defaults(func=lambda args: _add(args))