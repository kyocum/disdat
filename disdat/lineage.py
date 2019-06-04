#
# Copyright Human Longevity, Inc.
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

import disdat.api as api
import disdat.fs
from disdat import logger as _logger


def _lineage(args):
    """Invoke the api.lineage() call from the CLI to find the lineage.

    Args:
        args: command line args.

    Returns:
        None

    """

    fs = disdat.fs.DisdatFS()

    if not fs.in_context():
        _logger.warning('Not in a data context')
        return

    ctxt = fs._curr_context.get_local_name()

    # (depth, uuid, lineage)
    lin_tuples = api.lineage(ctxt, args.uuid, args.depth)

    for (d,uuid,l) in lin_tuples:
        print ("------ DEPTH {} ----- ".format(d))
        if l is None:
            print("No lineage found for UUID {}".format(uuid))
        else:
            print(l)

    return


def init_lineage_cl(subparsers):
    """Initialize a command line set of subparsers with the lineage command.

    Args:
        subparsers: A collection of subparsers as defined by `argsparse`.
    """
    # add

    lineage_p = subparsers.add_parser('lineage', description='View and manage lineage of a bundle.')
    lineage_p.add_argument('-f', '--follow', action='store_true', help="Follow lineage back through D levels.")
    lineage_p.add_argument('-d', '--depth', type=int, default=3, help="Number of levels to follow (default is 3).")
    lineage_p.add_argument('uuid', type=str, default=None, help='Find lineage by bundle UUID')
    lineage_p.set_defaults(func=lambda args: _lineage(args))

