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

from datetime import datetime


def print_lineage_protobuf(lineage_pb, offset=0):
    """ Print out the protobuf fields.  This mirrors printing the protobuf python object
    directly, except we re-format some fields, like date and give an URL for the git commit.

    Args:
        lineage_pb(`hyperframe_pb2.Lineage`): Protobuf lineage object.
        offset (int):  The tab level

    Returns:
        None
    """

    indent = ''.join(['\t' for i in range(offset)])

    print ("{}------ Lineage @ depth {} ----- ".format(indent, offset))
    print("{}processing name: {}".format(indent, lineage_pb.hframe_proc_name))
    print("{}uuid: {}".format(indent, lineage_pb.hframe_uuid))
    print("{}creation date: {}".format(indent, datetime.fromtimestamp(lineage_pb.creation_date)))
    print("{}code repo: {}".format(indent, lineage_pb.code_repo))
    print("{}code hash: {}".format(indent, lineage_pb.code_hash))
    print("{}code method: {}".format(indent, lineage_pb.code_method if hasattr(lineage_pb, 'code_method') else None))

    try:
        repo = lineage_pb.code_repo.split('@')
        if len(repo) == 2:
            # Assume: git@github.intuit.com:user/project.git
            _, site = repo
            site, project_url = site.split(':')
            project_url = project_url.replace('.git', '')
            url = "https://{}/{}/commit/{}".format(site, project_url, lineage_pb.code_hash)
        else:
            # Assume:  https://github.com/user/project.git
            assert len(repo) == 1
            project_url = repo[0].replace('.git', '')
            url = "{}/commit/{}".format(project_url, lineage_pb.code_hash)
    except Exception as e:
        print (e)
        url = "unknown"

    print("{}git commit URL: {}".format(indent,url))
    print("{}code branch: {}".format(indent,lineage_pb.code_branch))

    duration = lineage_pb.stop_time - lineage_pb.start_time

    print("{}Start {} Stop {} Duration {}".format(indent, lineage_pb.start_time, lineage_pb.stop_time, duration))


def _lineage(**kwargs):
    """Invoke the api.lineage() call from the CLI to find the lineage.

    Args:
        kwargs: command line args or internal dict call, must contain uuid:str and depth:int.

    Returns:
        None

    """

    fs = disdat.fs.DisdatFS()

    if not fs.in_context():
        _logger.warning('Not in a data context')
        return

    ctxt = fs._curr_context.get_local_name()

    # (depth, uuid, lineage)
    lin_tuples = api.lineage(ctxt, kwargs['uuid'], kwargs['depth'])

    for (d,uuid,l) in lin_tuples:
        if l is None:
            print("No lineage found for UUID {}".format(uuid))
        else:
            print_lineage_protobuf(l, d)
            print()

    return


def add_arg_parser(subparsers):
    """Initialize a command line set of subparsers with the lineage command.

    Args:
        subparsers: A collection of subparsers as defined by `argsparse`.
    """
    # add

    lineage_p = subparsers.add_parser('lineage', description='View and manage lineage of a bundle.')
    lineage_p.add_argument('-f', '--follow', action='store_true', help="Follow lineage back through D levels.")
    lineage_p.add_argument('-d', '--depth', type=int, default=3, help="Number of levels to follow (default is 3).")
    lineage_p.add_argument('uuid', type=str, default=None, help='Find lineage by bundle UUID')
    lineage_p.set_defaults(func=lambda args: _lineage(**vars(args)))


if __name__ == '__main__':
    args = {'uuid': '0cf8f35f-1bf8-4404-a9de-db4bca93c09d', 'depth':0}
    _lineage(**args)