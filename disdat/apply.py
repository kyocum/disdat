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

"""
apply

API for executing a pipe

pipes apply output_bundle pipes_cls

author: Kenneth Yocum
"""
from __future__ import print_function

import sys
import os
import argparse
import multiprocessing

import luigi.task_register
from luigi import build
from luigi.execution_summary import LuigiStatusCode, _partition_tasks

import disdat.common as common  # config, especially logging, before luigi ever loads
import disdat.fs as fs
from disdat import logger as _logger


def apply(output_bundle, pipe_params, pipe_cls, input_tags, output_tags, force, force_all,
          output_bundle_uuid=None, central_scheduler=False, workers=1, data_context=None,
          incremental_push=False, incremental_pull=False):
    """
    Given an input bundle, run the pipesline on the bundle.
    Note, we first make a copy of all tasks that are parameterized identically to the tasks we will run.
    This is so we can figure out what we will need to re-run.
    This is why we make a single uuid for the output bundle of apply (for the driver).

    Args:
        output_bundle: The new bundle to be created
        pipe_params (dict):   mapping of parameter name to Luigi Task parameter value
        pipe_cls (type[disdat.pipe.PipeTask]):      reference to the task class
        force:         force recomputation of the last user task.
        force_all:     force recomputation of dependencies
        input_tags (dict):  Tags used to find the input bundle
        output_tags (dict):  Tags that need to be placed on the output bundle
        force_all (bool): whether to re-run this pipe
        output_bundle_uuid (str):  Optionally specify exactly the UUID of the output bundle IFF we actually need to produce it
        central_scheduler: Use a centralized Luigi scheduler (default False, i.e., --local-scheduler is used)
        workers: The number of luigi workers to use for this workflow (default 1)
        data_context: Actual context object or None and read current context.
        incremental_push (bool): Whether this job should push tasks as they complete to the remote (if configured)
        incremental_pull (bool): Whether this job should localize bundles as needed from the remote (if configured)

    Returns:
        bool: True if there were no failed tasks and no failed schedulings (missing external dependencies)
    """

    _logger.debug("pipe_cls {}".format(pipe_cls))
    _logger.debug("pipe params: {}".format(pipe_params))
    _logger.debug("force: {}".format(force))
    _logger.debug("force_all: {}".format(force_all))
    _logger.debug("input tags: {}".format(input_tags))
    _logger.debug("output tags: {}".format(output_tags))
    _logger.debug("sys.path {}".format(sys.path))
    _logger.debug("central_scheduler {}".format(central_scheduler))
    _logger.debug("workers {}".format(workers))
    _logger.debug("incremental_push {}".format(incremental_push))
    _logger.debug("incremental_pull {}".format(incremental_pull))

    if incremental_push:
        _logger.warn("incremental_push {}".format(incremental_push))

    if incremental_pull:
        _logger.warn("incremental_pull {}".format(incremental_pull))

    pfs = fs.DisdatFS()

    if data_context is None:
        if not pfs.in_context():
            _logger.warning('Not in a data context')
            return None
        data_context = pfs.curr_context

    # Increment the reference count for this process
    apply.reference_count += 1

    # If we are using Fork, we cannot have internal apply's with workers > 1
    # otherwise luigi loops forever with "There are no more tasks to run" and "<some task> is currently run by worker"
    # This happens with Vanilla luigi in fork mode.   In <=P37, MP fork for OS X is the default
    # in >=P38, Spawn is the default.
    if apply.reference_count > 1:
        if multiprocessing.get_start_method() == 'fork':
            workers = 1

    def cleanup_cached_state():
        """
        After running, decrement our reference count (which tells how many simultaneous apply methods are
        running nested in this process.  Once the last one completes, blow away the luigi instance cache and git hash.
        Needed if we're run twice (from scratch) in the same process.  Otherwise, on the next run, we could find
        the same class instances, with the old cached_output_bundle fields set.
        """
        apply.reference_count -= 1
        if not apply.reference_count:
            fs.DisdatFS().clear_pipe_version()
            luigi.task_register.Register.clear_instance_cache()

    # Only pass data_context name, not reference to the pipe class
    # data contexts may have open sql connections and other state
    # that is not ForingPickler safe.
    data_context_name = data_context.get_local_name()

    # Re-execute logic -- make copy of task DAG
    # Creates a cache of {pipe:path_cache_entry} in the pipesFS object.
    # This "task_path_cache" is used throughout execution to find output bundles.
    dag = create_users_task(pipe_cls, pipe_params, output_bundle,
                            output_bundle_uuid, force_all, output_tags,
                            data_context_name, incremental_push, incremental_pull)

    # Get version information for pipeline
    pipeline_path = os.path.dirname(sys.modules[dag.__module__].__file__)
    fs.DisdatFS().get_pipe_version(pipeline_path)

    # If the user just wants to re-run this task, use mark_force
    if force:
        dag.mark_force()

    # Will be LuigiRunResult
    status = build([dag], local_scheduler=not central_scheduler, workers=workers, detailed_summary=True)
    success = False
    if status.status == LuigiStatusCode.SUCCESS:
        success = True
    task_sets = _partition_tasks(status.worker)
    did_work = len(task_sets['completed']) > 0

    cleanup_cached_state()

    return {'success': success, 'did_work': did_work}


# Add a reference count to apply, so we can determine when to clean up the path_cache
apply.reference_count = 0

def create_users_task(pipe_cls,
                      pipe_params,
                      root_bundle_name,
                      forced_output_bundle_uuid,
                      force,
                      output_tags,
                      data_context_name,
                      incremental_push,
                      incremental_pull):
    """
    Create the users task

    Every apply or run is logically a single execution and produces a set of outputs.  Each
    task produces a single bundle represented as a hyperframe internally.

    Args:
        pipe_cls (disdat.Pipe): The user's Pipe class
        pipe_params: parameters for this pipeline
        root_bundle_name (str): user set output bundle name for last task
        forced_output_bundle_uuid (str): user set output bundle uuid for last task, else None
        force (bool): force re-run of entire pipeline
        output_tags (dict):  str,str tag dict
        data_context_name (str): name in which pipe will run
        incremental_push (bool): push bundle when pipe finishes
        incremental_pull (bool): pull non-localized bundles before execution

    Returns:
        `disdat.Pipe`
    """

    # Force root task to take an explicit bundle name?
    if root_bundle_name == '-':
        root_bundle_name = None

    task_params = {'is_root_task':  True,
                   'root_output_bundle_name': root_bundle_name,
                   'forced_output_bundle_uuid': forced_output_bundle_uuid,
                   'force': force,
                   'output_tags': output_tags,
                   'data_context_name': data_context_name,
                   'incremental_push': incremental_push,
                   'incremental_pull': incremental_pull
                   }

    # Get user pipeline parameters for this Pipe / Luigi Task
    if pipe_params:
        task_params.update(pipe_params)

    # Instantiate and return the class directly with the parameters
    # Instance caching is taken care of automatically by luigi
    return pipe_cls(**task_params)


def different_code_versions(code_version, lineage_obj):
    """
    Given the current version, see if it is different than found_version
    Note, if either version is dirty, we are forced to say they are different

    Typically we get the code_version from the pipe and the lineage object from the
    bundle.   We then see if the current code == the information in lineage object.

    Args:
        current_version (CodeVersion) :
        lineage_obj (LineageObject):

    Returns:

    """

    conf = common.DisdatConfig.instance()

    if conf.ignore_code_version:
        return False

    # If there were uncommitted changes, then we have to re-run, mark as different
    if code_version.dirty:
        return True

    if code_version.semver != lineage_obj.pb.code_semver:
        return True

    if code_version.hash != lineage_obj.pb.code_hash:
        return True

    ## Currently ignoring tstamp, branch, url
    ## CodeVersion = collections.namedtuple('CodeVersion', 'semver hash tstamp branch url dirty')

    return False


def new_output_bundle(pipe, data_context, force_uuid=None):
    """
    This proposes a new output bundle
    1.) Create a new UUID
    2.) Create the directory in the context
    3.) Add this to the path cache

    Note: We don't add to context's db yet.  The job or pipe hasn't run yet.  So it
    hasn't made all of its outputs.  If it fails, by definition it won't right out the
    hframe to the context's directory.   On rebuild / restart we will delete the directory.
    However, the path_cache will hold on to this directory in memory.

    Args:
        pipe (`disdat.pipe.PipeTask`):  The task generating this output
        data_context (`disdat.data_context.DataContext`): Place output in this context
        force_uuid (str): Override uuid chosen by Disdat Bundle API

    Returns:
        None
    """
    import disdat.api as api  # 3.7 allows us to put this import at the top, but not 3.6.8
    pce = PathCache.get_path_cache(pipe)

    if pce is None:
        _logger.debug("new_output_bundle: Adding a new (unseen) task to the path cache.")
    else:
        _logger.debug("new_output_bundle: Found a task in our dag already in the path cache: reusing!")
        return

    b = api.Bundle(data_context).open(force_uuid=force_uuid)

    PathCache.put_path_cache(pipe, b, b.uuid, b.local_dir, True)


def cli_apply(args):
    """
    Parse and prepare strings from argparse arguments into suitable Python objects
    to call the api's version of apply.  Note, args.pipe_cls is already a cls object.
    Most of the work here is to deser each input parameter value according to its
    Luigi definition.

    Parameters:
        disdat_config:
        args:

    Returns:
        None
    """

    if not fs.DisdatFS().in_context():
        print("Apply unavailable -- Disdat not in a valid context.")
        return

    # Create a dictionary of str->str arguments to str->python objects deser'd by Luigi Parameters
    deser_user_params = common.parse_params(args.pipe_cls, args.params)

    input_tags = common.parse_args_tags(args.input_tag)

    output_tags = common.parse_args_tags(args.output_tag)

    # NOTE: sysexit=False is required for us to pass a data_context object through luigi tasks.
    # Else we build up arguments as strings to run_with_retcodes().  And it crashes because the data_context is
    # not a string.
    result = apply(args.output_bundle, deser_user_params, args.pipe_cls, input_tags, output_tags,
                   args.force, args.force_all,
                   central_scheduler=args.central_scheduler,
                   workers=args.workers,
                   incremental_push=args.incremental_push,
                   incremental_pull=args.incremental_pull)

    # If we didn't successfully run any task, sys.exit with non-zero code
    common.apply_handle_result(result)


def add_arg_parser(subparsers):
    """Initialize a command line set of subparsers with file system commands.

    Args:
        subparsers: A collection of subparsers as defined by `argsparse`.
    """

    apply_p = subparsers.add_parser('apply',
                                    description="Apply a transform to an input bundle to produce an output bundle.")
    apply_p.add_argument('-cs', '--central-scheduler', action='store_true', default=False,
                         help="Use a central Luigi scheduler (defaults to local scheduler)")
    apply_p.add_argument('-w', '--workers', type=int, default=1, help="Number of Luigi workers on this node")
    apply_p.add_argument('-it', '--input-tag', nargs=1, type=str, action='append',
                         help="Input bundle tags: '-it authoritative:True -it version:0.7.1'")
    apply_p.add_argument('-ot', '--output-tag', nargs=1, type=str, action='append',
                         help="Output bundle tags: '-ot authoritative:True -ot version:0.7.1'")
    apply_p.add_argument('-o', '--output-bundle', type=str, default='-',
                         help="Name output bundle: '-o my.output.bundle'.  Default name is '<TaskName>_<param_hash>'")
    apply_p.add_argument('-f', '--force', action='store_true', help="Force re-computation of only this task.")
    apply_p.add_argument('--force-all', action='store_true', help="Force re-computation of ALL upstream tasks.")
    apply_p.add_argument('--incremental-push', action='store_true',
                         help="Commit and push each task's bundle as it is produced to the remote.")
    apply_p.add_argument('--incremental-pull', action='store_true',
                         help="Localize bundles as they are needed by downstream tasks from the remote.")
    apply_p.add_argument('pipe_cls', type=common.load_class, help="User-defined transform, e.g., 'module.PipeClass'")
    apply_p.add_argument('params', type=str, nargs=argparse.REMAINDER,
                         help="Optional set of parameters for this pipe '--parameter value'")
    apply_p.set_defaults(func=lambda args: cli_apply(args))
