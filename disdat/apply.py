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
import json
import collections

from luigi import build, worker

import disdat.common as common  # config, especially logging, before luigi ever loads
import disdat.pipe_base as pipe_base
import disdat.fs as fs
import disdat.driver as driver
from disdat import logger as _logger


def apply(output_bundle, pipe_params, pipe_cls, input_tags, output_tags, force,
          output_bundle_uuid=None, central_scheduler=False, workers=1, data_context=None,
          incremental_push=False, incremental_pull=False):
    """
    Given an input bundle, run the pipesline on the bundle.
    Note, we first make a copy of all tasks that are parameterized identically to the tasks we will run.
    This is so we can figure out what we will need to re-run.
    This is why we make a single uuid for the output bundle of apply (for the driver).

    Args:
        output_bundle: The new bundle to be created
        pipe_params (str):   Luigi Task parameters string
        pipe_cls:      String <module.ClassName>
        force:         force recomputation of dependencies
        input_tags (dict):  Tags used to find the input bundle
        output_tags (dict):  Tags that need to be placed on the output bundle
        force (bool): whether to re-run this pipe
        output_bundle_uuid (str):  Optionally specify exactly the UUID of the output bundle IFF we actually need to produce it
        central_scheduler: Use a centralized Luigi scheduler (default False, i.e., --local-scheduler is used)
        workers: The number of luigi workers to use for this workflow (default 1)
        data_context: Actual context object or None and read current context.
        incremental_push (bool): Whether this job should push tasks as they complete to the remote (if configured)
        incremental_pull (bool): Whether this job should localize bundles as needed from the remote (if configured)

    Returns:
        bool: True if tasks needed to be run, False if no tasks (beyond wrapper task) executed.
    """

    _logger.debug("driver {}".format(driver.DriverTask))
    _logger.debug("pipe_cls {}".format(pipe_cls))
    _logger.debug("pipe params: {}".format(pipe_params))
    _logger.debug("force: {}".format(force))
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
        data_context = pfs.get_curr_context()

    # Re-execute logic -- make copy of task DAG
    # Creates a cache of {pipe:path_cache_entry} in the pipesFS object.
    # This "task_path_cache" is used throughout execution to find output bundles.
    reexecute_dag = driver.DriverTask(output_bundle, pipe_params,
                                      pipe_cls, input_tags, output_tags, force,
                                      data_context, incremental_push, incremental_pull)

    did_work = resolve_workflow_bundles(reexecute_dag, data_context)

    # At this point the path cache should be full of existing or new UUIDs.
    # we are going to replace the final pipe's UUID if the user has passed one in.
    # this happens when we run the docker container.
    # TODO: don't replace if it already exists.
    if output_bundle_uuid is not None:
        users_root_task = reexecute_dag.deps()[0]
        pce = pfs.get_path_cache(users_root_task)
        if pce.rerun: # if we have to re-run then replace it with our UUID
            # TODO: this is the same code as new_output_hframe, FIX!!!
            dir, uuid, _ = data_context.make_managed_path(output_bundle_uuid)
            fs.DisdatFS.put_path_cache(users_root_task,
                                       uuid,
                                       dir,
                                       pce.rerun,
                                       pce.is_left_edge_task,
                                       overwrite=True)

    success = build([reexecute_dag], local_scheduler=not central_scheduler, workers=workers)

    # After running a pipeline, blow away our path cache.  Needed if we're run twice in the same process.
    fs.DisdatFS().clear_path_cache()

    return {'success': success, 'did_work': did_work}

        
def topo_sort_tasks(root_task):
    """
    Return a stack with a valid topological sort of the task graph.
    Luigi edges point downstream to upstream.   We breadth first search the 
    task graph and append to a list to create a stack.   Reverse pop the stack for your
    topological sort. 

    Naturally Luigi has to do similar things.  See luigi.CentralPlanner._traverse_graph()
    ASSUME:  That each task as a task.deps() function
    This function provides a flatten on the tasks requires.

    NOTE: We use luigi.worker._is_external() .  This is dangerous.  However we also use luigi.task.externalize()
    which should be self-consistent with the internal call.   Time will tell.  If it breaks, then we'll need
    to create our own marker.

    """

    stack = []
    to_visit_fifo = collections.deque([root_task])

    while len(to_visit_fifo) > 0:
        next_node = to_visit_fifo.popleft()
        stack.append(next_node)
        to_visit_fifo.extend(next_node.deps() if not worker._is_external(next_node) else [])
        
    return stack


def is_left_edge_task(task):
    """
    Determine if task is on the left edge of the dag (first task)
    Args:
        task:
    Returns: True or False
    """
    deps = task.deps()

    if len(deps) > 0:
        return False
    else:
        return True
    

def resolve_workflow_bundles(root_task, data_context):
    """
    Given a task graph rooted at root task, we need to determine whether 
    each task needs a new bundle or if it can reuse an existing bundle. 

    This is equivalent to deciding what needs to run and what can re-use prior results. 
    This is Luigi-specific.  We need to know all of our output paths ahead of time for all tasks. 

    1.) topologically sort tasks
    2.) for each task in that order, determine bundle (or make new output bundle)
    3.) store the bundle in the PipeFS static variable.  This allows the output method to find the cached result 
        of this computation.

    Args:
        root_task:
        data_context:

    Returns:
        bool: is_work -- True if there is anything to re-run, False if no work to be done (only driver wrapper task)

    """

    pfs = fs.DisdatFS()

    stack = topo_sort_tasks(root_task)

    num_regen = 0
    num_reuse = 0

    # For each task in the sort order, figure out if we need a new bundle (re-run it)
    while len(stack) > 0:
        p = stack.pop()
        # print "WORKING {}".format(luigi.task.task_id_str(p.task_family, p.to_str_params(only_significant=True)))
        if p.__class__.__name__ is 'DriverTask':
            # DriverTask is a WrapperTask, it produces no bundles.
            continue
        if resolve_bundle(pfs, p, is_left_edge_task(p), data_context):
            num_reuse += 1
        else:
            num_regen += 1

    # If regen == 0, then there is nothing to run.
    return num_regen > 0


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

    
def resolve_bundle(pfs, pipe, is_left_edge_task, data_context):
    """
    Args:
        pfs: pipe fs object
        pipe: the pipe to investigate
        is_left_edge_task: True if this task starts the DAG.
        data_context: the data context object from which we should resolve bundles.

    Returns:
        bool: True if bundle found (not re-running).  False if bundle not found or being regenerated

    """

    # These are constants
    verbose = False
    use_bundle = True
    regen_bundle = False

    # 1.) Get output bundle for pipe_id (the specific pipeline/transform/param hash).

    if verbose: print("resolve_bundle: looking up bundle {}".format(pipe.pipe_id()))

    if pipe._mark_force and not worker._is_external(pipe):
        # Forcing recomputation through a manual annotation in the pipe.pipe_requires() itself
        # If it is external, we don't recompute in any case.
        _logger.debug("resolve_bundle: pipe.mark_force forcing a new output bundle.")
        if verbose: print("resolve_bundle: pipe.mark_force forcing a new output bundle.\n")
        pfs.new_output_hframe(pipe, is_left_edge_task, data_context=data_context)
        return regen_bundle

    if pipe.force:
        # Forcing recomputation through a manual --force directive
        _logger.debug("resolve_bundle: --force forcing a new output bundle.")
        if verbose: print("resolve_bundle: --force forcing a new output bundle.\n")
        pfs.new_output_hframe(pipe, is_left_edge_task, data_context=data_context)
        return regen_bundle

    bndls = pfs.get_hframe_by_proc(pipe.pipe_id(), getall=True, data_context=data_context)
    if bndls is None or len(bndls) <= 0:
        if verbose: print("resolve_bundle: No bundle with proc_name {}, getting new output bundle.\n".format(pipe.pipe_id()))
        # no bundle, force recompute
        pfs.new_output_hframe(pipe, is_left_edge_task, data_context=data_context)
        return regen_bundle

    bndl = bndls[0]  # our best guess is the most recent bundle with the same pipe_id()

    # 2.) Bundle exists - lineage object tells us input bundles.
    lng = bndl.get_lineage()
    if lng is None:
        if verbose: print("resolve_bundle: No lineage present, getting new output bundle.\n")
        pfs.new_output_hframe(pipe, is_left_edge_task, data_context=data_context)
        return regen_bundle

    # 3.) Lineage record exists -- if new code, re-run
    current_version = pipe_base.get_pipe_version(pipe)
    if different_code_versions(current_version, lng):
        if verbose: print("resolve_bundle: New code version, getting new output bundle.\n")
        pfs.new_output_hframe(pipe, is_left_edge_task, data_context=data_context)
        return regen_bundle

    # 3.5.) Have we changed the output human bundle name?  If so, re-run task.
    # Note: we need to go through all the bundle versions with that processing_id.
    # because, at the moment, we make new bundles when we change name.  When in some sense
    # it's just a tag set that should include other names and the data should be the same.

    current_human_name = pipe.pipeline_id()
    found = False
    for bndl in bndls:
        if current_human_name == bndl.get_human_name():
            found = True
            break
    if not found:
        if verbose: print("resolve_bundle: New human name {} (prior {}), getting new output bundle.\n".format(
            current_human_name, bndl.get_human_name()))
        pfs.new_output_hframe(pipe, is_left_edge_task, data_context=data_context)
        return regen_bundle

    # 4.) Check the inputs -- assumes we have processed upstream tasks already
    for task in pipe.requires():
        """ Are we re-running an upstream input (look in path cache)?
        At this time the only bundles a task depends on are the ones created by its upstream tasks.
        We have to look through its *current* list of possible upstream tasks, not the ones it had
        on its prior run.   If the UUID has changed relative to lineage, then
        we need to re-run.
        
        In general, the only reason we should re-run an upstream is b/c of a code change.  And that change
        did not change the tasks parameters.  So it looks the same, but it is actually different.  OR someone 
        re-runs a sql query and the table has changed and the output changes those the parameters are the same. 
        Sometimes folks remove an output to force a single stage to re-run, just for that reason. 
        
        But if an output exists and we want to ignore code version and ignore data changes then
        while we do this, we should re-use our bundle independent of whether an upstream needs to re-run 
        or whether one of our inputs is out of date. 
        
        So one option is to ignore upstreams that need to be re-run.  Re-use blindly.  Like Luigi.  
        
        Another option is that anytime we don't have an input bundle, we attempt to read it not just
        locally, but remotely as well.   
        
        """
        pce = pfs.get_path_cache(task)

        LUIGI_RERUN = False

        if LUIGI_RERUN:
            # Ignore whether upstreams had to be re-run b/c they didn't have bundles.
            # Ignore whether this has to be re-run because existing inputs are newer
            continue

        if pce is None:
            # this can happen with bundles created by other pipelines.
            # still surface the warning, but no longer raise exception
            _logger.info(
                "Resolve bundles: input bundle {} with no path cache entry.  Likely produced by other pipesline".format(
                    task.task_id))
        else:
            if pce.rerun:
                if verbose: print("Resolve_bundle: an upstream task is in the pce and is being re-run, so we need to reun. getting new output bundle.\n")
                pfs.new_output_hframe(pipe, is_left_edge_task, data_context=data_context)
                return regen_bundle

            local_bundle = pfs.get_hframe_by_proc(task.task_id, data_context=data_context)
            assert(local_bundle is not None)

            """ Now we need to check if we should re-run this task because an upstream input exists and has been updated        
            Go through each of the inputs used for this current task.  
            POLICY
            1.) if the date is more recent, it is "new" data.
            2.) if it is older, we should require force (but currently do not and re-run).
            XXX TODO: Add date to the depends_on pb data structure to enforce 2 XXX
            """
            for tup in lng.pb.depends_on:
                if tup.hframe_name == local_bundle.pb.processing_name and tup.hframe_uuid != local_bundle.pb.uuid:
                    if verbose: print("Resolve_bundle: prior input bundle {} {} has new uuid {}\n".format(
                        task.task_id,
                        tup.hframe_uuid,
                        local_bundle.pb.uuid))
                    pfs.new_output_hframe(pipe, is_left_edge_task, data_context=data_context)
                    return regen_bundle

    # 5.) Woot!  Reuse the found bundle.
    if verbose: print("resolve_bundle: reusing bundle\n")
    pfs.reuse_hframe(pipe, bndl, is_left_edge_task, data_context=data_context)
    return use_bundle


def main(args):
    """

    Parameters:
        disdat_config:
        args:

    Returns:
        None
    """

    if not fs.DisdatFS().in_context():
        print("Apply unavailable -- Disdat not in a valid context.")
        return

    dynamic_params = json.dumps(common.parse_params(args.params))

    input_tags = common.parse_args_tags(args.input_tag)

    output_tags = common.parse_args_tags(args.output_tag)

    # NOTE: sysexit=False is required for us to pass a data_context object through luigi tasks.
    # Else we build up arguments as strings to run_with_retcodes().  And it crashes because the data_context is
    # not a string.
    result = apply(args.output_bundle, dynamic_params, args.pipe_cls, input_tags, output_tags,
                   args.force,
                   central_scheduler=args.central_scheduler,
                   workers=args.workers,
                   incremental_push=args.incremental_push,
                   incremental_pull=args.incremental_pull)

    # If we didn't successfully run any task, sys.exit with non-zero code
    common.apply_handle_result(result)
