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
Disdat Luigi Interface file

The path cache keeps a binding from luigi tasks to bundles.

"""

import collections

from disdat import logger as _logger

PathCacheEntry = collections.namedtuple('PathCacheEntry', 'instance bundle uuid path rerun')


class PathCache(object):
    """
    PipeCache singelton

    The path cache requires that other components import "disdat.path_cache" and not "import path_cache".
    Otherwise we will put entries in a class object whose name is '<class 'path_cache.PathCache'>' but search in an
    instanceof <class 'disdat.path_cache.PathCache'>. The driver uses the path_cache.PathCache class object, but when
    you run from a pipe defined outside the project, then the system finds the class object with the package name
    appended. And that class object doesn't have a path_cache with anything in it.

    """
    task_path_cache = {}  ## [<pipe/luigi task id>] -> PipeCacheEntry(instance, bundle, uuid, directory, re-run)

    def __init__(self):
        """ The PathCache is a class object that never gets instantiated. """
        assert False, "The PathCache is only a class-only object, do not instantiate."

    @staticmethod
    def clear_path_cache():
        """
        If you grab the singleton instance, and you try to grab the task_path_cache, you will end up reading
        the class variable.  But if you try to set the class variable, you will make a copy and set it instead.
        So to really clear it, make a static method.

        Returns:
            None
        """
        PathCache.task_path_cache.clear()

    @staticmethod
    def get_path_cache(pipe_instance):
        """
        Given a pipe process name, return the resolved path.

        Note: The path cache has to use the pipe.pipe_id(), which is a string
        that takes into account enough of the configuration of the executed pipe that it is
        unique.  That is, it cannot use the pipeline_id(), as that typically is just the human-readable bundle name
        that is the output of the pipeline.

        Args:
            pipe_instance:

        Returns:
            (instance,path,rerun) as PipeCacheEntry

        """
        pipe_name = pipe_instance.processing_id()

        return PathCache.get_path_cache_by_name(pipe_name)

    @staticmethod
    def get_path_cache_by_name(processing_name):
        """
        Given a pipe name, return the resolved path.
        Args:
            processing_name:
        Returns:
              (instance,path,rerun) as PipeCacheEntry
        """
        if processing_name in PathCache.task_path_cache:
            rval = PathCache.task_path_cache[processing_name]
        else:
            rval = None

        return rval

    @staticmethod
    def path_cache():
        """
        Returns:
            (dict): cache dictionary
        """
        return PathCache.task_path_cache

    @staticmethod
    def put_path_cache(pipe_instance, bundle, uuid, path, rerun, overwrite=False):
        """  The path cache is used to associate a pipe instance with its output path and whether
        we have decided to re-run this pipe.   If rerun is True, then there should be no
        ouput at this path.  AND it should eventually be added as a new version of this bundle.

        Args:
            pipe_instance:     instance of a pipe
            bundle (disdat.api.Bundle):  The bundle to hold the output data and metadata
            uuid:              specific uuid of the output path
            path:              where to write the bundle
            rerun:             whether or not we are re-running or re-using
            overwrite:         overwrite existing entry (if exists)

        Returns:
            pce or raise KeyError
        """
        pipe_name = pipe_instance.processing_id()
        pce = PathCacheEntry(pipe_instance, bundle, uuid, path, rerun)
        if pipe_name not in PathCache.task_path_cache:
            PathCache.task_path_cache[pipe_name] = pce
        else:
            if pce == PathCache.task_path_cache[pipe_name]: # The tuples are identical
                _logger.error("path_cache dup key: pipe {} already bound to same PCE {} ".format(pipe_name, pce))
            else:
                if overwrite:
                    PathCache.task_path_cache[pipe_name] = pce
                else:
                    raise KeyError("path_cache dup key: pipe {} bound to pce {} but trying to re-assign to {}".format(
                        pipe_name, PathCache.task_path_cache[pipe_name], pce))
        return pce