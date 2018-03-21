"""
pipe_base.py

Unify DriverTask and PipeTask with one abstract base class.

"""

# Using print as a function makes it easier to switch between printing
# during development and using logging.{debug, info, ...} in production.
from __future__ import print_function

from abc import ABCMeta, abstractmethod
from disdat.fs import DisdatFS
from disdat.data_context import DataContext
from disdat.hyperframe import LineageRecord, HyperFrameRecord, FrameRecord

import disdat.common as common
import os
import logging
import luigi
import getpass
import subprocess
import inspect
import collections
from urlparse import urlparse
import numpy as np
import pandas as pd
import disdat.hyperframe_pb2 as hyperframe_pb2

_logger = logging.getLogger(__name__)

CodeVersion = collections.namedtuple('CodeVersion', 'semver hash tstamp branch url dirty')


def _run_git_cmd(git_dir, git_cmd, get_output=False):
    '''Run a git command in a local git repository.

    :param git_dir: A path within a local git repository, i.e. it may be a
        subdirectory within the repository.
    :param git_cmd: The git command string to run, i.e., everything that
        would follow after :code:`git` on the command line.
    :param get_output: If :code:`True`, return the command standard output
        as a string; default is to return the command exit code.
    '''
    cmd = ['git', '-C', git_dir] + git_cmd.split()
    _logger.debug('Running git command {}'.format(cmd))
    if get_output:
        try:
            with open(os.devnull, 'w') as null_file:
                output = subprocess.check_output(cmd, stderr=null_file)
        except subprocess.CalledProcessError as e:
            _logger.error("Failed to run git command {}: Got exit code {}".format(cmd, e.returncode))
            return e.returncode
    else:
        with open(os.devnull, 'w') as null_file:
            output = subprocess.call(cmd, stdout=null_file, stderr=null_file)
    return output


def get_pipe_version(pipe_instance):
    '''Get a pipe version record.

    :param pipe_instance: An instance of a pipe class.
    :return: a version record
    :rtype: a code:`CodeVersion` named tuple
    '''
    source_file = inspect.getsourcefile(pipe_instance.__class__)
    git_dir = os.path.dirname(source_file)

    # ls-files will verify both that a source file is located in a local
    # git repository and that it is under version control.
    # TODO I bet this STILL doesn't work with git add.
    git_ls_files_cmd = 'ls-files --error-unmatch {}'.format(source_file)
    if _run_git_cmd(git_dir, git_ls_files_cmd) == 0:
        # Get the hash and date of the last commit for the pipe.
        git_tight_hash_cmd = 'log -n 1 --pretty=format:"%h;%aI" -- {}'.format(source_file)
        git_tight_hash_result = _run_git_cmd(git_dir, git_tight_hash_cmd, get_output=True).split(';')
        if len(git_tight_hash_result) == 2:
            tight_hash, tight_date = git_tight_hash_result

            git_tight_dirty_cmd = 'diff-index --name-only HEAD -- {}'.format(source_file)
            tight_dirty = len(_run_git_cmd(git_dir, git_tight_dirty_cmd, get_output=True)) > 0

            git_curr_branch_cmd = 'rev-parse --abbrev-ref HEAD'
            curr_branch = _run_git_cmd(git_dir, git_curr_branch_cmd, get_output=True).rstrip()

            git_fetch_url_cmd = 'config --get remote.origin.url'
            fetch_url = _run_git_cmd(git_dir, git_fetch_url_cmd, get_output=True).rstrip()

            obj_version = CodeVersion(semver="0.1.0", hash=tight_hash, tstamp=tight_date, branch=curr_branch, url=fetch_url, dirty=tight_dirty)
        elif len(git_tight_hash_result) == 1 and git_tight_hash_result[0] == '':
            # git has the file but does not have a hash for the file,
            # which means that the file is a newly git-added file.
            # TODO: fake a hash, use date == now()
            _logger.warning('{}.{}: Source file {} added but not committed to git repository'.format(pipe_instance.__module__, pipe_instance.__class__.__name__, source_file))
            obj_version = CodeVersion(semver="0.1.0", hash='', tstamp='', branch='', url='', dirty=True)
        else:
            raise ValueError("Got invalid git hash: expected either a hash;date or a blank, got {}".format(git_tight_hash_result))
    else:
        _logger.warning('{}.{}: Source file {} not under git version control'.format(pipe_instance.__module__, pipe_instance.__class__.__name__, source_file))
        # TODO: fake a hash, use date == now()
        obj_version = CodeVersion(semver="0.1.0", hash='', tstamp='', branch='', url='', dirty=True)

    return obj_version


class PipeBase(object):
    __metaclass__ = ABCMeta

    BUNDLE_META = 'bundle_meta'
    BUNDLE_LINEAGE = 'bundle_lineage'
    HFRAME = 'hframe'
    FRAME = 'frame'
    AUTH = 'auth'

    @property
    def pfs(self):
        return DisdatFS()

    @abstractmethod
    def bundle_outputs(self):
        """
        Given this pipe, return the set of bundles created by this pipe.
        Mirrors Luigi task.outputs()

        :param pipe_task:  A PipeTask or a DriverTask (both implement PipeBase)
        :return:  list of bundle names
        """
        pass

    @abstractmethod
    def bundle_inputs(self):
        """

        Given this pipe, return the set of bundles created by the input pipes.
        Mirrors Luigi task.inputs()

        :param pipe_task:  A PipeTask or a DriverTask (both implement PipeBase)
        Returns
            [(bundle_name, uuid), ... ]
        """
        pass

    @abstractmethod
    def pipe_id(self):
        """
        Given a pipe instance, return a unique string based on the class name and
        the parameters.

        Bundle Tag:   Used to fill in bundle.processing_name
        """
        pass

    @abstractmethod
    def pipeline_id(self):
        """
        This is a "less unique" id than the unique id.  It is supposed to be the "human readable" name of the stage
        this pipe occupies in the pipesline.

        Bundle Tag:   Used to fill in bundle.bundle_name
        """
        pass

    @staticmethod
    def add_bundle_meta_files(pipe_task):
        """
        Given a pipe or driver task, create the bundle metaoutput files and Luigi
        output targets for them.

        Use the pipe_task (or driver task) to get the name of the bundle.
        Use the name of the bundle to look up the output path in the pipe cache in the
        PipeFS class object.

        Create an hframe.  The individual frame records have to be written out before hand.

        Args:
            pipe_task: The pipe task that will use these outputs

        Returns:
            [ luigi output for meta file, luigi output for lineage file ]

        """
        pce = DisdatFS.get_path_cache(pipe_task)

        if pce is None:
            # This can happen when the pipe has been created with non-deterministic parameters
            _logger.error("add_bundle_meta_files: could not find pce for task {}".format(pipe_task.pipe_id()))
            _logger.error("It is possible one of your tasks is parameterized in a non-deterministic fashion.")
            raise Exception("add_bundle_meta_files: Unable to find pce for task {}".format(pipe_task.pipe_id()))

        hframe = {PipeBase.HFRAME: luigi.LocalTarget(os.path.join(pce.path, HyperFrameRecord.make_filename(pce.uuid)))}

        return hframe

    def make_hframe(self, output_frames, output_bundle_uuid, depends_on,
                    human_name=None, processing_name=None, tags=None, presentation=hyperframe_pb2.DEFAULT):
        """
        Create HyperFrameRecord or HFR
        HFR contains a LineageRecord
        HFR contains UUIDs of FrameRecords or FRs
        FR contains data or LinkRecords

        Use the pipe_task to look in the path cache for the output directory
        Use the pipe_task outputs to find the named file for the final HF proto buf file.
        Write out all Frames, and at the very last moment, write out the HF proto buff.

        Args:
            output_frames (:list:`FrameRecord`):  List of frames to be placed in bundle / hframe
            output_bundle_uuid:
            depends_on (:list:tuple):  must be the processing_name, uuid of the upstream pipes / base bundles
            human_name:
            processing_name:
            tags:
            presentation (enum):  how to present this hframe when we use it as input to a function -- default None

            That default means it will be a HF, but it wasn't a "presentable" hyperframe.

        Returns:
            `HyperFrameRecord`
        """

        # Set up the bundle name and the processing_name.  These default bundle "tags" identify bundles
        if human_name is None:
            output_bundle_name = self.pipeline_id()
        else:
            output_bundle_name = human_name

        if processing_name is None:
            output_processing_name = self.pipe_id()
        else:
            output_processing_name = processing_name

        # Grab code version and path cache entry -- only called if we ran
        cv = get_pipe_version(self)

        lr = LineageRecord(hframe_name=output_processing_name,
                           hframe_uuid=output_bundle_uuid,
                           code_repo=cv.url,
                           code_name='unknown',
                           code_semver=cv.semver,
                           code_hash=cv.hash,
                           code_branch=cv.branch,
                           depends_on=depends_on)

        hfr = HyperFrameRecord(owner=getpass.getuser(),
                               human_name=output_bundle_name,
                               processing_name=output_processing_name,
                               uuid=output_bundle_uuid,
                               frames=output_frames,
                               lin_obj=lr,
                               tags=tags,
                               presentation=presentation)

        return hfr

    @staticmethod
    def _interpret_scheme(full_path):
        scheme = urlparse(full_path).scheme

        if scheme == '' or scheme == 'file':
            ''' LOCAL FILE '''
            return luigi.LocalTarget(full_path)
        elif scheme == 's3':
            ''' S3  FILE '''
            return luigi.s3.S3Target(full_path)

        assert False

    def make_luigi_targets_from_fqp(self, output_value):
        """
        Given Fully Qualified Path -- Determine the Luigi objects

        This is called from the output of PipeExternalBundle.

        Given [], return [] of Luigi targets.
        If len([]) == 1, return without []


        Args:
            output_value:

        Returns:

        """

        if isinstance(output_value, list) or isinstance(output_value, tuple) or isinstance(output_value, dict):
            assert False
        else:
            # This is principally for PipesExternalBundle, in which there is no index.
            luigi_outputs = self._interpret_scheme(output_value)
            print("OUTPUT VAL {} output {}".format(output_value, luigi_outputs))

        return luigi_outputs

    def make_luigi_targets_from_basename(self, output_value):
        """
        Determine the output paths AND create the Luigi objects.

        Return the same object type as output_value, but with Luigi.Targets instead.

        Note that we get the path from the DisdatFS Path Cache.   The path cache is a dictionary from
        pipe.unique_id() to a path_cache_entry, which contains the fields: instance uuid path rerun

        Args:
            output_value (str, dict, list): A basename, dictionary of basenames, or list of basenames.

        Return:
            (`luigi.LocalTarget`, `luigi.S3Target`): Singleton, list, or dictionary of Luigi Target objects.
        """

        # Find the path cache entry for this pipe to find its output path
        pce = self.pfs.get_path_cache(self)
        assert(pce is not None)

        if isinstance(output_value, list) or isinstance(output_value, tuple):
            luigi_outputs = []
            for i in output_value:
                full_path = os.path.join(pce.path, i)
                luigi_outputs.append(self._interpret_scheme(full_path))
            if len(luigi_outputs) == 1:
                luigi_outputs = luigi_outputs[0]
        elif isinstance(output_value, dict):
            luigi_outputs = {}
            for k, v in output_value.iteritems():
                full_path = os.path.join(pce.path, v)
                luigi_outputs[k] = self._interpret_scheme(full_path)
        else:
            full_path = os.path.join(pce.path, output_value)
            luigi_outputs = self._interpret_scheme(full_path)

        return luigi_outputs

    def parse_pipe_return_val(self, hfid, val, human_name=None):
        """

        Interpret the return values and create an HFrame to wrap them.
        This means setting the correct presentation bit in the HFrame so that
        we call downstream tasks with parameters as the author intended.

        POLICY / NOTE:  An non-HF output is a Presentable.
        NOTE: For now, a task output is *always* presentable.
        NOTE: No other code should set presentation in a HyperFrame.

        The mirror to this function (that unpacks a presentable is disdat.fs.present_hfr()

        Args:
            hfid:
            val:

        Returns:
            Frames, Presentation

        """
        frames = []

        managed_path = os.path.join(self.pfs.get_curr_context().get_object_dir(), hfid)

        if val is None:
            presentation = hyperframe_pb2.HF

        elif isinstance(val, HyperFrameRecord):
            presentation = hyperframe_pb2.HF
            frames.append(FrameRecord.make_hframe_frame(hfid, self.pipeline_id(), [val]))

        elif isinstance(val, np.ndarray) or isinstance(val, list):
            presentation = hyperframe_pb2.TENSOR
            if isinstance(val, list):
                val = np.array(val)
            frames.append(DataContext.convert_serieslike2frame(hfid, common.DEFAULT_FRAME_NAME + ':0', val, managed_path))

        elif isinstance(val, tuple):
            presentation = hyperframe_pb2.ROW
            for i, _ in enumerate(tuple):
                frames.append(DataContext.convert_serieslike2frame(hfid, common.DEFAULT_FRAME_NAME + ':{}'.format(i), val, managed_path))

        elif isinstance(val, dict):
            presentation = hyperframe_pb2.ROW
            for k, v in val.iteritems():
                if False: # For now require dict values to be sequences
                    if not isinstance(v, collections.Sequence):
                        frames.append(DataContext.convert_scalar2frame(hfid, k, v, managed_path))
                assert isinstance(v, (list, tuple, pd.core.series.Series, np.ndarray))
                frames.append(DataContext.convert_serieslike2frame(hfid, k, v, managed_path))

        elif isinstance(val, pd.DataFrame):
            presentation = hyperframe_pb2.DF
            frames.extend(DataContext.convert_df2frames(hfid, val, managed_path))

        else:
            presentation = hyperframe_pb2.SCALAR
            frames.append(DataContext.convert_scalar2frame(hfid, common.DEFAULT_FRAME_NAME + ':0', val, managed_path))

        hfr = self.make_hframe(frames, hfid, self.bundle_inputs(),
                               human_name=human_name,
                               tags={"presentable": "True"},
                               presentation=presentation)

        return hfr