"""
pipe_base.py

Unify DriverTask and PipeTask with one abstract base class.

"""

# Using print as a function makes it easier to switch between printing
# during development and using logging.{debug, info, ...} in production.
from __future__ import print_function

from abc import ABCMeta, abstractmethod
import os
import shutil
import collections

import luigi
from luigi.contrib.s3 import S3Target
import six
from six.moves import urllib
import numpy as np
import pandas as pd

import disdat.common as common
from disdat.fs import DisdatFS
from disdat.data_context import DataContext
from disdat.hyperframe import HyperFrameRecord, FrameRecord
import disdat.hyperframe_pb2 as hyperframe_pb2
from disdat.path_cache import PathCache
from disdat import logger as _logger


CodeVersion = collections.namedtuple('CodeVersion', 'semver hash tstamp branch url dirty')


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
    def bundle_output(self):
        """
        Given this pipe, return the set of bundles created by this pipe.
        Mirrors Luigi task.outputs()

        Returns:
            (processing_name, uuid)
        """
        pass

    @abstractmethod
    def bundle_inputs(self):
        """
        Given this pipe, return the set of bundles created by the input pipes.
        Mirrors Luigi task.inputs()

        :param pipe_task:  A PipeTask or a DriverTask (both implement PipeBase)
        Returns:
            [(processing_name, uuid), ... ]
        """
        pass

    @abstractmethod
    def processing_id(self):
        """
        Given a pipe instance, return a unique string based on the class name and
        the parameters.

        Bundle Tag:   Used to fill in bundle.processing_name
        """
        pass

    @abstractmethod
    def human_id(self):
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
        pce = PathCache.get_path_cache(pipe_task)

        if pce is None:
            # This can happen when the pipe has been created with non-deterministic parameters
            _logger.error("add_bundle_meta_files: could not find pce for task {}".format(pipe_task.processing_id()))
            _logger.error("It is possible one of your tasks is parameterized in a non-deterministic fashion.")
            raise Exception("add_bundle_meta_files: Unable to find pce for task {}".format(pipe_task.processing_id()))

        hframe = {PipeBase.HFRAME: luigi.LocalTarget(os.path.join(pce.path, HyperFrameRecord.make_filename(pce.uuid)))}

        return hframe

    @staticmethod
    def _interpret_scheme(full_path):
        scheme = urllib.parse.urlparse(full_path).scheme

        if scheme == '' or scheme == 'file':
            ''' LOCAL FILE '''
            return luigi.LocalTarget(full_path)
        elif scheme == 's3':
            ''' S3  FILE '''
            return S3Target(full_path)

        assert False

    @staticmethod
    def filename_to_luigi_targets(output_dir, output_value):
        """
        Create Luigi file objects from a file name, dictionary of file names, or list of file names.

        Return the same object type as output_value, but with Luigi.Targets instead.

        Args:
            output_dir (str): Managed output path.
            output_value (str, dict, list): A basename, dictionary of basenames, or list of basenames.

        Return:
            (`luigi.LocalTarget`, `luigi.contrib.s3.S3Target`): Singleton, list, or dictionary of Luigi Target objects.
        """

        if isinstance(output_value, list) or isinstance(output_value, tuple):
            luigi_outputs = []
            for i in output_value:
                full_path = os.path.join(output_dir, i)
                luigi_outputs.append(PipeBase._interpret_scheme(full_path))
            if len(luigi_outputs) == 1:
                luigi_outputs = luigi_outputs[0]
        elif isinstance(output_value, dict):
            luigi_outputs = {}
            for k, v in output_value.items():
                full_path = os.path.join(output_dir, v)
                luigi_outputs[k] = PipeBase._interpret_scheme(full_path)
        else:
            full_path = os.path.join(output_dir, output_value)
            luigi_outputs = PipeBase._interpret_scheme(full_path)

        return luigi_outputs

    @staticmethod
    def rm_bundle_dir(output_path, uuid):
        """
        We created a directory (managed path) to hold the bundle and any files.   The files have been
        copied in.   Removing the directory removes any created files.  If the user has told us about
        any DBTargets, also call rm() on those.

        TODO: Integrate with data_context bundle remove.   That deals with information already
        stored in the local DB.

        ASSUMES:  That we haven't actually updated the local DB with information on this bundle.

        Args:
            output_path (str):
            uuid (str):
            db_targets (list(DBTarget)):

        Returns:
            None
        """
        try:
            shutil.rmtree(output_path, ignore_errors=True)
            os.rmdir(output_path)
            # TODO: if people create s3 files, s3 file targets, inside of an s3 context,
            # TODO: then we will have to clean those up as well.
        except IOError as why:
            _logger.error("Removal of hyperframe directory {} failed with error {}. Continuing removal...".format(
                uuid, why))

    @staticmethod
    def parse_return_val(hfid, val, data_context):
        """
        Interpret the return values and create an HFrame to wrap them.
        This means setting the correct presentation bit in the HFrame so that
        we call downstream tasks with parameters as the author intended.

        POLICY / NOTE:  An non-HF output is a Presentable.
        NOTE: For now, a task output is *always* presentable.
        NOTE: No other code should set presentation in a HyperFrame.

        The mirror to this function (that unpacks a presentable is disdat.fs.present_hfr()

        Args:
            hfid (str): UUID
            val (object): A scalar, dict, tuple, list, dataframe
            data_context (DataContext): The data context into which to place this value

        Returns:
            (presentation, frames[])

        """

        possible_scalar_types = (
            int,
            float,
            str,
            bool,
            np.bool_,
            np.int8,
            np.int16,
            np.int32,
            np.int64,
            np.uint8,
            np.uint16,
            np.uint32,
            np.uint64,
            np.float16,
            np.float32,
            np.float64,
            six.binary_type,
            six.text_type,
            np.unicode_,
            np.string_
        )

        frames = []

        if val is None:
            """ None's stored as json.dumps([None]) or '[null]' """
            presentation = hyperframe_pb2.JSON
            frames.append(data_context.convert_scalar2frame(hfid, common.DEFAULT_FRAME_NAME + ':0', val))

        elif isinstance(val, HyperFrameRecord):
            presentation = hyperframe_pb2.HF
            frames.append(FrameRecord.make_hframe_frame(hfid, pipe.human_id(), [val]))

        elif isinstance(val, np.ndarray) or isinstance(val, list):
            presentation = hyperframe_pb2.TENSOR
            if isinstance(val, list):
                val = np.array(val)
            frames.append(data_context.convert_serieslike2frame(hfid, common.DEFAULT_FRAME_NAME + ':0', val))

        elif isinstance(val, tuple):
            presentation = hyperframe_pb2.ROW
            val = np.array(val)
            frames.append(data_context.convert_serieslike2frame(hfid, common.DEFAULT_FRAME_NAME + ':0', val))

        elif isinstance(val, dict):
            presentation = hyperframe_pb2.ROW
            for k, v in val.items():
                if not isinstance(v, (list, tuple, pd.core.series.Series, np.ndarray, collections.Sequence)):
                    # assuming this is a scalar
                    assert isinstance(v, possible_scalar_types), 'Disdat requires dictionary values to be one of {} not {}'.format(possible_scalar_types, type(v))
                    frames.append(data_context.convert_scalar2frame(hfid, k, v))
                else:
                    assert isinstance(v, (list, tuple, pd.core.series.Series, np.ndarray, collections.Sequence))
                    frames.append(data_context.convert_serieslike2frame(hfid, k, v))

        elif isinstance(val, pd.DataFrame):
            presentation = hyperframe_pb2.DF
            frames.extend(data_context.convert_df2frames(hfid, val))

        else:
            presentation = hyperframe_pb2.SCALAR
            frames.append(data_context.convert_scalar2frame(hfid, common.DEFAULT_FRAME_NAME + ':0', val))

        return presentation, frames

