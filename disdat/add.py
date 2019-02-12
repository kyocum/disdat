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
Add

This is a "base pipe" like Driver and PipeTask.py.  It handles adding a bundle
to the system.  This allows us to re-use all of the "make_bundle" machinery in
pipe_base, so we're not repeating that code inside fs.py.

author: Kenneth Yocum
"""
from __future__ import print_function

from disdat.pipe_base import PipeBase
import disdat.constants as constants
from disdat.hyperframe import FrameRecord
import disdat.hyperframe_pb2 as hyperframe_pb2
from disdat.fs import DataContext
import luigi
import pandas as pd
import logging
import os
from six.moves import urllib

_logger = logging.getLogger(__name__)


class AddTask(luigi.Task, PipeBase):
    """
    Given a directory or a (csv/tsv) file, create a new local bundle

    Properties:
         input_path:  The data set to be processed
         output_bundle: The name of the collection of resulting data items
    """
    input_path = luigi.Parameter(default=None)
    output_bundle = luigi.Parameter(default=None)
    tags = luigi.DictParameter()

    def __init__(self, *args, **kwargs):
        """
        Initialize some variables we'll use to track state.

        Returns:
        """
        super(AddTask, self).__init__(*args, **kwargs)

    def bundle_outputs(self):
        """
        Override PipeBase.bundle_outputs

        Standard "operator" pipes first create a bundle and make an entry in the pce
        before being called.

        Returns: [(bundle_processing_name, uuid), ... ]
        """

        output_bundles = [(self.pipe_id(), self.pfs.get_path_cache(self).uuid)]
        return output_bundles

    def bundle_inputs(self):
        """
        Override PipeBase.bundle_inputs

        This is a "base" data input bundle.  No inputs.

        Returns: [(bundle_name, uuid), ... ]
        """

        return []

    def pipe_id(self):
        """
        PipeBase.name_output_bundle

        Given a pipe instance, return a unique string based on the class name and
        the parameters.  This re-uses Luigi code for getting the unique string, placed in
        LuigiTask.task_id.   Note that since one of the driver's parameters is the output_bundle_name,
        this name includes the pipesline name (which is the output bundle name).

        """

        return self.task_id

    def pipeline_id(self):
        """
        This is a "less unique" id than the unique id.  It is supposed to be the "human readable" name of the stage
        this pipe occupies in the pipesline.

        For the driver, we are producing the final user-named bundle.   The pipe version of this call produces
        <driver_output_bundle_name>-<task_name>-<task_index>.    However, here there is no task_index and the task_name
        is "Driver" and it is implicit.  So we only output the first part <driver_output_bundle_name>

        Returns:
            (str)
        """
        return self.output_bundle

    def requires(self):
        """ Operates with no upstream dependencies.
        Returns:
            None
        """
        return None

    def output(self):
        """ The driver output only the bundle meta files.  The actual driver bundle
        consists of these files plus the output bundles of all pipes in the pipesline.

        Returns:
            (dict): {PipeBase.BUNDLE_META: luigifileobj, PipeBase.BUNDLE_LINEAGE, luigifileobj}
        """

        return PipeBase.add_bundle_meta_files(self)

    def run(self):
        """ Convert an existing file, csv, or dir to the bundle
        """

        bundle_processing_name, add_hf_uuid = self.bundle_outputs()[0]  # @UnusedVariable
        bundle_hframe_file = self.output()[PipeBase.HFRAME].path
        managed_path = os.path.dirname(bundle_hframe_file)

        if os.path.isdir(self.input_path):
            """ With a directory, add all files under one special frame """
            abs_input_path = os.path.abspath(self.input_path)
            files = [urllib.parse.urljoin('file:', os.path.join(abs_input_path, f)) for f in os.listdir(abs_input_path)]
            file_set = DataContext.copy_in_files(files, managed_path)
            frames = [FrameRecord.make_link_frame(add_hf_uuid, constants.FILE, file_set, managed_path), ]
            presentation = hyperframe_pb2.TENSOR
        elif os.path.isfile(self.input_path):
            if str(self.input_path).endswith('.csv') or str(self.input_path).endswith('.tsv'):
                bundle_df = pd.read_csv(self.input_path, sep=None) # sep=None means python parse engine detects sep
                frames = DataContext.convert_df2frames(add_hf_uuid, bundle_df, managed_path=managed_path)
                presentation = hyperframe_pb2.DF
            else:
                """ Other kinds of file """
                abs_input_path = os.path.abspath(self.input_path)
                files = [urllib.parse.urljoin('file:', abs_input_path)]
                file_set = DataContext.copy_in_files(files, managed_path)
                frames = [FrameRecord.make_link_frame(add_hf_uuid, constants.FILE, file_set, managed_path), ]
                presentation = hyperframe_pb2.TENSOR
        else:
            raise RuntimeError('Unable to find input file or path {}'.format(self.input_path))

        """ Make a single HyperFrame output for an add """

        if 'taskname' in self.tags or 'presentable' in self.tags:
            print("Unable to add bundle {}: tags contain reserved keys 'taskname' or 'presentable'".format(self.output_bundle))
            # Todo: Delete temporary bundle here
            return

        tags = {'taskname': 'add', 'presentable': 'True', 'root_task':'True'}

        tags.update(self.tags)

        task_hfr = self.make_hframe(frames, add_hf_uuid, self.bundle_inputs(),
                                    self.pipeline_id(), self.pipe_id(), self,
                                    tags=tags,
                                    presentation=presentation)

        self.pfs.get_curr_context().write_hframe(task_hfr)

if __name__ == '__main__':
    luigi.run()
