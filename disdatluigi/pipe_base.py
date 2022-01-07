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

from abc import ABCMeta, abstractmethod
import os

import luigi
from luigi.contrib.s3 import S3Target
from six.moves import urllib

from disdat.fs import DisdatFS

from disdatluigi import logger as _logger


MISSING_EXT_DEP_UUID = 'UnresolvedExternalDep'
YIELD_PIPETASK_ARG_NAME = "YieldArgName"

class PipeBase(object):
    __metaclass__ = ABCMeta

    @property
    def pfs(self):
        return DisdatFS()

    @abstractmethod
    def output_bundle(self):
        """
        Given this pipe, return the set of bundles created by this pipe.
        Mirrors Luigi task.outputs()

        Returns:
            (processing_name, uuid)
        """
        pass

    @abstractmethod
    def input_bundles(self):
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
