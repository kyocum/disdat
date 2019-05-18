#
# Copyright 2017 Human Longevity, Inc.
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
Bundle Local FS Links Example

First task creates N files and passes them to the second task.
Some files are Luigi targets, some files are created directly in the bundle directory.

This examples shows the different ways one can create and return files in Disdat:
The CreateFiles task creates files using:
1.) self.create_output_file(<your path>)  This will put a file directly in the output file.
2.) Or self.get_output_dir() This gives you a directory you can directly place files into.
Then you can return files by either:
1.) Return the luigi.Target object that self.create_output_file() returned
2.) Or return the directory returned by self.get_output_dir()
3.) Or return a file that exists somewhere on your FS


Pre Execution:
$export PYTHONPATH=$DISDAT_HOME/disdat/examples/pipelines
$dsdt context examples; dsdt switch examples

Execution:
$python ./files.py
or:
$dsdt apply - ReadFiles.example.output files.ReadFiles --num_luigi_files 5


author: Kenneth Yocum
"""

from disdat.pipe import PipeTask
import disdat.api as api
import luigi
import os
from collections import defaultdict


class CreateFiles(PipeTask):
    """ Example showing how to create file outputs and tell Disdat about them.
    1.) Return a local FS path
    2.) Return a luigi.Target object

    Luigi Parameters:
        input_row (str): json string

    """
    num_luigi_files  = luigi.IntParameter(default=2)
    num_dir_files    = luigi.IntParameter(default=2)

    def pipe_run(self):
        """ Create num_files output files.

        Returns:
            (dict):  dictionary of files
        """

        # Track the locations of your output files
        outputs = defaultdict(list)

        # Create files using Luigi.Target class
        for i in range(int(self.num_luigi_files)):
            target = self.create_output_file("new_dir/lf_output_{}".format(i))
            outputs['luigi target files'].append(target)
            with target.open('w') as of:
                of.write("Luigi file test string {}".format(i))

        # Create files directly inside output bundle dir, simply pass output dir in outputs
        output_dir = self.get_output_dir()
        outputs['output directory files'].append(output_dir)
        for i in range(int(self.num_dir_files)):
            f = os.path.join(output_dir, "dir_output_{}".format(i))
            with open(f, 'w') as of:
                of.write("Output dir file test string {}".format(i))

        # Point to a file that already exists anywhere in the FS
        outputs['pre-existing files'].append(os.path.abspath(__file__))

        return outputs


class ReadFiles(PipeTask):
    """ Consume files from an upstream task
    """
    num_luigi_files  = luigi.IntParameter(default=2)
    num_dir_files    = luigi.IntParameter(default=2)

    def pipe_requires(self):
        """ No new tasks
        Returns:
            None
        """
        self.add_dependency("input_files", CreateFiles, {'num_luigi_files': self.num_luigi_files,
                                                         'num_dir_files': self.num_dir_files})
        return None

    def pipe_run(self, input_files=None):
        """ For each file, print out its name and contents.
        """
        max_len = 0
        nfiles = 0
        for k, v in input_files.items():
            for f in v:
                with open(f,'r') as of:
                    s = of.read()
                    if len(s) > max_len:
                        max_len = len(s)
                    print ("Reading file: {} length:{}".format(f, len(s)))
                    nfiles += 1

        return {'num categories': [len(input_files)], 'num files': [nfiles], 'max string': [max_len]}


if __name__ == "__main__":
    api.apply('examples', 'ReadFiles', params={'num_luigi_files':5})
