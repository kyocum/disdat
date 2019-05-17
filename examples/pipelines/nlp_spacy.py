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

from disdat.pipe import PipeTask
import disdat.api as api
import luigi
import logging
import spacy
import os
import pkg_resources
import sys

"""
NLP Spacy

Single Disdat task that loads the Spacy english model.

One dockerizes using the The $DISDAT_HOME/examples/setup.py file. 
The Disdat dockerizer depends on you creating a setup.py that can create a source distribution.
I.e., `python setup.py sdist` should produce valid source distribution artifacts.
As part of that you can tell setuptools to include data files through a MANIFEST.in file.
To include the Spacy EN model we added a MANIFEST.in file at the same level as setup.py, and 
added a line to include those data files in the source distribution. 

This is a simple Disdat task that just loads that model to validate the data is available. 

"""

_logger = logging.getLogger(__name__)


example_text= "Neural networks have become an essential part of machine learning workflows."

spacy_path = pkg_resources.resource_filename('pipelines', '')

if sys.version_info > (3, 0):
    P3 = True
    P2 = False
else:
    P3 = False
    P2 = True

class SimpleNLP(PipeTask):
    """
    Create a simple binary tree of tasks.   First level is B, last level is C. 
    """
    text = luigi.Parameter(default=example_text)

    def pipe_run(self, **kwargs):
        """ Load spacy english model and do something simple
        """
        global spacy_path

        spacy_path = os.path.join(spacy_path, 'en_core_web_sm', 'en_core_web_sm-2.1.0')
        if not P2:
            nlp = spacy.load(spacy_path, disable=['parser', 'ner'])
        else:
            print ("Python 2.x environment detected.  Spacy has narrow unicode build issues.")

        return True


if __name__ == "__main__":
    api.apply('examples', 'SimpleNLP')
