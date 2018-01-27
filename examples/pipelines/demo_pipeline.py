import numpy as np
from disdat.pipe import PipeTask

"""
Example Pipeline
"""


class GenData(PipeTask):
    """
    Generate a small data set of possible basketball scores
    """

    def pipe_requires(self, pipeline_input=None):
        self.set_bundle_name("GenData")

    def pipe_run(self, pipeline_input=None):
        return np.array([77, 100, 88])


class Average(PipeTask):
    """
    Average scores of an upstream task
    """

    def pipe_requires(self, pipeline_input=None):
        """ Depend on GenData """
        self.add_dependency('my_input_data', GenData, {})

    def pipe_run(self, pipeline_input=None, my_input_data=None):
        """ Compute average and return as a dictionary """
        return {'average': [np.average(my_input_data)]}




