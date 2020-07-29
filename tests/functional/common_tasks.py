from disdat.pipe import PipeTask
import luigi

""" These are simple tasks used for test_api_run """

COMMON_DEFAULT_ARGS=[10, 100, 1000]


class B(PipeTask):
    """ B required by A """
    int_array = luigi.ListParameter(default=None)

    def pipe_run(self):
        print ("B saving type [{}]".format(type(self.int_array)))
        return self.int_array


class A(PipeTask):
    """ A is the root task"""
    int_array = luigi.ListParameter(default=COMMON_DEFAULT_ARGS)

    def pipe_requires(self):
        self.add_dependency('b', B, {'int_array': self.int_array})

    def pipe_run(self, b=None):
        print ("Saving the sum of B {}".format(b))
        print ("A got type [{}]".format(type(b)))
        return sum(list(b))
