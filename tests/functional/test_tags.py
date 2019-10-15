import uuid
import pytest

from disdat import api, common
from disdat.pipe import PipeTask


TAGS = {'tag1': 'omg', 'tag2': 'it works'}


class Source(PipeTask):

    def pipe_requires(self):
        self.set_bundle_name('tagged')

    def pipe_run(self):
        self.add_tags(TAGS)
        return 0


class Destination(PipeTask):

    def pipe_requires(self):
        self.set_bundle_name('output')
        self.add_dependency('tagged', Source, params={})

    def pipe_run(self, tagged):
        tags = self.get_tags('tagged')
        assert tags is not TAGS
        assert tags == TAGS
        return 1


@pytest.fixture
def context():

    try:
        print('ensuring disdat is initialized')
        common.DisdatConfig.init()
    except:
        print('disdat already initialized, no worries...')

    print('creating temporary local context')
    context = uuid.uuid1().hex
    api.context(context)

    yield context

    print('deleting temporary local context')
    api.delete_context(context)


class TestContext:

    def test_tags(self, context):
        api.apply(context, Destination)


if __name__ == '__main__':
    pytest.main([__file__])
