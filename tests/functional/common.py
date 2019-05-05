import pytest
import os

import disdat.api as api
from disdat.common import DisdatConfig

TEST_CONTEXT = '___test_context___'


@pytest.fixture(autouse=True)
def run_test(tmpdir):
    # Remove test context before running test
    setup(tmpdir)
    yield
    api.delete_context(context_name=TEST_CONTEXT)


def setup(tmpdir):
    test_dir = tmpdir / 'project'

    # Init with simulated project root
    DisdatConfig.init(directory=str(test_dir))
    os.chdir(str(test_dir))

    if TEST_CONTEXT in api.ls_contexts():
        api.delete_context(context_name=TEST_CONTEXT)

    api.context(context_name=TEST_CONTEXT)


def init_project(directory):
    # Init with simulated project root
    DisdatConfig.init(directory=str(directory))
    os.chdir(str(directory))