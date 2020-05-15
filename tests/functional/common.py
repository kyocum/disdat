import pytest

import disdat.api as api

TEST_CONTEXT = '___test_context___'


@pytest.fixture(autouse=True)
def run_test():
    # Remove test context before running test
    setup()
    yield
    api.delete_context(context_name=TEST_CONTEXT)


def setup():
    if TEST_CONTEXT in api.ls_contexts():
        api.delete_context(context_name=TEST_CONTEXT)

    api.context(context_name=TEST_CONTEXT)
