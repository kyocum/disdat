"""
Unit / functional tests for fs and data_context
"""


from sqlalchemy import create_engine
import os
import shutil
import tempfile
import pytest
from _pytest.monkeypatch import MonkeyPatch
import sys
import disdat.hyperframe
import disdat.fs
import disdat.common

##########################################
# Create a local context not in the current users initialized world.
##########################################

testdir = os.path.join(tempfile.gettempdir(), "disdat_tests")

if os.path.exists(testdir):  # and os.path.isfile(os.path.join(meta_dir,META_CTXT_FILE)):
    shutil.rmtree(testdir)

os.makedirs(testdir)


@pytest.fixture(scope="session")
def monkeysession(request):
    mpatch = MonkeyPatch()
    yield mpatch
    mpatch.undo()


@pytest.fixture(scope="module")
def get_db():
    """ Create in-memory DB """
    engine_g = create_engine('sqlite:///:memory:', echo=True)


@pytest.fixture(scope="module")
def disdatfs(monkeysession):
    """
    Create a fixture to create a context for all the tests to share.
    There should be a place for us to make and write stuff to a context.

    Returns:
        (:object `data_context`): One data context

    """

    cwd = os.path.dirname(sys.modules[__name__].__file__)

    # If we want to use a standard config, then set this up.
    # For now, we use the user's config
    #config_dir = os.path.join(cwd, 'config/disdat.cfg')

    _ = disdat.common.DisdatConfig.instance(meta_dir_root=testdir)

    pfs = disdat.fs.DisdatFS()

    return pfs


##########################################
# Context Test Calls
##########################################


def test_load_context(disdatfs):
    """
    Using our fs (which will load existing context), make a new branch
    and check it out.

    return:
    """

    context_name = "dsdt-test-branch"

    disdatfs.branch(context_name)
    disdatfs.switch(context_name)
    assert(disdatfs._curr_context.local_ctxt == context_name)
    assert(disdatfs.in_context())

    return


def defunct_test_add_and_load_file(disdatfs):
    """

    This is a defunct test.
    1.) add shouldn't be a task.
    2.) the test_add_xx.xx files have broken s3 paths so making bundle will fail.

    So, fix add by using the api, then test creating a bundle that way.

    return:
    """

    branch_name = "dsdt-test-add-branch"

    disdatfs.branch(branch_name)
    disdatfs.switch(branch_name)

    bundle     = 'dsdt-test-bundle'

    cwd      = os.path.dirname(sys.modules[__name__].__file__)
    test_csv = os.path.join(cwd, '../data/test_add_hf.csv')
    test_tsv = os.path.join(cwd, '../data/test_add_hf.tsv')

    disdatfs.add(bundle, test_csv, {})
    disdatfs.add(bundle, test_tsv, {})

    return


def _add_and_load_dir(disdatfs):
    """

    We have a context.  Load it.

    return:
    """

    branch_name = "dsdt-test-add-branch"

    disdatfs.branch(branch_name)
    disdatfs.switch(branch_name)

    hf_name     = 'dsdt-test-hframe'

    cwd      = os.path.dirname(sys.modules[__name__].__file__)
    test_dir = os.path.join(cwd, './data/testfiles')

    disdatfs.add(hf_name, test_dir)

    return


##########################################
# XXX Test Calls
##########################################


if __name__ == '__main__':
    test_load_context(disdatfs)
