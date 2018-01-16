"""
Test for hyperframe implementations.
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
# Protocol Buffer Read/Write to local FS test calls
##########################################

testdir = os.path.join(tempfile.gettempdir(), "hframetests")

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

    monkeysession.setenv("DISDAT_CONFIG_PATH", os.path.join(cwd, 'config/disdat.cfg'))

    disdat_config = disdat.common.DisdatConfig.instance()

    pfs = disdat.fs.DisdatFS(disdat_config)

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

    branch_name = "dsdt-test-branch"

    print "Found disdat fs {}".format(disdatfs)

    disdatfs.branch(branch_name)
    disdatfs.checkout(branch_name)

    return


def test_add_and_load_file(disdatfs):
    """

    We have a context.  Load it.

    return:
    """

    branch_name = "dsdt-test-add-branch"

    disdatfs.branch(branch_name)
    disdatfs.checkout(branch_name)

    hf_name     = 'dsdt-test-hframe'

    cwd      = os.path.dirname(sys.modules[__name__].__file__)
    test_csv = os.path.join(cwd, './data/test_add_hf.csv')
    test_tsv = os.path.join(cwd, './data/test_add_hf.tsv')

    disdatfs.add(hf_name, test_csv)
    disdatfs.add(hf_name, test_tsv)

    return


def _add_and_load_dir(disdatfs):
    """

    We have a context.  Load it.

    return:
    """

    branch_name = "dsdt-test-add-branch"

    disdatfs.branch(branch_name)
    disdatfs.checkout(branch_name)

    hf_name     = 'dsdt-test-hframe'

    cwd      = os.path.dirname(sys.modules[__name__].__file__)
    test_dir = os.path.join(cwd, './data/testfiles')

    disdatfs.add(hf_name, test_dir)

    return


##########################################
# XXX Test Calls
##########################################


