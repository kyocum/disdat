import pytest
import os

from disdat.common import DisdatConfig, PROJECT_CONFIG_NAME, META_DIR
from disdat.init import init

# Test DisdatConfig can be created
def test_successful_config_init(tmpdir):
    test_dir = tmpdir / 'sub'
    meta_dir = tmpdir / 'meta'

    # Init with simulated project root
    init(directory=str(test_dir))

    # Assert directory exists
    assert PROJECT_CONFIG_NAME in [x.basename for x in test_dir.listdir()]

    # Change directory to be in simulated project
    os.chdir(str(test_dir))

    DisdatConfig(meta_dir_root=str(meta_dir))

    # Assert that the Meta Directory is present
    assert META_DIR in [x.basename for x in meta_dir.listdir()]

    # Check that recreating the object will not fail
    DisdatConfig(meta_dir_root=str(meta_dir))

    # Assert that the Meta directory is present and there is only one
    assert META_DIR in [x.basename for x in meta_dir.listdir()]
    assert len(meta_dir.listdir()) == 1

# Test that project needs to have config in the path in order for the config to work
def test_failed_config_init(tmpdir):
    tmpdir.mkdir('sub')
    test_dir = tmpdir / 'sub'

    # Assert config directory does not exist
    assert PROJECT_CONFIG_NAME not in [x.basename for x in test_dir.listdir()]

    os.chdir(str(test_dir))

    with pytest.raises(SystemExit) as ex:
        DisdatConfig()

    # Assert Exited with error code of 1
    assert ex.type == SystemExit
    assert ex.value.code == 1
