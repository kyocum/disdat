import pytest
import os

from disdat.common import DisdatConfig
from disdat.common import PROJECT_CONFIG_DIR, LUIGI_FILE, CFG_FILE


# Test DisdatConfig can be created
def test_successful_config_init(tmpdir):
    test_dir = tmpdir / 'project'

    # Init with simulated project root
    DisdatConfig.init(directory=str(test_dir))

    # Assert directory exists
    assert PROJECT_CONFIG_DIR in [x.basename for x in test_dir.listdir()]

    config_file_path = os.path.join(str(test_dir), PROJECT_CONFIG_DIR)

    # Assert Luigi and Config Files are in directory
    config_files = [LUIGI_FILE, CFG_FILE]

    for config_file in config_files:
        assert config_file in os.listdir(config_file_path), "{} file not present".format(config_file)

    # Assert that instance can be found from simulated project
    DisdatConfig(project_root=str(test_dir))


# Test DisdatConfig instance fails if project has not been initialized
def test_failed_instance(tmpdir):
    with pytest.raises(SystemExit) as ex:
        DisdatConfig(project_root=str(tmpdir))

    # Assert Exited with error code of 1
    assert ex.type == SystemExit
    assert ex.value.code == 1


# Running init in a project that has already been initialized should fail
def test_double_init_fails(tmpdir):
    test_dir = tmpdir / 'project'

    # Init with simulated project root
    DisdatConfig.init(directory=str(test_dir))

    # Assert directory exists
    assert PROJECT_CONFIG_DIR in [x.basename for x in test_dir.listdir()]

    config_file_path = os.path.join(str(test_dir), PROJECT_CONFIG_DIR)

    # Assert Luigi and Config Files are in directory
    config_files = [LUIGI_FILE, CFG_FILE]

    for config_file in config_files:
        assert config_file in os.listdir(config_file_path), "{} file not present".format(config_file)

    # Run second init. This should fail
    with pytest.raises(SystemExit) as ex:
        # Init with simulated project root
        DisdatConfig.init(directory=str(test_dir))

        # Assert Exited with error code of 1
        assert ex.type == SystemExit
        assert ex.value.code == 1


# Test that creating an instance from a subdirectory of a project resolves instance properly
def test_running_instance_from_subdirectory(tmpdir):
    test_dir = tmpdir / 'project'
    sub_dir = tmpdir / 'project' / 'sub'

    # Init with simulated project root
    DisdatConfig.init(directory=str(test_dir))

    # Assert directory exists
    assert PROJECT_CONFIG_DIR in [x.basename for x in test_dir.listdir()]

    config_file_path = os.path.join(str(test_dir), PROJECT_CONFIG_DIR)

    # Assert Luigi and Config Files are in directory
    config_files = [LUIGI_FILE, CFG_FILE]

    for config_file in config_files:
        assert config_file in os.listdir(config_file_path), "{} file not present".format(config_file)

    # Assert that instance can be found from simulated project
    DisdatConfig(project_root=str(test_dir))

    # Assert that instance can be found from subdirectory of simulated project
    os.mkdir(str(sub_dir))
    DisdatConfig(project_root=str(sub_dir))
