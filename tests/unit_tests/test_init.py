import pytest

import ConfigParser

from disdat.common import PROJECT_CONFIG_NAME, LUIGI_FILE, CFG_FILE, LOGGING_FILE
from disdat.init import init


# Test successful project specific config init
def test_successful_init(tmpdir):
    test_dir = tmpdir / 'sub'

    # Init with simulated project root
    init(directory=str(test_dir))

    # Assert directory exists
    assert PROJECT_CONFIG_NAME in [x.basename for x in test_dir.listdir()]

    project_config_path = test_dir / PROJECT_CONFIG_NAME

    # Assert all disdat config files are present
    config_files = [x.basename for x in project_config_path.listdir()]
    for config_file in [LUIGI_FILE, CFG_FILE, LOGGING_FILE]:
        assert config_file in config_files

    # Assert that the logging path in the luigi file points to the logging file
    logging_path = project_config_path / LOGGING_FILE
    luigi_path = project_config_path / LUIGI_FILE

    config = ConfigParser.ConfigParser()
    config.read(str(luigi_path))
    lp = config.get('core', 'logging_conf_file')
    assert logging_path == lp


# Test Failing init where disdat config directory already exists
def test_failed_init(tmpdir):
    test_dir = tmpdir.mkdir('sub')

    # Create project configs
    test_dir.mkdir(PROJECT_CONFIG_NAME)

    # Init with simulated project root
    with pytest.raises(SystemExit) as ex:
        init(directory=str(test_dir))

    # Assert Exited with error code of 1
    assert ex.type == SystemExit
    assert ex.value.code == 1
