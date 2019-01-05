import os
import shutil
import ConfigParser

from disdat.common import error, PACKAGE_CONFIG_DIR, LUIGI_FILE, LOGGING_FILE, PROJECT_CONFIG_NAME
import resource


def init(directory=None):
    """
    Create a default configuration in the config directory. This makes a
    disdat folder which contains all configuration files.
    """

    # Use current directory if nothing is passed in
    if not directory:
        directory = os.getcwd()

    config_dir = os.path.join(directory, PROJECT_CONFIG_NAME)

    # Make sure disdat has not already been initialized
    if os.path.exists(config_dir):
        error(
            'DisDat already initialized in {}.'.format(config_dir)
        )

    # Copy configurations from disdat package to project
    src = resource.filename('disdat.config', PACKAGE_CONFIG_DIR)
    dst = config_dir
    shutil.copytree(src, dst)

    # Make sure paths are absolute in luigi config
    luigi_dir = os.path.join(config_dir, LUIGI_FILE)
    config = ConfigParser.ConfigParser()
    config.read(luigi_dir)
    logging_config_path = os.path.join(config_dir, LOGGING_FILE)
    config.set('core', 'logging_conf_file', logging_config_path)
    with open(luigi_dir, 'wb') as handle:
        config.write(handle)
