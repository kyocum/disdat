#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import logging
import os
import sys
import shutil
import uuid
import importlib

from six.moves import urllib
from six.moves import configparser

from disdat import resource
import disdat.config
from disdat import logger as _logger


SYSTEM_CONFIG_DIR = '~/.config/disdat'
PACKAGE_CONFIG_DIR = 'disdat'
LOGGING_FILE = 'logging.conf'
CFG_FILE = 'disdat.cfg'
META_DIR = '.disdat'
DISDAT_CONTEXT_DIR = 'context'  # ~/.disdat/context/<local_context_name>
DEFAULT_FRAME_NAME = 'unnamed'
BUNDLE_URI_SCHEME = 'bundle://'

# Some tags in bundles are special.  They are prefixed with '__'
BUNDLE_TAG_PARAMS_PREFIX = '__param.'
BUNDLE_TAG_TRANSIENT = '__transient'
BUNDLE_TAG_PUSH_META = '__push_meta'


class CatNoBundleError(Exception):
    def __init__(self, message):
        super(CatNoBundleError, self).__init__(message)


class BadLinkError(Exception):
    def __init__(self, message):
        super(BadLinkError, self).__init__(message)


def error(msg, *args, **kwargs):
    _logger.error(msg, *args, **kwargs)
    sys.exit(1)


class SingletonType(type):
    def __call__(self, *args, **kwargs):
        try:
            return self.__instance
        except AttributeError:
            self.__instance = super(SingletonType, self).__call__(*args, **kwargs)
            return self.__instance


class DisdatConfig(object):
    """
    Configure Disdat.  Configure logging.
    Note: This configures root logging with 'basicConfig', which should be idempotent.
    """

    _instance = None

    def __init__(self, meta_dir_root=None, config_dir=None):
        """

        Args:
            meta_dir_root (str): Optional place to store disdat contexts. Default `~/`
            config_dir (str): Optional directory from which to get disdat.cfg and (optionally) luigi.cfg.  Default SYSTEM_CONFIG_DIR
        """

        # Find configuration directory
        if config_dir:
            config_dir = config_dir
        else:
            config_dir = os.path.expanduser(SYSTEM_CONFIG_DIR)

        if not os.path.exists(config_dir):
            error(
                'Did not find Disdat configuration. '
                'Call "dsdt init" to initialize Disdat.'
            )

        disdat_cfg = os.path.join(config_dir, CFG_FILE)

        if meta_dir_root:
            self.meta_dir_root = meta_dir_root
        else:
            self.meta_dir_root = '~/'
        self.logging_config = None
        self.parser = self._read_configuration_file(disdat_cfg)

    @staticmethod
    def instance(meta_dir_root=None, config_dir=None):
        """
        Singleton getter

        Args:
            meta_dir_root (str): Optional place to store disdat contexts. Default `~/`
            config_dir (str): Optional directory from which to get disdat.cfg and (optional) luigi.cfg.  Default SYSTEM_CONFIG_DIR
        """
        if DisdatConfig._instance is None:
            DisdatConfig._instance = DisdatConfig(meta_dir_root=meta_dir_root, config_dir=config_dir)
        return DisdatConfig._instance

    @staticmethod
    def _fix_relative_path(config_file, to_fix_path):
        if not os.path.isabs(to_fix_path):
            return os.path.join(os.path.dirname(config_file), to_fix_path)
        return to_fix_path

    def _read_configuration_file(self, disdat_config_file):
        """
        Check for environment variable 'DISDAT_CONFIG_PATH' -- should point to disdat.cfg
        Paths in the config might be relative.  If so, add the prefix to them.
        Next, see if there is a disdat.cfg in cwd.  Then configure disdat and (re)configure logging.
        """
        # _logger.debug("Loading config file [{}]".format(disdat_config_file))
        config = configparser.ConfigParser({'meta_dir_root': self.meta_dir_root, 'ignore_code_version': 'False'})
        config.read(disdat_config_file)
        self.meta_dir_root = os.path.expanduser(config.get('core', 'meta_dir_root'))
        self.meta_dir_root = DisdatConfig._fix_relative_path(disdat_config_file, self.meta_dir_root)
        self.ignore_code_version = config.getboolean('core', 'ignore_code_version')

        # Tell everything to push warnings through the logging infrastructure
        logging.captureWarnings(True)

        # unfortunately that's not enough -- kill all warnings
        import warnings
        warnings.filterwarnings("ignore")

        meta_dir = os.path.join(self.meta_dir_root, META_DIR)
        if not os.path.exists(meta_dir):
            os.makedirs(meta_dir)

        return config

    def get_meta_dir(self):
        return os.path.join(self.meta_dir_root, META_DIR)

    def get_context_dir(self):
        return os.path.join(self.get_meta_dir(), DISDAT_CONTEXT_DIR)

    @staticmethod
    def init():
        """
        Create a default configuration in the config directory. This makes a
        disdat folder which contains all configuration files.
        """
        directory = os.path.expanduser(SYSTEM_CONFIG_DIR)

        # Make sure disdat has not already been initialized
        if os.path.exists(directory):
            error(
                'DisDat already initialized in {}.'.format(directory)
            )

        # Create outer folder if the system does not have it yet
        path = os.path.dirname(directory)
        if not os.path.exists(path):
            os.mkdir(path)

        # Copy over default configurations
        src = resource.filename(disdat.config, PACKAGE_CONFIG_DIR)
        dst = directory
        shutil.copytree(src, dst)


def create_uuid():
    """
    Note: We had been using uuid1, but found duplicate uuid's when using multiprocessing

    Returns:
        str: byte string of the 128bit UUID

    """
    return str(uuid.uuid4())


def parse_args_tags(args_tag, to='dict'):
    """ parse argument string of tags 'tag:value tag:value'
    into a dictionary.

    Args:
        args_tag (str): tags in string format 'tag:value tag:value'
        to (str): Make a 'list' or 'dict' (default)
    Returns:
        (list(str) or dict(str)):
    """

    if to == 'list':
        tag_thing = []
    else:
        tag_thing = {}

    if args_tag:
        if to == 'list':
            tag_thing = ['{}'.format(kv[0]) for kv in args_tag]
        if to == 'dict':
            tag_thing = {k: v for k, v in [kv[0].split(':') for kv in args_tag]}

    return tag_thing


def get_local_file_path(url):
    """
    Get a local file path from a file:// URL.

    Args:
        url: A `file` scheme URL or an empty (default) scheme URL

    Returns:
        A path into the local file system

    Raises:
        TypeError if the URL is not a file URL
    """
    parsed_url = urllib.parse.urlparse(url)
    if parsed_url.scheme != 'file' and parsed_url.scheme != '':
        raise TypeError('Expected file scheme in URL, got {}'.format(parsed_url.scheme))
    return parsed_url.path


def slicezip(a, b):
    """
    Fast method of zip + flatten with same-sized lists.
    Args:
        a: list one
        b: list two

    Returns:
        [ a[0], b[0], ..., a[n], b[n] ]
    """
    result = [0]*(len(a)+len(b))
    result[::2] = a
    result[1::2] = b
    return result


def load_class(class_path):
    """
    Given a fully-qualified [pkg.mod.sub.classname] class name,
    load the specified class and return a reference to it.

    Args:
        class_path (str): '.' separated module and classname

    Returns:
        class: reference to the loaded class
    """
    try:
        mod_path, cls_name = class_path.rsplit('.', 1)
    except ValueError:
        raise ValueError('must include fully specified classpath, not local reference')

    mod = importlib.import_module(mod_path)
    cls = getattr(mod, cls_name)
    return cls
