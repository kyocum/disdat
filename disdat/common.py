#
# Copyright 2015, 2016, 2017  Human Longevity, Inc.
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
"""
Configuration
"""

import json
import logging
import os
import sys
import ConfigParser
import shutil
from disdat import resource
import disdat.config
import luigi

from urlparse import urlparse

_logger = logging.getLogger(__name__)

_logging_format_simple = '%(levelname)s : %(name)s : %(message)s'
_logging_format_fancy = '%(asctime)s : %(levelname)s : %(name)s : %(message)s'
_logging_default_level = logging.WARN


SYSTEM_CONFIG_DIR = '~/.config/disdat'
PACKAGE_CONFIG_DIR = 'disdat'
LOGGING_FILE = 'logging.conf'
LUIGI_FILE = 'luigi.cfg'
CFG_FILE = 'disdat.cfg'
META_DIR = '.disdat'
DISDAT_CONTEXT_DIR = 'context'  # ~/.disdat/context/<local_context_name>
DEFAULT_FRAME_NAME = 'unnamed'
BUNDLE_URI_SCHEME = 'bundle://'

PUT_LUIGI_PARAMS_IN_FUNC_PARAMS = False  # transparently place luigi parameters as kwargs in run() and requires()


def error(msg, *args, **kwargs):
    _logger.error(msg, *args, **kwargs)
    sys.exit(1)


def setup_default_logging():
    """
    Set up root logger so all inherited loggers get console for free.

    Args:
        logger:

    Returns:

    """
    logging.basicConfig(stream=sys.stderr, format=_logging_format_simple, level=_logging_default_level)


class SingletonType(type):
    def __call__(self, *args, **kwargs):
        try:
            return self.__instance
        except AttributeError:
            self.__instance = super(SingletonType, self).__call__(*args, **kwargs)
            return self.__instance


class MySingleton(object):
    __metaclass__ = SingletonType


class DisdatConfig(object):
    """
    Configure Disdat.  Configure logging.
    Note: This configures root logging with 'basicConfig', which should be idempotent.
    """

    _instance = None

    def __init__(self, *args, **kwargs):

        # Set up default logging to begin with. Can later be updated.
        setup_default_logging()

        # Find configuration directory
        config_dir = os.path.expanduser(SYSTEM_CONFIG_DIR)
        if not os.path.exists(config_dir):
            error(
                'Did not find configuration. '
                'Call "dsdt init" to initialize context.'
            )

        # Extract individual configuration files
        disdat_cfg = os.path.join(config_dir, CFG_FILE)
        luigi_cfg = os.path.join(config_dir, LUIGI_FILE)

        """ If running through pyinstaller, then use the current pythonpath to find the apply transforms"""
        if getattr(sys, 'frozen', False):
            # we are running in a bundle
            bundle_dir = sys._MEIPASS
            sys.path.extend(os.environ.get('PYTHONPATH', '').split(':'))
        else:
            # we are running in a normal Python environment
            bundle_dir = os.path.dirname(os.path.abspath(__file__))

        self.meta_dir_root = '~/'
        self.logging_config = None
        self.parser = self._read_configuration_file(disdat_cfg, luigi_cfg)

    @staticmethod
    def instance(*args, **kwargs):
        """
        Singleton getter
        """
        if DisdatConfig._instance is None:
            DisdatConfig._instance = DisdatConfig(*args, **kwargs)
        return DisdatConfig._instance

    @staticmethod
    def _fix_relative_path(config_file, to_fix_path):
        if not os.path.isabs(to_fix_path):
            return os.path.join(os.path.dirname(config_file), to_fix_path)
        return to_fix_path

    def _read_configuration_file(self, disdat_config_file, luigi_config_file):
        """
        Check for environment varialbe 'DISDAT_CONFIG_PATH' -- should point to disdat.cfg
        Paths in the config might be relative.  If so, add the prefix to them.
        Next, see if there is a disdat.cfg in cwd.  Then configure disdat and (re)configure logging.
        """

        _logger.debug("Loading config file [{}]".format(disdat_config_file))
        config = ConfigParser.SafeConfigParser({'meta_dir_root': self.meta_dir_root, 'ignore_code_version': 'False'})
        config.read(disdat_config_file)
        self.meta_dir_root = os.path.expanduser(config.get('core', 'meta_dir_root'))
        self.meta_dir_root = DisdatConfig._fix_relative_path(disdat_config_file, self.meta_dir_root)
        self.ignore_code_version = config.getboolean('core', 'ignore_code_version')

        try:
            self.logging_config = os.path.expanduser(config.get('core', 'logging_conf_file'))
            self.logging_config = DisdatConfig._fix_relative_path(disdat_config_file, self.logging_config)
            logging.config.fileConfig(self.logging_config, disable_existing_loggers=False)
        except ConfigParser.NoOptionError:
            pass

        # Set up luigi configuration
        luigi.configuration.get_config().read(luigi_config_file)

        # Tell everything to push warnings through the logging infrastructure
        logging.captureWarnings(True)

        # unfortunately that's not enough -- kill all luigi (and disdat) warnings
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

        # Make sure paths are absolute in luigi config
        luigi_dir = os.path.join(directory, LUIGI_FILE)
        config = ConfigParser.ConfigParser()
        config.read(luigi_dir)
        value = config.get('core', 'logging_conf_file')
        config.set('core', 'logging_conf_file', os.path.expanduser(value))
        with open(luigi_dir, 'wb') as handle:
            config.write(handle)


#
# Make Docker images names from pipeline class names
#

def make_pipeline_image_name(pipeline_class_name):
    """ Create the string for the image for this pipeline_class_name

    'disdat-module[-submodule]...'

    Args:
        pipeline_class_name:

    Returns:
        str: The name of the image 'disdat-module[-submodule]...'
    """

    return '-'.join(['disdat'] + pipeline_class_name.split('.')[:-1]).lower()


def make_pipeline_repository_name(docker_repository_prefix, pipeline_class_name):
    return '/'.join(([docker_repository_prefix.strip('/')] if docker_repository_prefix is not None else []) + [make_pipeline_image_name(pipeline_class_name)])


#
# Make run commands
#

def get_run_command_parameters(pfs):
    output_bundle_uuid = pfs.disdat_uuid()
    remote = pfs.get_curr_context().remote_ctxt_url
    if remote is None:
        raise ValueError
    remote = remote.replace('/{}'.format(DISDAT_CONTEXT_DIR), '')
    local_ctxt = "{}/{}".format(pfs.get_curr_context().remote_ctxt, pfs.get_curr_context().local_ctxt)
    return output_bundle_uuid, remote, local_ctxt


def make_run_command(input_bundle, output_bundle, output_bundle_uuid, remote, local_ctxt, input_tags,
                     output_tags, pipeline_params):
    return [x.strip() for x in [
        '--output-bundle-uuid ', output_bundle_uuid,
        '--remote', remote,
        '--branch', local_ctxt,
        '--input-tags', "'" + json.dumps(dict(input_tags)) + "'",
        '--output-tags', "'" + json.dumps(dict(output_tags)) + "'",
        input_bundle,
        output_bundle,
    ] + pipeline_params]


def parse_args_tags(args_tag):
    """ parse argument string of tags 'tag:value tag:value'
    into a dictionary.

    Args:
        args_tag (str): tags in string format 'tag:value tag:value'

    Returns:
        (dict):

    """

    if args_tag:
        tag_dict = {k: v for k, v in [kv[0].split(':') for kv in args_tag]}
    else:
        tag_dict = {}

    return tag_dict


def parse_params(params):
    """
    Input is the string "--arg value --arg2 value2"
    Convert to dict {'arg':value,'arg2':value2}

    :param params: from argparse
    :return:   dict {'arg':value,'arg2':value2}
    """

    return {k.lstrip('--'): v for k, v in zip(params[::2], params[1::2])}


def explode_json_column(df, column, prefix=None, inplace=False, drop=False):
    """
    Explode a JSON text column into discrete columns. If the texts are lists,
    create a column per element of the form "column.0", "column.1", etc.; the
    numbers will be zero-filled so that lexical sorting of the new column
    names preserves the order of the elements in the original lists. If the
    texts are objects, create a column per key of the form "column.key0", etc.
    Assumes that each row in the dataframe contains JSON texts of the same
    format: all arrays with the same number of elements, or all objects with
    the same keys.

    Note that this method will raise an exception if the column contains only
    JSON primitives.

    Args:
        df: The dataframe containing the JSON column
        column: The name of the JSON column
        inplace: If `True`, modify the dataframe in place, otherwise, create
            and modify a copy of the dataframe; by default, `False`.
        drop: If `True`, drop the original JSON column; by default, `False`

    Returns:
        A tuple containing the dataframe, and a list of the new columns, if
            any.

    """
    _logger.debug('Dataframe columns: %s' % (df.columns))
    if column not in df.columns:
        raise KeyError('Column %s not in the dataframe' % (column))

    # Check to see if it's already been decoded
    tp = type(df[column][0])
    if tp not in [unicode, str]:
        _logger.debug('Column %s already of type %s; unchanged' % (column, tp))
        return (df, [])

    # If no prefix supplied, use an empty string.
    if prefix is None:
        prefix = ''

    # Make a copy of the dataframe if necessary, then explode the
    if not inplace:
        df = df.copy()
    json_texts = df[column].apply(json.loads)
    if json_texts.shape[0] <= 0:
        _logger.warning('Column %s has no records' % (column))
        return (df, [])

    json_type = type(json_texts[0])
    columns_new = []
    if json_type == list:
        length = len(json_texts[0])
        _logger.debug('Column %s is a JSON list with %d elements' % (column, length))
        # The column extension template ensures that numeric column names are
        # zero-filled; otherwise, operations that lexically sort column names
        # will behave strangely (think '0 2 20 3' vs. '00 02 03 20').
        column_extension = '.%s{:0%dd}' % (prefix, len(str(length - 1)))
        for i in range(length):
            column_new = column + column_extension.format(i)
            columns_new += [column_new]
            df[column_new] = json_texts.apply((lambda x: x[i]))
    elif json_type == dict:
        keys = json_texts[0].keys()
        _logger.debug('Column %s is a JSON object with %d keys' % (column, len(keys)))
        for i in keys:
            column_new = '%s.%s%s' % (column, prefix, i)
            columns_new += [column_new]
            df[column_new] = json_texts.apply((lambda x: x[i]))
    else:
        raise ValueError('Column %s is a JSON primitive type')

    # Drop the original column
    if drop:
        df.drop(column, axis=1, inplace=True)
    _logger.debug('Created new columns: {}'.format(df.columns))

    return df, columns_new


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
    parsed_url = urlparse(url)
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
