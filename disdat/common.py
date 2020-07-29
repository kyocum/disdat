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

import logging
import os
import sys
import shutil
import importlib
import subprocess
import uuid

import luigi
from six.moves import urllib
from six.moves import configparser
import six

from disdat import resource
import disdat.config
from disdat import logger as _logger


SYSTEM_CONFIG_DIR = '~/.config/disdat'
PACKAGE_CONFIG_DIR = 'disdat'
LOGGING_FILE = 'logging.conf'
LUIGI_FILE = 'luigi.cfg'
CFG_FILE = 'disdat.cfg'
META_DIR = '.disdat'
DISDAT_CONTEXT_DIR = 'context'  # ~/.disdat/context/<local_context_name>
DEFAULT_FRAME_NAME = 'unnamed'
BUNDLE_URI_SCHEME = 'bundle://'

# Some tags in bundles are special.  They are prefixed with '__'
BUNDLE_TAG_PARAMS_PREFIX = '__param.'
BUNDLE_TAG_TRANSIENT = '__transient'
BUNDLE_TAG_PUSH_META = '__push_meta'

LOCAL_EXECUTION = 'LOCAL_EXECUTION'  # Docker endpoint env variable if we're running a container locally


class ApplyError(Exception):
    def __init__(self, message, apply_result):
        super(ApplyError, self).__init__(message)
        self.apply_result = apply_result
    @property
    def result(self):
        return self.apply_result


class ExtDepError(Exception):
    def __init__(self, message):
        super(ExtDepError, self).__init__(message)


class CatNoBundleError(Exception):
    def __init__(self, message):
        super(CatNoBundleError, self).__init__(message)


def error(msg, *args, **kwargs):
    _logger.error(msg, *args, **kwargs)
    sys.exit(1)


def apply_handle_result(apply_result, raise_not_exit=False):
    """ Execute an appropriate sys.exit() call based on the dictionary
    returned by apply.

    Args:
        apply_result(dict): Has keys 'success' and 'did_work' that give Boolean values.
        raise_not_exit (bool): Raise ApplyException instead of performing sys.exit

    Returns:
        None

    """

    if apply_result['success']:
        if raise_not_exit:
            pass
        else:
            sys.exit(None) # None yields exit value of 0
    else:
        error_str = "Disdat Apply ran, but one or more tasks failed."
        if raise_not_exit:
            raise ApplyError(error_str, apply_result)
        else:
            sys.exit(error_str)


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

    def __init__(self, meta_dir_root=None, config_dir=None):
        """

        Args:
            meta_dir_root (str): Optional place to store disdat contexts. Default `~/`
            config_dir (str): Optional directory from which to get disdat.cfg and luigi.cfg.  Default SYSTEM_CONFIG_DIR
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

        # Extract individual configuration files
        disdat_cfg = os.path.join(config_dir, CFG_FILE)
        luigi_cfg = os.path.join(config_dir, LUIGI_FILE)

        if meta_dir_root:
            self.meta_dir_root = meta_dir_root
        else:
            self.meta_dir_root = '~/'
        self.logging_config = None
        self.parser = self._read_configuration_file(disdat_cfg, luigi_cfg)

    @staticmethod
    def instance(meta_dir_root=None, config_dir=None):
        """
        Singleton getter

        Args:
            meta_dir_root (str): Optional place to store disdat contexts. Default `~/`
            config_dir (str): Optional directory from which to get disdat.cfg and luigi.cfg.  Default SYSTEM_CONFIG_DIR
        """
        if DisdatConfig._instance is None:
            DisdatConfig._instance = DisdatConfig(meta_dir_root=meta_dir_root, config_dir=config_dir)
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
        # _logger.debug("Loading config file [{}]".format(disdat_config_file))
        config = configparser.ConfigParser({'meta_dir_root': self.meta_dir_root, 'ignore_code_version': 'False'})
        config.read(disdat_config_file)
        self.meta_dir_root = os.path.expanduser(config.get('core', 'meta_dir_root'))
        self.meta_dir_root = DisdatConfig._fix_relative_path(disdat_config_file, self.meta_dir_root)
        self.ignore_code_version = config.getboolean('core', 'ignore_code_version')

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
        config = configparser.ConfigParser()
        config.read(luigi_dir)
        with open(luigi_dir, 'w') as handle:
            config.write(handle)

#
# subprocess wrapper
#


def do_subprocess(cmd, cli):
    """ Standardize error processing

    Args:
        cmd (str): command to execute
        cli (bool): whether called from CLI (True) or API (False)

    Returns:
        (int): 0 if success, >0 if failure

    """
    output = 'No captured output from running CMD [{}]'.format(cmd)
    try:
        if not cli:
            output = subprocess.check_output(cmd)
        else:
            subprocess.check_call(cmd)
    except subprocess.CalledProcessError as cpe:
        if not cli:
            print (output)
            return cpe.returncode
        raise

    return 0


def do_subprocess_with_output(cmd):
    """ Standardize error processing

    Args:
        cmd (str): command to execute

    Returns:
        (str): output of command

    """
    try:
        output = subprocess.check_output(cmd)
        return output
    except subprocess.CalledProcessError as cpe:
        raise

#
# One place to update all the uuid's made by Disdat
#

def create_uuid():
    """
    Note: We had been using uuid1, but found duplicate uuid's when using multiprocessing

    Returns:
        str: byte string of the 128bit UUID

    """
    return str(uuid.uuid4())


#
# Make Docker images names from pipeline class names
#


def make_project_image_name(setup_file_path):
    """
    Create a container name from the name field in the setup.py file.
    This uses some setuptools magic.  When you install disdat, we install
    an entrypoint.   It becomes available to anyone using setup tools.
    This extracts the information from the setup.py.

    see disdat/infrastructure/dockerizer/setup_tools_commands.py

    Args:
        setup_file_path (str): The FQP to the setup.py file used to dockerize.

    Returns:
        (str): image name string

    """

    python_command = [
        'python',
        setup_file_path,
        '-q',
        'dsdt_distname'
    ]

    retval = do_subprocess_with_output(python_command).strip()

    # If P3, this may be a byte array.   If P2, if not unicode, convert ...
    retval = six.ensure_str(retval)

    return retval


def make_sagemaker_project_image_name(setup_file_path):
    """ Create the string for the image for this pipeline if it uses sagemaker's
    calling convention

    Args:
        setup_file_path (str): The FQP to the setup.py file used to dockerize

    Returns:
        str: The name of the image + '-sagemaker'
    """

    return make_project_image_name(setup_file_path) + "-sagemaker"


def make_project_repository_name(docker_repository_prefix, setup_file_path):
    return '/'.join(([docker_repository_prefix.strip('/')] if docker_repository_prefix is not None else []) + [make_project_image_name(setup_file_path)])


def make_sagemaker_project_repository_name(docker_repository_prefix, setup_file_path):
    return '/'.join(([docker_repository_prefix.strip('/')] if docker_repository_prefix is not None else []) + [make_sagemaker_project_image_name(setup_file_path)])


#
# Make run commands
#

def get_run_command_parameters(pfs):
    remote = pfs.curr_context.remote_ctxt_url
    if remote is not None:
        remote = remote.replace('/{}'.format(DISDAT_CONTEXT_DIR), '')
        local_ctxt = "{}/{}".format(pfs.curr_context.remote_ctxt, pfs.curr_context.local_ctxt)
    else:
        local_ctxt = "{}".format(pfs.curr_context.local_ctxt)
    return remote, local_ctxt


def make_run_command(
        output_bundle,
        output_bundle_uuid,
        pipe_cls,
        remote,
        context,
        input_tags,
        output_tags,
        force,
        force_all,
        no_pull,
        no_push,
        no_push_int,
        workers,
        pipeline_params
):
    """ Create a list of args.  Note that for execution via run, we always set
    --output-bundle, even though it is optional.   The CLI and API will place a '-'
    if the user does not specify it, which means use the default output bundle name.  Here
    we make sure to pass it through.

    Args:
        output_bundle:
        output_bundle_uuid:
        pipe_cls:
        remote:
        context:
        input_tags:
        output_tags:
        force:
        force_all:
        no_pull:
        no_push:
        no_push_int:
        workers:
        pipeline_params:

    Returns:

    """
    args = [
        '--output-bundle-uuid ', output_bundle_uuid,
        '--output-bundle', output_bundle,
        '--branch', context,
        '--workers', str(workers)
    ]
    if remote:
        args.extend(['--remote', remote])
    if no_pull:
        args += ['--no-pull']
    if no_push:
        args += ['--no-push']
    if no_push_int:
        args += ['--no-push-intermediates']
    if force:
        args += ['--force']
    if force_all:
        args += ['--force-all']
    if len(input_tags) > 0:
        for next_tag in input_tags:
            args += ['--input-tag', next_tag]
    if len(output_tags) > 0:
        for next_tag in output_tags:
            args += ['--output-tag', next_tag]

    args += [str(pipe_cls)]  # The one required argument to the entrypoint

    return [x.strip() for x in args + pipeline_params]


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


def parse_params(cls, params):
    """
    Create a dictionary of str->str arguments to str->python objects deser'd by Luigi Parameters

    Input is the string "--arg value --arg2 value2"

    Convert to dict {'arg':str,'arg2':str2}

    then

    Convert to dict {'arg':luigi.Parameter.value,'arg2':luigi.Parameter.value2}

    Args:
        cls (type[disdat.pipe.PipeTask]):
        params: from argparse

    Returns:
         dict {'arg':value,'arg2':value2}
    """

    params_str_dict = {k.lstrip('--'): v for k, v in zip(params[::2], params[1::2])}

    return convert_str_params(cls, params_str_dict)


def convert_str_params(cls, params_str):
    """
    This is similar to Luigi.Task.from_str_params(cls, params_str)
    But we don't create the class here, and we outer loop through our params (not the classes
    params).  We just want to convert each of the params that are in the class and in this dictionary
    into the deserialized form.

    NOTE:  This is somewhat dangerous and could break if Luigi changes around
    this code.  The alternative is to use Luigi.load_task() but then we have to ensure
    all the input parameters are "strings" and we have to then put special code
    inside of apply to know when to create a class normally, or create it from the CLI.

    Parameters:
        params_str (dict): dict of str->str.  param name -> value .
    """
    kwargs = {}

    cls_params = {n: p for n, p in cls.get_params()}  # get_params() returns [ (name, param), ... ]

    for param_name, param_str in params_str.items():
        if param_name in cls_params:
            param = cls_params[param_name]
            if isinstance(param_str, list):
                kwargs[param_name] = param._parse_list(param_str)
            else:
                kwargs[param_name] = param.parse(param_str)
        else:
            _logger.error("Parameter {} is not defined in class {}.".format(param_name, cls.__name__))
            raise ValueError("Parameter {} is not defined in class {}.".format(param_name, cls.__name__))

    return kwargs


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


def setup_exists(fqp_setup):
    """ Check if file exists
    """
    if not os.path.exists(fqp_setup):
        print ("No setup.py found at {}.".format(fqp_setup))
        return False
    return True


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
