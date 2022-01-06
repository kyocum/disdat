#!/usr/bin/env python

import argparse
import importlib as imp
import mock
import os
import setuptools


def find_packages(setup_py):
    packages = []
    # All this horrid hackery to recover the install_requires parameter from
    # a setup() call in setup.py.
    #
    # https://stackoverflow.com/questions/24236266/how-to-extract-dependencies-information-from-a-setup-py
    try:
        # Patch setuptools to intercept the setup() call
        with mock.patch.object(setuptools, 'setup') as setup_mock:
            # Get an open file handle and a description of the
            # setup file.
            setup_file, setup_filename, setup_description = imp.find_module('setup', [os.path.dirname(setup_py)])
            # Load setup.py as the module setup. We have to
            # intercept calls to find_packages as well since
            # find_packages will run a 'find'-like operation from
            # the current working directory - which is Bad if the
            # CWD is the root directory...
            with mock.patch.object(setuptools, 'find_packages'):
                imp.load_module('setup', setup_file, setup_filename, setup_description)
        # Grab the call args to setup
        _, setup_kwargs = setup_mock.call_args
        # ...and recover the install_requires parameter. Fun, eh?
        # Don't forget to remove trailing version specifiers that
        # lack version numbers.
        packages = ['{}'.format(p.rstrip('<>=')) for p in setup_kwargs['install_requires']]
    finally:
        # As warned in the docs, we have to close the setup file
        # ourselves.
        if setup_file is not None:
            setup_file.close()
    return packages


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Select Python packages from setup.py'
    )
    parser.add_argument(
        'setup_py',
        type=str,
        help='The setup.py file',
    )
    args = parser.parse_args()

    if not os.path.exists(args.setup_py):
        raise RuntimeError('Failed to find file {}'.format(args.setup_py))

    print ('\n'.join(find_packages(args.setup_py)))