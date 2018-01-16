#!/usr/bin/env python

# This Python script must be DEAD SIMPLE - it can *only* use standard
# Python modules. We relax this rule if you want to install from a setup.py
# file.

import argparse
import os
import re
import subprocess
import sys
import tempfile

from find_packages_from_setup import find_packages

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Select conda-provided Python packages from a pip-style requirements file'
    )
    parser.add_argument(
        'requirements',
        type=str,
        help='The requirements file',
    )
    parser.add_argument(
        '--no-non-conda',
        action='store_false',
        help='Do not output non-conda-provided packages (default is to output non-conda-provided packages on stderr)',
        dest='non_conda',
    )
    args = parser.parse_args()

    if not os.path.exists(args.requirements):
        raise RuntimeError('Failed to find file {}'.format(args.requirements))

    with tempfile.NamedTemporaryFile() as sanitized_file:
        # Create a sanitized requirements file that does not contain https
        # or -e pseudo-package references. Because Anaconda hates pip.
        packages = []
        if os.path.basename(args.requirements) == 'setup.py':
            packages = find_packages(args.requirements)
        else:
            with open(args.requirements, 'r') as requirements_file:
                packages = [p.rstrip() for p in requirements_file if not (p.startswith('git+') or p.startswith('http') or p.startswith('-e'))]
        sanitized_file.write('\n'.join(packages))
        sanitized_file.seek(0)
        # Use the conda installer to figure out which packages Continuum does
        # and does not host.
        non_conda_packages_p = subprocess.Popen(
            ['conda', 'install', '--dry-run', '--file', sanitized_file.name],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        # The installer lists the non-conda packages in stderr in the following
        # format:
        #   - package_name [version constraint operators (<>=)] version_num
        #
        # Oh yeah, and Python package names are case-insensitive.
        non_conda_packages_raw = non_conda_packages_p.communicate()[1]
        non_conda_packages_p.wait()
        non_conda_packages = []
        if non_conda_packages_p.returncode != 0:
            non_conda_packages = [p.lstrip(' -').split()[0].lower() for p in non_conda_packages_raw.split('\n') if re.match(r'^\s*-', p)]
        # Drop the version constraint, and filter out packages not provided by
        # Continuum.
        sanitized_file.seek(0)
        if len(non_conda_packages) > 0:
            non_conda_packages_re = r'^({})[<>=]*'.format('|'.join(non_conda_packages))
            conda_packages = [p.rstrip() for p in sanitized_file if not re.match(non_conda_packages_re, p.lower())]
        else:
            conda_packages = [p.rstrip() for p in sanitized_file]
        # Print the filtered requirements in a format usable with the conda
        # installer.
        print '\n'.join(conda_packages)
        if args.non_conda:
            print >> sys.stderr, '\n'.join(non_conda_packages)
