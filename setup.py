#
# Copyright 2015, 2016  Human Longevity, Inc.
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
from setuptools import setup, find_packages
import os


def find_version():
    here = os.path.abspath(os.path.dirname(__file__))
    with open(os.path.join(here, 'disdat','VERSION')) as version_file:
        version = version_file.read().strip()
    return version

setup(
    name='disdat',
    version=find_version(),
    description='DisDat: versioned data science',
    author='Ken Yocum',
    author_email='kyocum@gmail.com',
    url='https://github.com/kyocum/disdat',

    # Choose your license
    license='Apache License, version 2.0',

    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 4 - Beta',

        # Indicate who your project is intended for
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering',

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: Apache Software License',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.6',
        'Operating System :: OS Independent',
        'Natural Language :: English',
    ],

    # You can just specify the packages manually here if your project is
    # simple. Or you can use find_packages().
    packages=find_packages(exclude=['tests*',
                                    'infrastructure.tests*']),

    # Include non-python files found in each package in the install, if in your MANIFEST.in
    include_package_data=True,

    # If any package contains other resource files, include them here.
    # We copy config/disdat so 'dsdt init' still runs from an installed
    # .egg.  This package_data only works if you're not using "sdist"
    # Otherwise only MANIFEST.in actually works, and then only if include_package_data=True
    package_data={
        '': ['*.json'],
        'disdat': [
            'config/disdat/*',
            'VERSION',
        ],
        'infrastructure': [
            'Dockerfiles/hyperframe_def/*'
            'dockerizer/Makefile',
            'dockerizer/Dockerfiles/*',
            'dockerizer/bin/*.py',
            'dockerizer/kickstart/bin/*',
            'dockerizer/kickstart/etc/*',
        ],
    },

    exclude_package_data={
        'disdat': [
            'dockerizer/kickstart/bin/*.pyc',
        ]
    },

    data_files=[('', ['setup.py'])],

    # List run-time dependencies here.  These will be installed by pip when
    # your project is installed.  If >=, means it worked with the base version.
    # If <= means higher versions broke something.

    install_requires=[
        'luigi>=2.8.11,<3.0',
        'boto3>=1.13.10,<2.0',
        'termcolor>=1.1.0,<2.0',
        'docker>=4.1.0,<4.4.0',
        'pandas>=0.25.3,<=1.2.0',
        'numpy>=1.18.1,<1.19',
        'sqlalchemy>=1.3.13,<1.4',
        'protobuf>=3.11.2,<4.0',
        'six>=1.13.0,<=1.15',
        'docutils<0.16,>=0.10'
    ],

    # List additional groups of dependencies here (e.g. development
    # dependencies). You can install these using the following syntax,
    # for example:
    # $ pip install -e '.[dev, rel]'
    extras_require={
        'examples': [
            'spacy',
            'tensorflow',
        ],
        'dev': [
            'pytest',
            'ipython',
            'mock',
            'nose',
            'pylint',
            'coverage',
            'tox',
            'moto'
        ],
        'rel': [
            'wheel',
            'sphinx',
            'sphinx_rtd_theme'
        ]
    },

    entry_points={
        'console_scripts': [
            'dsdt = disdat.dsdt:main',
        ],
        'distutils.commands': [
            "dsdt_distname = disdat.infrastructure.dockerizer.setup_tools_commands:DistributionName",
        ]
    },
)
