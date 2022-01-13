#
# Copyright Human Longevity, Inc.
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
Resource
========
Package resource manager with the same basic API as pkg_resources.

Advantages:
* Loads much faster. pkg_resources takes 200ms to load, this takes 0ms to load.
* Allows you to pass in that actual loaded module instead of using a string.
  This allows for better refactoring.

"""

import pkgutil
import sys
import os
import types


def filename(package, resource):

    if isinstance(package, types.ModuleType):
        mod = package
    else:
        loader = pkgutil.get_loader(package)
        if loader is None or not hasattr(loader, 'get_data'):
            return None
        mod = sys.modules.get(package) or loader.load_module(package)
        if mod is None or not hasattr(mod, '__file__'):
            return None

    parts = resource.split('/')
    parts.insert(0, os.path.dirname(mod.__file__))
    return os.path.join(*parts)


def exists(package, resource):
    return os.path.exists(filename(package, resource))


def stream(package, resource):
    return open(filename(package, resource), 'rb')


def isdir(package, resource):
    os.path.isdir(filename(package, resource))


def listdir(package, resource):
    os.listdir(filename(package, resource))


def string(package, resource):
    with open(filename(package, resource), 'rb') as handle:
        return handle.read()
