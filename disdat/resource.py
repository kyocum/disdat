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

import importlib.util
import os
import sys
import types


def filename(package, resource):

    if isinstance(package, types.ModuleType):
        if not getattr(package, "__file__", None):
            return None
        base = os.path.dirname(package.__file__)
    else:
        # Resolve a package given by name. pkgutil.get_loader/load_module were
        # removed in Python 3.12; use importlib instead and avoid importing the
        # module when it is already loaded.
        mod = sys.modules.get(package)
        if mod is not None and getattr(mod, "__file__", None):
            base = os.path.dirname(mod.__file__)
        else:
            spec = importlib.util.find_spec(package)
            if spec is None or spec.origin is None:
                return None
            base = os.path.dirname(spec.origin)

    parts = resource.split("/")
    parts.insert(0, base)
    return os.path.join(*parts)


def exists(package, resource):
    return os.path.exists(filename(package, resource))


def stream(package, resource):
    return open(filename(package, resource), "rb")


def isdir(package, resource):
    os.path.isdir(filename(package, resource))


def listdir(package, resource):
    os.listdir(filename(package, resource))


def string(package, resource):
    with open(filename(package, resource), "rb") as handle:
        return handle.read()
