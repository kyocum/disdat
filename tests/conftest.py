#
# Copyright 2024 Disdat
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
import os

import pytest

import disdat.api as api
from disdat.common import SYSTEM_CONFIG_DIR, DisdatConfig


def _is_test_context(name):
    """A context the suite creates: leading underscores followed by 'test'
    (e.g. ``__test_context_1__``, ``___test_context___``). Real user contexts
    do not match, so they are never touched."""
    return name.startswith("_") and name.lstrip("_").startswith("test")


@pytest.fixture(scope="session", autouse=True)
def ensure_disdat_initialized():
    """Ensure a Disdat configuration exists before the suite runs.

    The tests assume an initialized Disdat (normally created once via
    ``dsdt init``). A fresh checkout or CI runner has none, so create it when
    absent. Guarded on existence because ``DisdatConfig.init()`` exits the
    process if the config directory already exists, so an existing developer
    configuration is left untouched.
    """
    if not os.path.exists(os.path.expanduser(SYSTEM_CONFIG_DIR)):
        DisdatConfig.init()
    yield


def pytest_sessionfinish(session, exitstatus):
    """Remove leftover test contexts when the run fails.

    Tests clean up their own contexts on success, but a test that fails partway
    leaves ``__test*`` contexts behind, which then pollute the next run (stale
    bundles -> spurious failures). On a non-zero exit, delete those contexts so
    a subsequent run starts clean. Best-effort: never raise from teardown.
    """
    if exitstatus == 0:
        return
    try:
        for name in list(api.ls_contexts()):
            if _is_test_context(name):
                try:
                    api.delete_context(context_name=name)
                except Exception:
                    pass
    except Exception:
        pass
