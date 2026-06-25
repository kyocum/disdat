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

from disdat.common import SYSTEM_CONFIG_DIR, DisdatConfig


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
