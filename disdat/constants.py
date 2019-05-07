import enum
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
"""
DisDat constants

"""

_MANAGED_OBJECTS = "objects"   # directory in the context for objects

# Columns in our CSV and DF's
JSON_DATA = 'json_data'
DATA = 'data'
FILE = 'files'


# Config Defaults
class Config(enum.Enum):
    # Docker
    OS_TYPE = 'python'
    OS_VERSION = '2.7.15 - slim'

    # General AWS
    AWS_PROFILE_NAME = 'default'

    # Batch
    DISDAT_BATCH_QUEUE = 'disdat_batch_queue'

    # Sagemaker
    AWS_SAGEMAKER_INSTANCE_TYPE = 'ml.m4.xlarge'
    AWS_SAGEMAKER_INSTANCE_COUNT = 1
    AWS_SAGEMAKER_VOLUME_SIZEGB = 128
    AWS_SAGEMAKER_MAX_RUNTIME_SEC = 300
