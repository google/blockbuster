# coding=utf-8
# Copyright 2022 Google LLC..
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

"""Contains all the common constants related to DAGs.

Any constant value which needs to be shared between DAGs or SubDAGs should be
added here.
"""

import enum


# AIRFLOW COMMON GLOBAL VARIABLES & CONSTANTS
# ------------------------------------------------------------------------------
# These variable key are set in Airflow Web UI and used by dags/subdags in this
# module.

# Airflow envirinment constant variable.
BLOCKBUSTER_GLOBAL_CONFIG = 'bb_project'
BLOCKBUSTER_PREPROCESS_CONFIG = 'bb_preprocess'
BLOCKBUSTER_PREDICTION_ACTIVATION_CONFIG = 'bb_prediction_activation'
BLOCKBUSTER_STORAGE_CONFIG = 'bb_storage'
BLOCKBUSTER_FEATURE_CONFIG = 'bb_features'
BLOCKBUSTER_PREDICTION_RECENT_MODEL = 'recent_model'
BLOCKBUSTER_TRAINING_CONFIG = 'bb_training'
BLOCKBUSTER_ACTIVATION_CONFIG = 'bb_prediction_activation'
BLOCKBUSTER_RECENT_MODEL_VAL = 'recent_model'
BLOCKBUSTER_RECENT_DATASET_VAL = 'recent_dataset'


# Airflow default config values.
DEFAULT_START_DAYS_AGO = 1
DEFAULT_DAG_RETRY = 1
DEFAULT_DAG_RETRY_DELAY_MINS = 5


class PreprocessingType(enum.Enum):
  """Indicates the type of activity the preprocessing is used for."""
  TRAINING = 'TRAINING'
  PREDICTION = 'PREDICTION'
