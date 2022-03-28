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

"""Main Airflow Training Dag to prepare source.Will be executed in cloud composer."""
import datetime
import os
from typing import Union, Any, Dict

import airflow
from airflow import models
from airflow.operators import subdag_operator

from dependencies.utils import airflow_utils
from dependencies import blockbuster_constants
from subdags import preprocess_dag

_AIRFLOW_ENV = 'AIRFLOW_HOME'
_DAG_ID = '2_BB_Preprocess'
_TASK_NAME = 'preprocess'
_ComplexDict = Dict[str, Union[Dict[str, Any], datetime.datetime]]


def create_preprocess_subdag(
    main_dag: models.DAG, args: _ComplexDict
) -> subdag_operator.SubDagOperator:
  """Creates preprocess pipeline subdag.

  Args:
    main_dag: DAG to add this operator to.
    args : Dict type DAG arguments.

  Returns:
    SubdagOperator to use within a DAG to create source for training.
  """
  preprocess_subdag = preprocess_dag.create_dag(
      args, blockbuster_constants.PreprocessingType.TRAINING, _DAG_ID)

  return subdag_operator.SubDagOperator(
      task_id=_TASK_NAME,
      subdag=preprocess_subdag,
      dag=main_dag)


def create_training_preprocess_dag():
  """Creates the main dag for preprocess main dag.

  Returns:
    Parent training DAG for preprocessing.
  """
  bb_storage_vars = airflow_utils.retrieve_airflow_variable_as_dict(
      blockbuster_constants.BLOCKBUSTER_STORAGE_CONFIG)
  bb_project_vars = airflow_utils.retrieve_airflow_variable_as_dict(
      blockbuster_constants.BLOCKBUSTER_GLOBAL_CONFIG)
  args = {
      'start_date': airflow.utils.dates.days_ago(1),
      'dataflow_default_options': {
          'project': bb_project_vars['gcp_project_id'],
          'region': bb_project_vars['gcp_region'],
          'zone': bb_project_vars['gcp_zone'],
          'tempLocation': bb_storage_vars['gcs_temp_path']
      },
  }

  main_dag = airflow_utils.initialize_airflow_dag(
      dag_id=_DAG_ID,
      schedule=None,
      retries=blockbuster_constants.DEFAULT_DAG_RETRY,
      retry_delay=blockbuster_constants.DEFAULT_DAG_RETRY_DELAY_MINS,
      start_days_ago=blockbuster_constants.DEFAULT_START_DAYS_AGO,
      **args)

  create_preprocess_subdag(main_dag, args)

  return main_dag

if os.getenv(_AIRFLOW_ENV):
  dag = create_training_preprocess_dag()
