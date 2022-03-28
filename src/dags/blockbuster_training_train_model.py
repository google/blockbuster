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

"""Main Airflow Training Dag to load a training dataset and create and train a new model.

Will be executed in cloud composer in AIRFLOW_HOME. To prevent unwanted dag
initialisation, there is a check on the AIRFLOW_HOME environment variable to
check if its running inside the Airflow environment.
"""
import datetime
import os
from typing import Union, Any, Dict

import airflow
from airflow import models
from airflow.operators import subdag_operator
from airflow.utils import helpers

from dependencies.utils import airflow_utils
from dependencies import blockbuster_constants
from subdags import load_data_dag
from subdags import train_model_dag

# Contains a name of the shell environment variable which indicates that dag is
# run by Airflow and should be fully initialized.
_AIRFLOW_ENV = 'AIRFLOW_HOME'
_DAG_ID = '3_BB_Data_load_and_train'
_TRAIN_MODEL_TASK_NAME = 'train_model'
_LOAD_DATA_TASK_NAME = 'load_data'
_AirflowDagArgs = Dict[str, Union[Dict[str, Any], datetime.datetime]]


def create_train_model_dag() -> models.DAG:
  """Creates the main dag for train model main dag.

  Returns:
    Parent training DAG.
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
  load_data_subdag = subdag_operator.SubDagOperator(
      task_id=_LOAD_DATA_TASK_NAME,
      subdag=load_data_dag.create_dag(args, _DAG_ID),
      dag=main_dag)
  train_model_subdag = subdag_operator.SubDagOperator(
      task_id=_TRAIN_MODEL_TASK_NAME,
      subdag=train_model_dag.create_dag(args, _DAG_ID),
      dag=main_dag)

  helpers.chain(load_data_subdag, train_model_subdag)
  return main_dag


if os.getenv(_AIRFLOW_ENV):
  dag = create_train_model_dag()
