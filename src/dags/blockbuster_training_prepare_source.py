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

"""Airflow Training Dag to prepare source."""

import datetime

import json
import os
from typing import Union, Any, Dict

import airflow
from airflow import models
from airflow.operators import subdag_operator

from dependencies.utils import airflow_utils
from dependencies import blockbuster_constants
from subdags import prepare_source_dag as prepare_source

# Declare Constants
_AIRFLOW_ENV = 'AIRFLOW_HOME'
_DAG_ID = '0_BB_Prepare_Source'


def create_prepare_source_subdag(
    main_dag: models.DAG, args: Dict[str, Union[Dict[str, Any],
                                                datetime.datetime]]
) -> subdag_operator.SubDagOperator:
  """Creates source preparing pipeline subdag.

  Args:
    main_dag: DAG to add this operator to.
    args : Dict type DAG arguments.

  Returns:
    SubdagOperator to use within a DAG to create source for training.
  """

  prepare_source_subdag = prepare_source.create_dag(
      args, blockbuster_constants.PreprocessingType.TRAINING, _DAG_ID)

  return subdag_operator.SubDagOperator(
      task_id=prepare_source.DAG_NAME,
      subdag=prepare_source_subdag,
      dag=main_dag)


def create_training_prepare_source_dag() -> models.DAG:
  """Creates the pipeline for preparing source for training the model.

  Returns:
    Parent training DAG.
  """
  bb_project_vars = json.loads(models.Variable.get('bb_project'))
  bb_storage_vars = json.loads(models.Variable.get('bb_storage'))
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
      _DAG_ID, None, blockbuster_constants.DEFAULT_DAG_RETRY,
      blockbuster_constants.DEFAULT_DAG_RETRY_DELAY_MINS,
      blockbuster_constants.DEFAULT_START_DAYS_AGO, **args)

  create_prepare_source_subdag(main_dag, args)
  return main_dag

if os.getenv(_AIRFLOW_ENV):
  dag = create_training_prepare_source_dag()
