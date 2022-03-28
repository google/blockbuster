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

"""Airflow Training Dag to run Batch Predictions Pipeline.

Works in composer within Airflow Home environment.

When this Parent Dag is initialized within AIRFLOW_HOME,it will create 5
subdags.
"""
import datetime
import os
from typing import Union, Any, Dict

import airflow
from airflow import models
from airflow.operators import subdag_operator

from dependencies.utils import airflow_utils
from dependencies import blockbuster_constants
from subdags import activate_ga_dag
from subdags import analyze_dag
from subdags import batch_predict_dag
from subdags import cleanup_storage_dag
from subdags import prepare_source_dag
from subdags import preprocess_dag


# Declare name of the shell environment variable in which dag will execute.
_AIRFLOW_ENV = 'AIRFLOW_HOME'
_DAG_ID = '4_BB_Predict_and_activate'
_AirflowDagArgs = Dict[str, Union[Dict[str, Any], datetime.datetime]]


def create_prepare_source_task(main_dag: models.DAG, args: _AirflowDagArgs,
                               task_id: str) -> subdag_operator.SubDagOperator:
  """Creates prepare_source subdag for the batch_predictions pipeline.

  Args:
    main_dag: DAG to add this operator to.
    args: Dict type DAG arguments.
    task_id: Task id for the subdag.

  Returns:
    SubdagOperator to use within a DAG to create source for batch predictions.
  """
  prepare_source_subdag = prepare_source_dag.create_dag(
      args, blockbuster_constants.PreprocessingType.PREDICTION, _DAG_ID)

  return subdag_operator.SubDagOperator(
      task_id=task_id, subdag=prepare_source_subdag, dag=main_dag)


def create_analyze_task(main_dag: models.DAG, args: _AirflowDagArgs,
                        task_id: str) -> subdag_operator.SubDagOperator:
  """Creates analyze subdag for the batch_predictions pipeline.

  Args:
    main_dag: DAG to add this operator to.
    args: Dict type DAG arguments.
    task_id: Task id for the subdag.

  Returns:
    SubdagOperator to use within a DAG to analyze.
  """
  analyze_subdag = analyze_dag.create_dag(
      args, blockbuster_constants.PreprocessingType.PREDICTION, _DAG_ID)

  return subdag_operator.SubDagOperator(
      task_id=task_id, subdag=analyze_subdag, dag=main_dag)


def create_preprocess_task(main_dag: models.DAG, args: _AirflowDagArgs,
                           task_id: str) -> subdag_operator.SubDagOperator:
  """Creates preprocess subdag for the batch_predictions pipeline.

  Args:
    main_dag: DAG to add this operator to.
    args: Dict type DAG arguments.
    task_id: Task id for the subdag.

  Returns:
    SubdagOperator to use within a DAG to preprocess.
  """
  preprocess_subdag = preprocess_dag.create_dag(
      args, blockbuster_constants.PreprocessingType.PREDICTION, _DAG_ID)

  return subdag_operator.SubDagOperator(
      task_id=task_id, subdag=preprocess_subdag, dag=main_dag)


def create_predict_task(main_dag: models.DAG, args: _AirflowDagArgs,
                        task_id: str) -> subdag_operator.SubDagOperator:
  """Creates batch_predict subdag for the batch_predictions pipeline.

  Args:
    main_dag: DAG to add this operator to.
    args: Dict type DAG arguments.
    task_id: Task id for the subdag.

  Returns:
    SubdagOperator to use within a DAG to make batch predictions.
  """
  batch_predict_subdag = batch_predict_dag.create_batch_predict_dag(
      args, _DAG_ID)

  return subdag_operator.SubDagOperator(
      task_id=task_id, subdag=batch_predict_subdag, dag=main_dag)


def create_activate_task(main_dag: models.DAG, args: _AirflowDagArgs,
                         task_id: str) -> subdag_operator.SubDagOperator:
  """Creates activate_ga subdag for the batch_predictions pipeline.

  Args:
    main_dag: DAG to add this operator to.
    args: Dict type DAG arguments.
    task_id: Task id for the subdag.

  Returns:
    SubdagOperator to use within a DAG to do activation.
  """
  activate_ga_subdag = activate_ga_dag.create_dag(args, _DAG_ID)

  return subdag_operator.SubDagOperator(
      task_id=task_id, subdag=activate_ga_subdag, dag=main_dag)


def create_cleanup_task(main_dag: models.DAG, args: _AirflowDagArgs,
                        task_id: str) -> subdag_operator.SubDagOperator:
  """Creates the main dag for cleaning up of GCS bucket.

  Args:
    main_dag: DAG to add this operator to.
    args: Dict type DAG arguments.
    task_id: Task id for the subdag.

  Returns:
    Parent prediction DAG for cleanup.
  """
  cleanup_gcs_subdag = cleanup_storage_dag.create_dag(args, _DAG_ID)

  return subdag_operator.SubDagOperator(
      task_id=task_id, subdag=cleanup_gcs_subdag, dag=main_dag)


def create_prediction_activate_dag():
  """Creates the main dag for analyze main dag.

  Returns:
    Parent training DAG for analyzing.
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
      start_days_ago=blockbuster_constants.DEFAULT_START_DAYS_AGO, **args)

  prepare_source_task = create_prepare_source_task(main_dag, args,
                                                   prepare_source_dag.DAG_NAME)
  analyze_task = create_analyze_task(main_dag, args, 'analyze')
  preprocess_task = create_preprocess_task(main_dag, args, 'preprocess')
  predict_task = create_predict_task(main_dag, args, 'batch_predict')
  activate_task = create_activate_task(main_dag, args, 'activate_ga')
  clean_up_task = create_cleanup_task(main_dag, args, 'cleanup_gcs')

  # Create task dependency pipeline.
  prepare_source_task.set_downstream(analyze_task)
  analyze_task.set_downstream(preprocess_task)
  preprocess_task.set_downstream(predict_task)
  predict_task.set_downstream(activate_task)
  activate_task.set_downstream(clean_up_task)
  return main_dag


if os.getenv(_AIRFLOW_ENV):  # Returns AIRFLOW_HOME.
  dag = create_prediction_activate_dag()
