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

"""Airflow subdag that creates and trains a new model using the training dataset."""
from typing import Any, Mapping, Optional

from airflow import models
from airflow.operators import python_operator
from airflow.utils import helpers

from dependencies.utils import airflow_utils
from dependencies import blockbuster_constants
from dependencies import dag_utils
from dependencies.operators import automl_operators
_DAG_NAME = 'train_model'
_CREATE_MODEL_TASK = 'create_model_task'
_UPDATE_AIRFLOW_VARS_TASK = 'update_airflow_vars'


def _add_create_model_task(dag, bb_vars, training_vars):
  """Adds task which creates and trains a new model using the training dataset.

  Args:
    dag: The dag object which will include this task.
    bb_vars: The parsed config values from airflow blockbuster global variable.
    training_vars: The parsed config values from airflow blockbuster taining
      variable.

  Returns:
    The task to move data from GCS to GA.
  """
  dataset_id = airflow_utils.get_airflow_variable('recent_dataset')
  training_milli_node_hours = 1000 * int(training_vars['model_training_hours'])
  model = {
      'display_name': bb_vars['gcp_project'],
      'dataset_id': dataset_id,
      'tables_model_metadata': {
          'train_budget_milli_node_hours': training_milli_node_hours,
          'optimization_objective': 'MAXIMIZE_AU_PRC',
      },
  }

  return automl_operators.AutoMLTrainModelOperator(
      task_id=_CREATE_MODEL_TASK,
      model=model,
      location=bb_vars['gcp_region'],
      project_id=bb_vars['gcp_project_id'],
      dag=dag,
  )


def _set_model_var(**context):
  """Sets the new AutoML model id as a variable in the airflow environment.

  Args:
    **context: Airflow context object that is passed by the python operator.
  """
  model_id = context['task_instance'].xcom_pull(
      task_ids='create_model_task', key='model_id')
  models.Variable.set(blockbuster_constants.BLOCKBUSTER_RECENT_MODEL_VAL,
                      model_id)


def _add_update_airflow_variable_task(
    dag: models.DAG) -> python_operator.PythonOperator:
  """Adds a airflow variable with the new dataset id.

  Args:
    dag: The dag that the task needs to be added to.

  Returns:
    PythonOperator used to update airflow variable within a DAG.
  """
  return python_operator.PythonOperator(
      task_id=_UPDATE_AIRFLOW_VARS_TASK,
      python_callable=_set_model_var,
      provide_context=True,
      dag=dag,
  )


def create_dag(
    args: Mapping[str, Any],
    parent_dag_name: Optional[str] = None,
) -> models.DAG:
  """Generates a DAG that trains a new model using the training dataset.

  Args:
    args: Arguments to provide to the Airflow DAG object as defaults.
    parent_dag_name: If this is provided, this is a SubDAG.

  Returns:
    The DAG object.
  """
  dag = airflow_utils.initialize_airflow_dag(
      dag_id=dag_utils.get_dag_id(_DAG_NAME, parent_dag_name),
      schedule=None,
      retries=blockbuster_constants.DEFAULT_DAG_RETRY,
      retry_delay=blockbuster_constants.DEFAULT_DAG_RETRY_DELAY_MINS,
      start_days_ago=blockbuster_constants.DEFAULT_START_DAYS_AGO,
      **args)

  bb_vars = airflow_utils.retrieve_airflow_variable_as_dict(
      blockbuster_constants.BLOCKBUSTER_GLOBAL_CONFIG)
  training_vars = airflow_utils.retrieve_airflow_variable_as_dict(
      blockbuster_constants.BLOCKBUSTER_TRAINING_CONFIG)

  create_model_task = _add_create_model_task(dag, bb_vars, training_vars)
  update_airflow_variable_task = _add_update_airflow_variable_task(dag)

  helpers.chain(create_model_task, update_airflow_variable_task)
  return dag
