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

"""Utility functions for Airflow pipelines."""
import datetime
import json
import os
import re
from typing import Any, Dict, Optional, Union

from airflow import configuration
from airflow import models
from airflow import utils
from airflow.operators import dagrun_operator


def get_airflow_variable(key: str) -> str:
  """Retrieves variable's value from Airflow given key.

  Args:
    key: Airflow Variable key.

  Returns:
    Airflow Variable value.
  """
  return models.Variable.get(key)


def initialize_airflow_dag(dag_id: str,
                           schedule: Union[str, None],
                           retries: int,
                           retry_delay: int,
                           start_days_ago: int = 1,
                           local_macros: Optional[Dict[str, Any]] = None,
                           **kwargs) -> models.DAG:
  """Creates Airflow DAG with appropriate default args.

  Args:
    dag_id: Id for the DAG.
    schedule: DAG run schedule. Ex: if set `@once`, DAG will be scheduled to run
      only once. For more refer:
        https://airflow.apache.org/docs/stable/scheduler.html
    retries: How many times DAG retries.
    retry_delay: The interval (in minutes) to trigger the retry.
    start_days_ago: Start date of the DAG. By default it's set to 1 (yesterday)
      to trigger DAG as soon as its deployed.
    local_macros: A dictionary of macros that will be exposed in jinja
      templates.
    **kwargs: Keyword arguments.

  Returns:
    Instance of airflow.models.DAG.
  """
  default_args = {
      'retries': retries,
      'retry_delay': datetime.timedelta(minutes=retry_delay),
      'start_date': utils.dates.days_ago(start_days_ago)
  }

  if kwargs:
    default_args.update(kwargs)

  return models.DAG(
      dag_id=dag_id,
      schedule_interval=schedule,
      user_defined_macros=local_macros,
      default_args=default_args)


def construct_bq_table_path(table_name: str) -> str:
  """Constructs BigQuery table path from Airflow variables.

  Args:
    table_name: BigQuery table name.

  Returns:
    Full path to BigQuery table in the format:
    <project_id>.<dataset_name>.<table_name>

  Raises:
    ValueError if incorrect table name is provided.
  """
  if not re.match(r'^\w+$', table_name):
    raise ValueError(
        f'{table_name} should contain only letters, numbers and underscore.')

  return '{}.{}.{}'.format(
      get_airflow_variable('dest_project'),
      get_airflow_variable('dest_dataset'), table_name)


def create_trigger_task(
    main_dag: models.DAG,
    trigger_dag_id: str) -> dagrun_operator.TriggerDagRunOperator:
  """Triggers DAG given the ID.

  Args:
    main_dag: The models.DAG instance.
    trigger_dag_id: The ID of the DAG to trigger.

  Returns:
    dagrun_operator.TriggerDagRunOperator.
  """
  return dagrun_operator.TriggerDagRunOperator(
      task_id=f'trigger-{trigger_dag_id}',
      trigger_dag_id=trigger_dag_id,
      dag=main_dag)


def retrieve_airflow_variable_as_dict(
    key: str) -> Dict[str, Union[str, Dict[str, str]]]:
  """Retrieves Airflow variables given key.

  Args:
    key: Airflow variable key.

  Returns:
    Airflow Variable value as a string or Dict.

  Raises:
    Exception if given Airflow Variable value cannot be parsed.
  """
  value = models.Variable.get(key)
  try:
    value_dict = json.loads(value)
  except json.decoder.JSONDecodeError as error:
    raise Exception('Provided key "{}" cannot be decoded. {}'.format(
        key, error))
  return value_dict


def get_sql_path(file_path: str) -> str:
  """Constructs fully qualified SQL file path.

  Args:
    file_path: Path of the SQL file.

  Returns:
    Fully qualified SQL file path.
  """
  dag_dir = configuration.get('core', 'dags_folder')
  return os.path.join(dag_dir, file_path)
