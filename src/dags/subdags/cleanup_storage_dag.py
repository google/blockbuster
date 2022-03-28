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

"""Airflow subdag module to delete files & folders from Cloud Storage bucket."""

from typing import Any, Mapping, Optional
from airflow import models

from dependencies.utils import airflow_utils
from dependencies import blockbuster_constants
from dependencies import dag_utils
from dependencies.operators import gcs_delete_operator

# Airflow configuration.
_DAG_NAME = 'cleanup_gcs'


def _cleanup_storage_task(dag: models.DAG, bucket_name: str, bucket_path: str):
  gcs_delete_operator.GoogleCloudStorageDeleteOperator(
      task_id='cleanup_storage_blobs',
      bucket=bucket_name,
      directory=bucket_path,
      dag=dag)


def create_dag(
    args: Mapping[str, Any],
    parent_dag_name: Optional[str] = None,
) -> models.DAG:
  """Generates a DAG that pushes data from Google Cloud Storage to GA.

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
      start_days_ago=blockbuster_constants.DEFAULT_START_DAYS_AGO, **args)

  storage_vars = airflow_utils.retrieve_airflow_variable_as_dict(
      blockbuster_constants.BLOCKBUSTER_STORAGE_CONFIG)

  bucket_name, bucket_path = dag_utils.extract_bucket_parts(
      storage_vars['gcs_output_path'])
  _cleanup_storage_task(dag, bucket_name, bucket_path)

  return dag
