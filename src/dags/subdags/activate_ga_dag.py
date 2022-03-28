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

"""Airflow subdag that pushes the output data to Google Analytics."""
from typing import Any, Mapping, Optional

from airflow import models

from dependencies.tcrm.operators import data_connector_operator
from dependencies.tcrm.utils import hook_factory
from dependencies.utils import airflow_utils
from dependencies import blockbuster_constants
from dependencies import dag_utils

_DAG_NAME = 'activate_ga'

# GA_BASE_PARAMS are default parameters that serve as the base on which to build
# the Measurement Protocol payload.
_GA_BASE_PARAMS = {'v': '1', 'ni': 1, 'ua': 'Chrome/70.0.3538.77'}

# The events could be formatted as CSV or JSON files in Google Cloud Storage
# buckets. This parameter indicates which format to use.
_GCS_CONTENT_TYPE = 'JSON'


def _add_storage_to_ga_task(dag, bucket_uri, ga_tracking_id, bq_dataset,
                            bq_table):
  """Adds Google Cloud Storage(GCS) to Google Analytics data transfer task.

  Args:
    dag: The dag object which will include this task.
    bucket_uri: The uri of the GCS path containing the data.
    ga_tracking_id: The Google Analytics tracking id.
    bq_dataset: BQ data set.
    bq_table: BQ Table for monitoring purposes.

  Returns:
    The task to move data from GCS to GA.
  """
  bucket_name, bucket_prefix = dag_utils.extract_bucket_parts(bucket_uri)
  return (data_connector_operator.DataConnectorOperator(
      dag_name=_DAG_NAME,
      task_id='storage_to_ga',
      input_hook=hook_factory.InputHookType.GOOGLE_CLOUD_STORAGE,
      output_hook=hook_factory.OutputHookType.GOOGLE_ANALYTICS,
      enable_monitoring=False,
      monitoring_dataset=bq_dataset,
      monitoring_table=bq_table,
      monitoring_bq_conn_id='bigquery_default',
      gcs_bucket=bucket_name,
      gcs_prefix=bucket_prefix,
      gcs_content_type=_GCS_CONTENT_TYPE,
      ga_base_params=_GA_BASE_PARAMS,
      ga_tracking_id=ga_tracking_id,
      dag=dag))


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
  activation_vars = airflow_utils.retrieve_airflow_variable_as_dict(
      blockbuster_constants.BLOCKBUSTER_ACTIVATION_CONFIG)

  bucket_uri = storage_vars['gcs_output_path']
  bq_dataset = storage_vars['bq_working_dataset']
  bq_table = 'monitoring'
  ga_tracking_id = activation_vars['ga_tracking_id']

  _add_storage_to_ga_task(dag, bucket_uri, ga_tracking_id, bq_dataset, bq_table)

  return dag
