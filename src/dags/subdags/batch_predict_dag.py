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

"""Airflow DAG that uses Google AutoML services to output batch predictions."""
import datetime as dt
import time
from typing import Any, Dict, Optional, Union

from airflow import models
from airflow.providers.google.cloud.operators import bigquery as bigquery_operator
from airflow.providers.google.cloud.transfers import bigquery_to_gcs
from dependencies.utils import airflow_utils
from dependencies.utils import pipeline_utils
from dependencies import blockbuster_constants
from dependencies import dag_utils
from dependencies.airflow.operators import automl_tables_batch_prediction_operator

DAG_NAME = 'batch_predict'


def _extract_dataset_id(bq_uri: str) -> str:
  return bq_uri.split('.')[1]


def create_batch_predict_dag(
    args: Dict[str, Union[Dict[str, Any], dt.datetime]],
    parent_dag_name: Optional[str] = None,
) -> models.DAG:
  """Generates a DAG/SubDAG that predicts using an existing model.

  Args:
    args: Arguments to provide to the operators as defaults.
    parent_dag_name: If this is provided, this is a SubDAG.

  Returns:
    DAG

  Raises:
    KeyError: If recent_model_id Airflow Variable hasn't been set.
  """
  dag_id = dag_utils.get_dag_id(DAG_NAME, parent_dag_name)

  dag = airflow_utils.initialize_airflow_dag(
      dag_id=dag_id,
      schedule=None,
      retries=blockbuster_constants.DEFAULT_DAG_RETRY,
      retry_delay=blockbuster_constants.DEFAULT_DAG_RETRY_DELAY_MINS,
      start_days_ago=blockbuster_constants.DEFAULT_START_DAYS_AGO,
      local_macros={'extract_dataset_id': _extract_dataset_id}, **args)

  batch_predict_task = _get_batch_predictions(dag, 'batch_predict_task')
  automl_bq_location = (
      '{{ extract_dataset_id('  # macro to be expanded in task Jinja template
      'task_instance.xcom_pull('
      '"batch_predict_task", key="bq_output_dataset")) }}')
  batch_predict_sql = _generate_batch_prediction_sql_template(
      'batch_predict', automl_bq_location)

  get_output_data_task = _store_final_results_to_bq(dag, 'get_output_data',
                                                    batch_predict_sql)

  bq_to_gcs_task = _transfer_bigquery_to_gcs(dag, 'bq_to_gcs')


  dag >> batch_predict_task >> get_output_data_task >> bq_to_gcs_task


  return dag


def _get_batch_predictions(dag: models.DAG,
                           task_id: str) -> models.BaseOperator:
  """Batch Predict the GA leads using pre-trained AutoML model.

  Args:
    dag: the DAG to add this operator to
    task_id: ID for this specific task within the DAG.

  Returns:
    Operator to use within a DAG to run the Batch Prediction Pipeline on automl.
  """
  storage_vars = airflow_utils.retrieve_airflow_variable_as_dict(
      blockbuster_constants.BLOCKBUSTER_STORAGE_CONFIG)

  model_id = airflow_utils.get_airflow_variable(
      blockbuster_constants.BLOCKBUSTER_PREDICTION_RECENT_MODEL)

  bq_input_path = 'bq://{project}.{dataset}.automl_prediction_input'.format(
      project=storage_vars['bq_working_project'],
      dataset=storage_vars['bq_working_dataset'])

  output_path = f'bq://{storage_vars["bq_working_project"]}'

  output_key = 'bq_output_dataset'
  task_batch_predict = (
      automl_tables_batch_prediction_operator
      .AutoMLTablesBatchPredictionOperator(
          task_id=task_id,
          model_id=model_id,
          input_path=bq_input_path,
          output_path=output_path,
          output_key=output_key,
          conn_id='google_cloud_default',
          dag=dag))

  return task_batch_predict


def _store_final_results_to_bq(dag: models.DAG, task_id: str,
                               batch_predict_sql: str) -> models.BaseOperator:
  """Store MP complaint results in Bigquery before GA transfer.

  Args:
    dag: the DAG to add this operator to
    task_id: ID for this specific task within the DAG.
    batch_predict_sql: Custom Query to pick records and add some additional
      colums as MP protocol.

  Returns:
    Operator to use within a DAG to store Prediction results to Bigquery.
  """
  bb_vars = airflow_utils.retrieve_airflow_variable_as_dict(
      blockbuster_constants.BLOCKBUSTER_GLOBAL_CONFIG)
  storage_vars = airflow_utils.retrieve_airflow_variable_as_dict(
      blockbuster_constants.BLOCKBUSTER_STORAGE_CONFIG)

  final_output_table = '{project}.{dataset}.final_output'.format(
      project=storage_vars['bq_working_project'],
      dataset=storage_vars['bq_working_dataset'])

  return bigquery_operator.BigQueryExecuteQueryOperator(
      task_id=task_id,
      sql=batch_predict_sql,
      use_legacy_sql=False,
      destination_dataset_table=final_output_table,
      create_disposition='CREATE_IF_NEEDED',
      write_disposition='WRITE_TRUNCATE',
      allow_large_results=True,
      location=bb_vars['gcp_region'],
      dag=dag,
  )


def _generate_batch_prediction_sql_template(template: str,
                                            bq_location: str) -> str:
  """Build batch_predict sql as per Measurement Protocol.

  Args:
    template: sql jinja template to load.
    bq_location: Location where the batch_predictions are stored by auto_ml

  Returns:
    SQL.
  """
  storage_vars = airflow_utils.retrieve_airflow_variable_as_dict(
      blockbuster_constants.BLOCKBUSTER_STORAGE_CONFIG)
  activation_vars = airflow_utils.retrieve_airflow_variable_as_dict(
      blockbuster_constants.BLOCKBUSTER_PREDICTION_ACTIVATION_CONFIG)
  prediction_source_table = '{project}.{bigquery_location}.predictions'.format(
      project=storage_vars['bq_working_project'], bigquery_location=bq_location)
  sql_template = pipeline_utils.render_sql_from_template(
      template,
      source_table=prediction_source_table,
      num_segments=20,
      event_label=activation_vars['event_label'],
      event_action="'" + activation_vars['event_action'] + "'",
      event_category="'" + activation_vars['event_category'] + "'"
      )

  return sql_template


def _transfer_bigquery_to_gcs(dag, task_id) -> models.BaseOperator:
  """Pipeline to transfer finally transferable output to GCS.

  Args:
    dag: the DAG to add this operator to
    task_id: ID for this specific task within the DAG.

  Returns:
    Operator to use within a DAG to run the Pipeline for moving records to GCS.
  """
  storage_vars = airflow_utils.retrieve_airflow_variable_as_dict(
      blockbuster_constants.BLOCKBUSTER_STORAGE_CONFIG)

  final_output_uri = '{path}/result-{timestamp}-*.json'.format(
      path=storage_vars['gcs_output_path'],
      timestamp=int(time.time()))

  final_output_table = '{project}.{dataset}.final_output'.format(
      project=storage_vars['bq_working_project'],
      dataset=storage_vars['bq_working_dataset'])

  return bigquery_to_gcs.BigQueryToGCSOperator(
      task_id=task_id,
      source_project_dataset_table=final_output_table,
      destination_cloud_storage_uris=[final_output_uri],
      export_format='NEWLINE_DELIMITED_JSON',
      dag=dag)
