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

"""Airflow SubDAG that creates the source table in BQ for blockbuster."""

import datetime as dt
from typing import Union, Optional, Dict, List, Any

from airflow import models
from airflow.providers.google.cloud.operators import bigquery as bigquery_operator

from dependencies.utils import airflow_utils
from dependencies.utils import pipeline_utils
from dependencies import blockbuster_constants
from dependencies import dag_utils

DAG_NAME = 'prepare_source_data'
TASK_NAME = 'prepare_source_data_task'


def get_leads_table(bq_working_project: str, bq_working_dataset: str,
                    table_suffix: str) -> str:
  leads_table = (f'{bq_working_project}.'
                 f'{bq_working_dataset}.'
                 f'ga_sessions_leads_{table_suffix}')
  return leads_table


def get_sql_params_for_training_stage(
    preprocess_vars: dag_utils.AirflowVarsConfig
) -> Dict[str, Union[str, int, bool, List[str]]]:
  """Returns the sql params required for the training phase.

  Args:
    preprocess_vars: The parsed config values from airflow preprocess variable.

  Returns:
    The sql param names and values as a Dict.
  """
  lead_start_dt = dt.datetime.strftime(
      dt.datetime.strptime(str(preprocess_vars['start_date']), '%Y%m%d'),
      '%Y%m%d')
  window_start_dt = lead_start_dt
  ga_source = preprocess_vars['ga_source']
  lead_filters = [
      f'_TABLE_SUFFIX >= \'{lead_start_dt}\'',
      '(hits.page.pagePathLevel1 = \'/cart\' or'
      ' hits.page.pagePathLevel2 = \'/cart\' or'
      ' hits.page.pagePathLevel3 = \'/cart\')'
  ]
  return {
      'lead_filter_criteria': lead_filters,
      'window_start_date': window_start_dt,
      'ga_source': ga_source
  }


def get_sql_params_for_prediction_stage(
    preprocess_vars: dag_utils.AirflowVarsConfig,
    prediction_vars: dag_utils.AirflowVarsConfig
) -> Dict[str, Union[str, int, bool, List[str]]]:
  """Returns the sql params required for the prediction phase.

  Args:
    preprocess_vars: The parsed config values from airflow preprocess variable.
    prediction_vars: The parsed config values from airflow prediction variable.

  Returns:
    The sql param names and values as a Dict.
  """
  lookback_days = int(preprocess_vars['lookback_days'])
  lookback_gap_in_days = int(preprocess_vars['lookbackGapInDays'])
  leads_window = int(prediction_vars['leads_submission_window'])

  lead_start_days_gap = leads_window + lookback_gap_in_days
  window_start_days_gap = lead_start_days_gap + lookback_days
  lead_start_dt = dt.datetime.strftime(
      (dt.datetime.today() - dt.timedelta(days=lead_start_days_gap)), '%Y%m%d')
  window_start_dt = dt.datetime.strftime(
      (dt.datetime.today() - dt.timedelta(days=window_start_days_gap)),
      '%Y%m%d')
  ga_source = preprocess_vars['ga_source']
  lead_filters = [
      f'_TABLE_SUFFIX >= \'{lead_start_dt}\'',
      '(hits.page.pagePathLevel1 = \'/cart\' or'
      ' hits.page.pagePathLevel2 = \'/cart\' or'
      ' hits.page.pagePathLevel3 = \'/cart\')'
  ]
  return {
      'lead_filter_criteria': lead_filters,
      'window_start_date': window_start_dt,
      'ga_source': ga_source
  }


def add_prepare_source_data_task_to_dag(
    dag: models.DAG, sql: str, leads_table: str,
    gcp_region: str) -> bigquery_operator.BigQueryExecuteQueryOperator:
  """Adds the BQ task to dag to create ML source table from GA source table.

  Args:
    dag: The dag that the task needs to be added to.
    sql: The parsed config values from airflow prediction variable.
    leads_table: The destination table the output data will be written to.
    gcp_region: GCP region the job will run in.

  Returns:
    The configured BigQueryOperator task that was added to the input dag.
  """
  prepare_source_data = bigquery_operator.BigQueryExecuteQueryOperator(
      task_id=TASK_NAME,
      sql=sql,
      use_legacy_sql=False,
      destination_dataset_table=leads_table,
      create_disposition='CREATE_IF_NEEDED',
      write_disposition='WRITE_TRUNCATE',
      allow_large_results=True,
      location=gcp_region,
      dag=dag,
  )
  return prepare_source_data


def create_dag(
    args: Dict[str, Union[Dict[str, Any], dt.datetime]],
    output_type: blockbuster_constants.PreprocessingType,
    parent_dag_name: Optional[str] = None,
) -> models.DAG:
  """Generates a DAG that create source table from GA tables.


  Args:
    args: Arguments to provide to the operators as defaults.
    output_type: Which set of variables to load for preprocessing and
      prediction.
    parent_dag_name: If this is provided, this is a SubDAG.

  Returns:
    The DAG object.
  """
  # Load params from Airflow Variables.
  bb_vars = airflow_utils.retrieve_airflow_variable_as_dict(
      blockbuster_constants.BLOCKBUSTER_GLOBAL_CONFIG)
  preprocess_vars = airflow_utils.retrieve_airflow_variable_as_dict(
      blockbuster_constants.BLOCKBUSTER_PREPROCESS_CONFIG)
  prediction_vars = airflow_utils.retrieve_airflow_variable_as_dict(
      blockbuster_constants.BLOCKBUSTER_PREDICTION_ACTIVATION_CONFIG)
  storage_vars = airflow_utils.retrieve_airflow_variable_as_dict(
      blockbuster_constants.BLOCKBUSTER_STORAGE_CONFIG)
  dag_id = dag_utils.get_dag_id(DAG_NAME, parent_dag_name)
  dag = airflow_utils.initialize_airflow_dag(
      dag_id, None, blockbuster_constants.DEFAULT_DAG_RETRY,
      blockbuster_constants.DEFAULT_DAG_RETRY_DELAY_MINS,
      blockbuster_constants.DEFAULT_START_DAYS_AGO, **args)
  if output_type == blockbuster_constants.PreprocessingType.TRAINING:
    table_suffix = 'training'
    sql_vars = get_sql_params_for_training_stage(preprocess_vars)
  elif output_type == blockbuster_constants.PreprocessingType.PREDICTION:
    table_suffix = 'prediction'
    sql_vars = get_sql_params_for_prediction_stage(preprocess_vars,
                                                   prediction_vars)
  sql = pipeline_utils.render_sql_from_template('source_leads', **sql_vars)
  bq_working_project = storage_vars['bq_working_project']
  bq_working_dataset = storage_vars['bq_working_dataset']
  leads_table = get_leads_table(bq_working_project, bq_working_dataset,
                                table_suffix)
  gcp_region = bb_vars['gcp_region']
  add_prepare_source_data_task_to_dag(dag, sql, leads_table, gcp_region)
  return dag
