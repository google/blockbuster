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

"""Airflow DAG that uses Google AutoML services."""

import datetime
from typing import Any, Mapping, Optional, Dict, Union, List, Iterator

from airflow import models
from airflow.providers.google.cloud.operators import bigquery as bigquery_operator
from airflow.providers.google.cloud.operators import dataflow as dataflow_operator
from airflow.utils import helpers

from dependencies.utils import airflow_utils
from dependencies.utils import pipeline_utils
from dependencies import blockbuster_constants
from dependencies import dag_utils

from dependencies.operators import gcs_delete_operator

_DAG_NAME = 'analyze'
_N_TOP_VALUES = 10
_CLEAN_TEMP_DIR_TASK = 'clean_temp_directory'
_USER_SESSION_TASK_ID = 'user_session_pipeline'
_DATA_VISUALISATION_TASK_ID = 'data_visualization_pipeline'
_GENERATE_CATEGORICAL_STATS_TASK = 'generate_categorical_stats'
_GENERATE_NUMERIC_STATS_TASK = 'generate_numeric_stats'


def get_date_from_str(date_str: str) -> datetime.datetime:
  """Returns a datetime object from a string in the '%Y%m%d' format.

  Args:
    date_str: The string in '%Y%m%d' format.

  Returns:
    The datetime object derived from the input date string.
  """
  return datetime.datetime.strptime(str(date_str), '%Y%m%d')


def get_date_str_from_date(date: datetime.datetime,
                           date_format: str = '%Y%m%d') -> str:
  """Returns a string from a datetime object in the required format.

  Args:
    date: The input date to be converted to string.
    date_format: Format in which date has to be returned.

  Returns:
    The input datetime string in required format.
  """
  return datetime.datetime.strftime(date, date_format)


def get_user_session_sql_params(
    output_type: blockbuster_constants.PreprocessingType,
    feature_vars: dag_utils.FeatureConfigListMapping,
    prediction_vars: dag_utils.AirflowVarsConfig,
    preprocess_vars: dag_utils.AirflowVarsConfig,
    storage_vars: dag_utils.AirflowVarsConfig
) -> Dict[str, Union[str, List[str], Iterator[str]]]:
  """Returns the sql params required for the user session pipeline.

  Args:
    output_type: Indicate whether this pipeline is to be used for training or
      prediction.
    feature_vars: The parsed config values from airflow feature object variable.
    prediction_vars: The parsed config values from airflow prediction variable.
    preprocess_vars: The parsed config values from airflow preprocess variable.
    storage_vars: The parsed config values from airflow storage variable.

  Returns:
    The user session pipeline sql param names and values as a Dict.
  """
  selected_fields = dag_utils.get_select_field_array(feature_vars)
  if output_type == blockbuster_constants.PreprocessingType.TRAINING:
    source_table_suffix = 'training'
    proc_st_dt = get_date_from_str(preprocess_vars['start_date'])
    proc_ed_dt = get_date_from_str(preprocess_vars['end_date'])
  else:
    source_table_suffix = 'prediction'
    lookback_days = int(preprocess_vars['lookback_days'])
    lookback_gap_in_days = int(preprocess_vars['lookbackGapInDays'])
    leads_window = int(prediction_vars['leads_submission_window'])
    window_st_days_gap = leads_window + lookback_gap_in_days + lookback_days
    window_ed_days_gap = lookback_gap_in_days
    proc_st_dt = (datetime.datetime.today() -
                  datetime.timedelta(days=window_st_days_gap)).date()
    proc_ed_dt = (datetime.datetime.today() -
                  datetime.timedelta(days=window_ed_days_gap)).date()

  source_table = (f'{storage_vars["bq_working_project"]}.'
                  f'{storage_vars["bq_working_dataset"]}.'
                  f'ga_sessions_leads_{source_table_suffix}')
  selected_fields.append(
      f'CONCAT(\'BB\', {preprocess_vars["userIdColumn"]}) AS BB_id')
  # Tables to use in the FROM, as a list of tuples
  from_tables = [(f'`{source_table}`', 'sessions')]
  if dag_utils.get_features(feature_vars, 'fact', r'^hits\..*'):
    from_tables.append(('UNNEST(hits)', 'hits'))
  return {
      'feature_columns': selected_fields,
      'from_tables': list(map(' AS '.join, from_tables)),
      'start_date': get_date_str_from_date(proc_st_dt),
      'end_date': get_date_str_from_date(proc_ed_dt)
  }


def add_user_session_task(
    dag: models.DAG, task_id: str,
    output_type: blockbuster_constants.PreprocessingType,
    feature_vars: dag_utils.FeatureConfigListMapping,
    prediction_vars: dag_utils.AirflowVarsConfig,
    preprocess_vars: dag_utils.AirflowVarsConfig,
    storage_vars: dag_utils.AirflowVarsConfig,
    training_vars: dag_utils.AirflowVarsConfig) -> models.BaseOperator:
  """Builds the UserSessionPipeline Operator.

  Args:
    dag: The dag that the task needs to be added to.
    task_id: Id string for this specific task within the DAG.
    output_type: Indicate whether this pipeline is to be used for training or
      prediction.
    feature_vars: The parsed config values from airflow feature object variable.
    prediction_vars: The parsed config values from airflow prediction variable.
    preprocess_vars: The parsed config values from airflow preprocess variable.
    storage_vars: The parsed config values from airflow storage variable.
    training_vars: The parsed config values from airflow training variable.

  Returns:
    Operator to use within a DAG to run the User Session Pipeline on Dataflow.
  """
  # Load start/end date from the appropriate Airflow Variable
  if output_type == blockbuster_constants.PreprocessingType.TRAINING:
    output_path = f'{storage_vars["gcs_temp_path"]}/training'

  elif output_type == blockbuster_constants.PreprocessingType.PREDICTION:
    output_path = f'{storage_vars["gcs_temp_path"]}/prediction'

  template_file_directory = storage_vars['gcs_dataflow_path']
  sql_vars = get_user_session_sql_params(
      output_type,
      feature_vars,
      prediction_vars,
      preprocess_vars,
      storage_vars,
  )
  sql = pipeline_utils.render_sql_from_template('usersession_source',
                                                **sql_vars)

  return dataflow_operator.DataflowTemplatedJobStartOperator(
      task_id=task_id,
      template=f'{template_file_directory}/UserSessionPipeline',
      parameters={
          'inputBigQuerySQL': sql,
          'outputSessionsAvroPrefix': f'{output_path}/usersession-output/',
          'predictionFactName': training_vars['predictionFactName'],
          'predictionFactValues': training_vars['predictionFactValues']
      },
      dag=dag)


def add_data_visualization_task(
    dag: models.DAG, task_id: str, preprocess_vars: dag_utils.AirflowVarsConfig,
    storage_vars: dag_utils.AirflowVarsConfig
) -> dataflow_operator.DataflowTemplatedJobStartOperator:
  """Builds the DataVisualizationPipeline Operator.

  Args:
    dag: The dag that the task needs to be added to.
    task_id: ID for this specific task within the DAG.
    preprocess_vars: The parsed config values from airflow preprocess variable.
    storage_vars: The parsed config values from airflow storage variable.

  Returns:
    Operator used to run the Data Visualization Pipeline on Dataflow.
  """
  template_file_directory = storage_vars['gcs_dataflow_path']

  p2_output_dataset = (f'{storage_vars["bq_working_project"]}:'
                       f'{storage_vars["bq_working_dataset"]}')

  proc_st_dt = datetime.datetime.strptime(
      str(preprocess_vars['start_date']), '%Y%m%d')
  proc_ed_dt = datetime.datetime.strptime(
      str(preprocess_vars['end_date']), '%Y%m%d')
  output_path = f'{storage_vars["gcs_temp_path"]}/training'
  lookback_days = int(preprocess_vars['lookback_days'])
  prediction_days = int(preprocess_vars['prediction_days'])

  return dataflow_operator.DataflowTemplatedJobStartOperator(
      task_id=task_id,
      template=f'{template_file_directory}/DataVisualizationPipeline',
      parameters={
          'snapshotStartDate':
              get_date_str_from_date(
                  proc_st_dt + datetime.timedelta(days=lookback_days),
                  date_format='%d/%m/%Y'),
          'snapshotEndDate':
              get_date_str_from_date(
                  proc_ed_dt - datetime.timedelta(days=prediction_days),
                  date_format='%d/%m/%Y'),
          'inputAvroSessionsLocation':
              f'{output_path}/usersession-output/*.avro',
          'stopOnFirstPositiveLabel':
              str(preprocess_vars['stopOnFirstPositiveLabel']),
          'slideTimeInSeconds':
              str(preprocess_vars['slideTimeInSeconds']),
          'minimumLookaheadTimeInSeconds':
              str(preprocess_vars['minimumLookaheadTimeInSeconds']),
          'maximumLookaheadTimeInSeconds':
              str(preprocess_vars['maximumLookaheadTimeInSeconds']),
          'outputBigQueryUserActivityTable':
              f'{p2_output_dataset}.instance',
          'outputBigQueryFactsTable':
              f'{p2_output_dataset}.facts',
      },
      dag=dag)


def add_categorical_stats_task(
    dag: models.DAG, feature_vars: dag_utils.FeatureConfigListMapping,
    storage_vars: dag_utils.AirflowVarsConfig
) -> bigquery_operator.BigQueryExecuteQueryOperator:
  """Builds an Operator that generates categorical fact stats within a DAG.

  Args:
    dag: The dag that the task needs to be added to.
    feature_vars: The parsed config values from airflow feature object variable.
    storage_vars: The parsed config values from airflow storage variable.

  Returns:
    Operator used to build categorical stats within a DAG.
  """
  cat_feats = dag_utils.get_features(feature_vars, 'type', 'Categorical')
  stats_dataset = (f'{storage_vars["bq_working_project"]}.'
                   f'{storage_vars["bq_working_dataset"]}')
  categorical_stats_sql = pipeline_utils.render_sql_from_template(
      'categorical_stats',
      fact_table=f'{stats_dataset}.facts',
      feature_columns=[
          f'\'{dag_utils.get_feature_name(x)}\'' for x in cat_feats
      ])

  return bigquery_operator.BigQueryExecuteQueryOperator(
      task_id=_GENERATE_CATEGORICAL_STATS_TASK,
      sql=categorical_stats_sql,
      use_legacy_sql=False,
      destination_dataset_table=f'{stats_dataset}.cat_facts_stats_table',
      create_disposition='CREATE_IF_NEEDED',
      write_disposition='WRITE_TRUNCATE',
      allow_large_results=True,
      dag=dag)


def add_numeric_stats_task(
    dag: models.DAG, feature_vars: dag_utils.FeatureConfigListMapping,
    storage_vars: dag_utils.AirflowVarsConfig
) -> bigquery_operator.BigQueryExecuteQueryOperator:
  """Builds an Operator that generates numeric fact stats within a DAG.

  Args:
    dag: The dag that the task needs to be added to.
    feature_vars: The parsed config values from airflow feature object variable.
    storage_vars: The parsed config values from airflow storage variable.

  Returns:
    Operator used to build numeric stats within a DAG.
  """
  num_feats = dag_utils.get_features(feature_vars, 'type', 'Numeric')
  stats_dataset = (f'{storage_vars["bq_working_project"]}.'
                   f'{storage_vars["bq_working_dataset"]}')
  numeric_stats_sql = pipeline_utils.render_sql_from_template(
      'numeric_stats',
      fact_table=f'{stats_dataset}.facts',
      feature_columns=[
          f'\'{dag_utils.get_feature_name(x)}\'' for x in num_feats
      ])

  return bigquery_operator.BigQueryExecuteQueryOperator(
      task_id=_GENERATE_NUMERIC_STATS_TASK,
      sql=numeric_stats_sql,
      use_legacy_sql=False,
      destination_dataset_table=f'{stats_dataset}.num_facts_stats_table',
      create_disposition='CREATE_IF_NEEDED',
      write_disposition='WRITE_TRUNCATE',
      allow_large_results=True,
      dag=dag)


def create_dag(
    args: Mapping[str, Any],
    output_type: blockbuster_constants.PreprocessingType,
    parent_dag_name: Optional[str] = None,
) -> models.DAG:
  """Generates a DAG that analyzes data before preprocessing.

  Args:
    args: Arguments to provide to the Operators as defaults.
    output_type: Which set of Variables to load for preprocessing.
    parent_dag_name: If this is provided, this is a SubDAG.

  Returns:
    The DAG object.
  """
  # Load params from Airflow Variables
  preprocess_vars = airflow_utils.retrieve_airflow_variable_as_dict(
      blockbuster_constants.BLOCKBUSTER_PREPROCESS_CONFIG)
  prediction_vars = airflow_utils.retrieve_airflow_variable_as_dict(
      blockbuster_constants.BLOCKBUSTER_PREDICTION_ACTIVATION_CONFIG)
  storage_vars = airflow_utils.retrieve_airflow_variable_as_dict(
      blockbuster_constants.BLOCKBUSTER_STORAGE_CONFIG)
  training_vars = airflow_utils.retrieve_airflow_variable_as_dict(
      blockbuster_constants.BLOCKBUSTER_TRAINING_CONFIG)
  feature_vars = dag_utils.get_feature_config_val(
      blockbuster_constants.BLOCKBUSTER_FEATURE_CONFIG)
  dag = airflow_utils.initialize_airflow_dag(
      dag_utils.get_dag_id(_DAG_NAME, parent_dag_name), None,
      blockbuster_constants.DEFAULT_DAG_RETRY,
      blockbuster_constants.DEFAULT_DAG_RETRY_DELAY_MINS,
      blockbuster_constants.DEFAULT_START_DAYS_AGO, **args)
  if output_type == blockbuster_constants.PreprocessingType.TRAINING:
    bucket_name, bucket_path = dag_utils.extract_bucket_parts(
        f'{storage_vars["gcs_temp_path"]}/training')
  else:
    bucket_name, bucket_path = dag_utils.extract_bucket_parts(
        f'{storage_vars["gcs_temp_path"]}/prediction')

  clean_temp_dir_task = gcs_delete_operator.GoogleCloudStorageDeleteOperator(
      task_id=_CLEAN_TEMP_DIR_TASK,
      bucket=bucket_name,
      directory=bucket_path,
      dag=dag)

  user_session_pipeline_task = add_user_session_task(
      dag, _USER_SESSION_TASK_ID, output_type, feature_vars, prediction_vars,
      preprocess_vars, storage_vars, training_vars)
  if output_type == blockbuster_constants.PreprocessingType.TRAINING:
    data_visualization_pipeline_task = add_data_visualization_task(
        dag, _DATA_VISUALISATION_TASK_ID, preprocess_vars, storage_vars)
    generate_categorical_stats_task = add_categorical_stats_task(
        dag, feature_vars, storage_vars)
    generate_numeric_stats_task = add_numeric_stats_task(
        dag, feature_vars, storage_vars)
    helpers.chain(
        clean_temp_dir_task, user_session_pipeline_task,
        data_visualization_pipeline_task,
        [generate_categorical_stats_task, generate_numeric_stats_task])
  else:
    helpers.chain(clean_temp_dir_task, user_session_pipeline_task)
  return dag
