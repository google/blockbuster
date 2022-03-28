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

"""Airflow DAG that uses ML Windowing Pipeline (MLWP) to generate input data for AutoML."""
import datetime
from typing import Any, Mapping, Optional

from airflow import models
from airflow.providers.google.cloud.operators import bigquery as bigquery_operator
from airflow.providers.google.cloud.operators import dataflow as dataflow_operator
from airflow.utils import dates
from airflow.utils import helpers

from dependencies.utils import airflow_utils
from dependencies.utils import pipeline_utils
from dependencies import blockbuster_constants
from dependencies import dag_utils

_DAG_NAME = 'preprocess'
# Date Format expected by ML Windowing Pipeline
# https://github.com/GoogleCloudPlatform/cloud-for-marketing/tree/master/marketing-analytics/predicting/ml-data-windowing-pipeline#to-run-this-directly-using-the-examples-from-options-above-1
_ML_WINDOWING_PIPELINE_DATE_FORMAT = '%d/%m/%Y'


def _get_sliding_window_pipeline_params_for_training(
    preprocess_vars: dag_utils.AirflowVarsConfig,
    storage_vars: dag_utils.AirflowVarsConfig,
    training_vars: dag_utils.AirflowVarsConfig):
  """Returns parameters used in training mode of sliding window pipeline.

  For details see the github page of ML Windowing Pipeline:
  https://github.com/GoogleCloudPlatform/cloud-for-marketing/tree/master/marketing-analytics/predicting/ml-data-windowing-pipeline

  Args:
    preprocess_vars: The parsed config values from airflow preprocess variable.
    storage_vars: The parsed config values from airflow storage variable.
    training_vars: The parsed config values from airflow training variable.

  Returns:
    The snapshot start and end dates of the GA dataset and the output path.
  """
  output_path = f'{storage_vars["gcs_temp_path"]}/training'
  prediction_days = int(preprocess_vars['prediction_days'])
  lookback_days = int(preprocess_vars['lookback_days'])
  snapshot_start_dt = (datetime.datetime.strptime(
      str(training_vars['start_date']),
      '%Y%m%d') + datetime.timedelta(days=lookback_days))
  snapshot_end_dt = (datetime.datetime.strptime(
      str(training_vars['end_date']),
      '%Y%m%d') - datetime.timedelta(days=prediction_days))
  snapshot_start_dt_str = datetime.datetime.strftime(
      snapshot_start_dt, _ML_WINDOWING_PIPELINE_DATE_FORMAT)
  snapshot_end_dt_str = datetime.datetime.strftime(
      snapshot_end_dt, _ML_WINDOWING_PIPELINE_DATE_FORMAT)
  return snapshot_start_dt_str, snapshot_end_dt_str, output_path


def _get_sliding_window_pipeline_params_for_prediction(
    prediction_vars: dag_utils.AirflowVarsConfig,
    preprocess_vars: dag_utils.AirflowVarsConfig,
    storage_vars: dag_utils.AirflowVarsConfig):
  """Returns parameters used in training mode of sliding window pipeline.

  For details see the github page of ML Windowing Pipeline:
  https://github.com/GoogleCloudPlatform/cloud-for-marketing/tree/master/marketing-analytics/predicting/ml-data-windowing-pipeline

  Args:
    prediction_vars: The parsed config values from airflow prediction variable.
    preprocess_vars: The parsed config values from airflow preprocess variable.
    storage_vars: The parsed config values from airflow storage variable.

  Returns:
    The snapshot start and end dates of the GA dataset and the output path.
  """
  output_path = f'{storage_vars["gcs_temp_path"]}/prediction'
  lookback_gap = int(preprocess_vars['lookbackGapInDays'])
  leads_window = int(prediction_vars['leads_submission_window'])
  lead_st_date_gap = lookback_gap + leads_window
  snapshot_start_dt = datetime.datetime.strftime(
      dates.days_ago(lead_st_date_gap), _ML_WINDOWING_PIPELINE_DATE_FORMAT)
  snapshot_end_dt = datetime.datetime.strftime(
      dates.days_ago(lookback_gap), _ML_WINDOWING_PIPELINE_DATE_FORMAT)
  return snapshot_start_dt, snapshot_end_dt, output_path


def _add_mlwp_sliding_window_pipeline_task(
    dag: models.DAG, output_type: blockbuster_constants.PreprocessingType,
    prediction_vars: dag_utils.AirflowVarsConfig,
    preprocess_vars: dag_utils.AirflowVarsConfig,
    storage_vars: dag_utils.AirflowVarsConfig,
    training_vars: dag_utils.AirflowVarsConfig
) -> dataflow_operator.DataflowTemplatedJobStartOperator:
  """Adds the Sliding Window Task of ML Windowing Pipeline to dag.

  Args:
    dag: The dag that the task needs to be added to.
    output_type: Indicates whether this pipeline is to be used for training or
      prediction.
    prediction_vars: The parsed config values from airflow prediction variable.
    preprocess_vars: The parsed config values from airflow preprocess variable.
    storage_vars: The parsed config values from airflow storage variable.
    training_vars: The parsed config values from airflow training variable.

  Returns:
    The configured Sliding Window task that was added to the input dag.
  """
  if output_type == blockbuster_constants.PreprocessingType.TRAINING:
    snapshot_start_dt, snapshot_end_dt, output_path = _get_sliding_window_pipeline_params_for_training(
        preprocess_vars, storage_vars, training_vars)
  elif output_type == blockbuster_constants.PreprocessingType.PREDICTION:
    snapshot_start_dt, snapshot_end_dt, output_path = _get_sliding_window_pipeline_params_for_prediction(
        prediction_vars, preprocess_vars, storage_vars)
  template_file_directory = storage_vars['gcs_dataflow_path']
  return dataflow_operator.DataflowTemplatedJobStartOperator(
      task_id='mlwp_step3',
      template=f'{template_file_directory}/SlidingWindowPipeline',
      parameters={
          'snapshotStartDate':
              snapshot_start_dt,
          'snapshotEndDate':
              snapshot_end_dt,
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
          'lookbackGapInSeconds':
              str(int(preprocess_vars['lookbackGapInDays']) * 86400),
          'windowTimeInSeconds':
              str(preprocess_vars['windowTimeInSeconds']),
          'outputSlidingWindowAvroPrefix':
              f'{output_path}/windowing-output/',
      },
      dag=dag)


def _add_mlwp_generate_features_pipeline_task(
    dag: models.DAG,
    output_type: blockbuster_constants.PreprocessingType,
    feature_options: dag_utils.FeatureConfigListMapping,
    storage_vars: dag_utils.AirflowVarsConfig,
) -> dataflow_operator.DataflowTemplatedJobStartOperator:
  """Adds the Generate Features Task of ML Windowing Pipeline to dag.

  Args:
    dag: The dag that the task needs to be added to.
    output_type: Indicate whether this pipeline is to be used for training or
      prediction.
    feature_options: The parsed config values from airflow feature object
      variable.
    storage_vars: The parsed config values from airflow storage variable.

  Returns:
    The configured Generate Features task that was added to the input dag.
  """
  if output_type == blockbuster_constants.PreprocessingType.TRAINING:
    output_table = 'training'
    training_mode = 'true'
    output_path = f'{storage_vars["gcs_temp_path"]}/training'
  elif output_type == blockbuster_constants.PreprocessingType.PREDICTION:
    output_table = 'prediction'
    training_mode = 'false'
    output_path = f'{storage_vars["gcs_temp_path"]}/prediction'
  template_file_directory = storage_vars['gcs_dataflow_path']
  step_4_output = (f'{storage_vars["bq_working_project"]}:'
                   f'{storage_vars["bq_working_dataset"]}.'
                   f'ga_{output_table}_input')

  # Always add the BB_id as a RECENT feature
  mod_features = list(feature_options['features'])
  mod_features.append({
      'fact': 'BB_id',
      'type': 'Categorical',
      'accumulators': 'recent'
  })
  feature_options_copy = dict(feature_options)
  feature_options_copy['features'] = mod_features

  return dataflow_operator.DataflowTemplatedJobStartOperator(
      task_id='mlwp_step4',
      template=f'{template_file_directory}/GenerateFeaturesPipeline',
      parameters={
          **dag_utils.generate_feature_pipeline_parameters(
              feature_options_copy), 'windowedAvroLocation':
              f'{output_path}/windowing-output/*.avro',
          'featureDestinationTable':
              step_4_output,
          'trainMode':
              training_mode,
          'showEffectiveDateWeekOfYear':
              'false',
          'showEffectiveDateMonthOfYear':
              'false'
      },
      dag=dag)


def _add_prepare_automl_data_in_bq_task(
    dag: models.DAG, output_type: blockbuster_constants.PreprocessingType,
    prediction_vars: dag_utils.AirflowVarsConfig,
    storage_vars: dag_utils.AirflowVarsConfig
) -> bigquery_operator.BigQueryExecuteQueryOperator:
  """Adds the task to write the output to Big Query to dag.

  Args:
    dag: The dag that the task needs to be added to.
    output_type: Indicate whether this pipeline is to be used for training or
      prediction.
    prediction_vars: The parsed config values from airflow prediction variable.
    storage_vars: The parsed config values from airflow storage variable.

  Returns:
    The configured BigQueryOperator task to write input data for automl that was
    added to the dag.
  """
  exclude_from_output = ['userId', 'RECENT_BB_id', 'RECENT_most_recent_lead']
  if output_type == blockbuster_constants.PreprocessingType.TRAINING:
    output_table = 'training'
    exclude_from_output.append('BB_id')
  elif output_type == blockbuster_constants.PreprocessingType.PREDICTION:
    output_table = 'prediction'
    exclude_from_output.append('MLDataSplit')
  features_table = dag_utils.construct_bq_table_path(
      storage_vars['bq_working_project'], storage_vars['bq_working_dataset'],
      f'ga_{output_table}_input')
  prepare_data_sql = pipeline_utils.render_sql_from_template(
      'prepare_data',
      features_table=features_table,
      exclude_from_output=exclude_from_output,
      inclusion_recency_days=prediction_vars['leads_submission_window'])

  output_dataset = dag_utils.construct_bq_table_path(
      storage_vars['bq_working_project'], storage_vars['bq_working_dataset'],
      f'automl_{output_table}_input')

  prepare_data_for_automl = bigquery_operator.BigQueryExecuteQueryOperator(
      task_id='prepare_data_for_automl',
      sql=prepare_data_sql,
      use_legacy_sql=False,
      destination_dataset_table=output_dataset,
      create_disposition='CREATE_IF_NEEDED',
      write_disposition='WRITE_TRUNCATE',
      allow_large_results=True,
      dag=dag,
  )
  return prepare_data_for_automl


def create_dag(
    args: Mapping[str, Any],
    output_type: blockbuster_constants.PreprocessingType,
    parent_dag_name: Optional[str] = None,
) -> models.DAG:
  """Generates a DAG that preprocesses data.

  Args:
    args: Arguments to provide to the Operators as defaults.
    output_type: Which set of Variables to load for preprocessing
    parent_dag_name: If this is provided, this is a SubDAG.

  Returns:
    The DAG object.
  """
  preprocess_vars = airflow_utils.retrieve_airflow_variable_as_dict(
      blockbuster_constants.BLOCKBUSTER_PREPROCESS_CONFIG)
  prediction_vars = airflow_utils.retrieve_airflow_variable_as_dict(
      blockbuster_constants.BLOCKBUSTER_PREDICTION_ACTIVATION_CONFIG)
  storage_vars = airflow_utils.retrieve_airflow_variable_as_dict(
      blockbuster_constants.BLOCKBUSTER_STORAGE_CONFIG)
  training_vars = airflow_utils.retrieve_airflow_variable_as_dict(
      blockbuster_constants.BLOCKBUSTER_TRAINING_CONFIG)
  feature_options = dag_utils.get_feature_config_val(
      blockbuster_constants.BLOCKBUSTER_FEATURE_CONFIG)

  dag = airflow_utils.initialize_airflow_dag(
      dag_utils.get_dag_id(_DAG_NAME, parent_dag_name), None,
      blockbuster_constants.DEFAULT_DAG_RETRY,
      blockbuster_constants.DEFAULT_DAG_RETRY_DELAY_MINS,
      blockbuster_constants.DEFAULT_START_DAYS_AGO, **args)

  mlwp_sliding_window_pipeline_task = _add_mlwp_sliding_window_pipeline_task(
      dag, output_type, prediction_vars, preprocess_vars, storage_vars,
      training_vars)
  mlwp_generate_features_pipeline_task = _add_mlwp_generate_features_pipeline_task(
      dag, output_type, feature_options, storage_vars)
  prepare_automl_data_in_bq_task = _add_prepare_automl_data_in_bq_task(
      dag, output_type, prediction_vars, storage_vars)
  helpers.chain(mlwp_sliding_window_pipeline_task,
                mlwp_generate_features_pipeline_task,
                prepare_automl_data_in_bq_task)

  return dag
