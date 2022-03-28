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
from typing import List, Any, Mapping, Text, Optional, Dict

from airflow import models
from airflow.operators import python_operator
from airflow.utils import helpers
from dependencies.utils import airflow_utils
from dependencies import blockbuster_constants
from dependencies import dag_utils
from dependencies.hooks import automl_hook
from dependencies.operators import automl_operators

_DAG_NAME = 'load_data'
_CREATE_DATASET_TASK = 'create_dataset_task'
_IMPORT_DATASET_TASK = 'import_dataset_task'
_LIST_TABLE_SPECS_TASK = 'list_table_specs_task'
_LIST_COLUMN_SPECS_TASK = 'list_column_specs_task'
_UPDATE_DATASET_TASK = 'update_dataset_task'
_UPDATE_AIRFLOW_VARS_TASK = 'update_variables'


def _get_column_spec(columns_specs: List[Dict[str, Any]],
                     column_name: Text) -> str:
  """Extracts object id from spec of the column.

  Args:
    columns_specs: List of column specs for the columns of the dataset.
    column_name: The name of the column whose id needs to be extracted.

  Returns:
    Column Spec id of the column.
  """
  for column in columns_specs:
    if column['displayName'] == column_name:
      return automl_hook.AutoMLTablesHook.extract_object_id(column)
  return ''


def _add_dataset_creation_task(
    dag: models.DAG, bb_vars: dag_utils.AirflowVarsConfig
) -> automl_operators.AutoMLCreateDatasetOperator:
  """Adds task for AutoML dataset creation.

  Args:
    dag: The dag that the task needs to be added to.
    bb_vars: The parsed config values from airflow blockbuster global variable.

  Returns:
    Operator used for AutoML dataset creation within a DAG.
  """
  dataset = {
      'display_name': bb_vars['gcp_project'],
      'tables_dataset_metadata': {}
  }
  return automl_operators.AutoMLCreateDatasetOperator(
      task_id=_CREATE_DATASET_TASK,
      dataset=dataset,
      location=str(bb_vars['gcp_region']),
      project_id=str(bb_vars['gcp_project_id']),
      dag=dag,
  )


def _add_import_data_task(
    dag: models.DAG, dataset_id: str, bb_vars: dag_utils.AirflowVarsConfig,
    storage_vars: dag_utils.AirflowVarsConfig
) -> automl_operators.AutoMLImportDataOperator:
  """Adds task for importing data into the dataset.

  This task will fetch the dataset_id from the dataset created by the
  previous task and import training data into the dataset from bigquery.

  Args:
    dag: The dag that the task needs to be added to.
    dataset_id: The ID of the newly created dataset.
    bb_vars: The parsed config values from airflow blockbuster global variable.
    storage_vars: The parsed config values from airflow storage variable.

  Returns:
    Automl operator used to import data into dataset within a DAG.
  """
  dataset_input = {
      'bigquery_source': {
          'input_uri': (f'bq://{storage_vars["bq_working_project"]}.'
                        f'{storage_vars["bq_working_dataset"]}.'
                        'automl_training_input')
      }
  }
  return automl_operators.AutoMLImportDataOperator(
      task_id=_IMPORT_DATASET_TASK,
      dataset_id=dataset_id,
      location=str(bb_vars['gcp_region']),
      project_id=str(bb_vars['gcp_project_id']),
      input_config=dataset_input,
      dag=dag,
  )


def _add_list_table_specs_task(
    dag: models.DAG,
    dataset_id: str,
    bb_vars: dag_utils.AirflowVarsConfig,
) -> automl_operators.AutoMLTablesListTableSpecsOperator:
  """Adds list table specs task to the dag.

  Args:
    dag: The dag that the task needs to be added to.
    dataset_id: The ID of the newly created dataset.
    bb_vars: The parsed config values from airflow blockbuster global variable.

  Returns:
    Operator used to list table specs of created dataset within a DAG.
  """
  return automl_operators.AutoMLTablesListTableSpecsOperator(
      task_id=_LIST_TABLE_SPECS_TASK,
      dataset_id=dataset_id,
      location=str(bb_vars['gcp_region']),
      project_id=str(bb_vars['gcp_project_id']),
      dag=dag,
  )


def _add_list_column_specs_task(
    dag: models.DAG,
    dataset_id: str,
    bb_vars: dag_utils.AirflowVarsConfig,
) -> automl_operators.AutoMLTablesListColumnSpecsOperator:
  """Adds column specs list task to the dag.

  Args:
    dag: The dag that the task needs to be added to.
    dataset_id: The ID of the newly created dataset.
    bb_vars: The parsed config values from airflow blockbuster global variable.

  Returns:
    Operator used to build numeric stats within a DAG.
  """
  table_spec_id = ('{{ extract_object_id('
                   'task_instance.xcom_pull("list_table_specs_task")[0]) }}')

  return automl_operators.AutoMLTablesListColumnSpecsOperator(
      task_id=_LIST_COLUMN_SPECS_TASK,
      dataset_id=dataset_id,
      table_spec_id=table_spec_id,
      location=str(bb_vars['gcp_region']),
      project_id=str(bb_vars['gcp_project_id']),
      field_mask=['name', 'data_type', 'display_name'],
      dag=dag,
  )


def _add_update_dataset_task(
    dag: models.DAG,
    bb_vars: dag_utils.AirflowVarsConfig,
) -> automl_operators.AutoMLTablesUpdateDatasetOperator:
  """Adds list tablespecs list task to the dag.

  Args:
    dag: The dag that the task needs to be added to.
    bb_vars: The parsed config values from airflow blockbuster global variable.

  Returns:
    Operator used to build numeric stats within a DAG.
  """
  update = {
      'display_name': bb_vars['gcp_project'],
      'tables_dataset_metadata': {}
  }
  update[
      'name'] = '{{ task_instance.xcom_pull("create_dataset_task")["name"] }}'
  update['tables_dataset_metadata']['target_column_spec_id'] = (
      '{{ get_column_spec('
      'task_instance.xcom_pull("list_column_specs_task"), target) }}')
  update['tables_dataset_metadata']['ml_use_column_spec_id'] = (
      '{{ get_column_spec(task_instance.xcom_pull('
      '"list_column_specs_task"), "MLDataSplit") }}')
  # TODO: Assign each column to have the correct Data Type
  return automl_operators.AutoMLTablesUpdateDatasetOperator(
      task_id=_UPDATE_DATASET_TASK,
      dataset=update,
      location=str(bb_vars['gcp_region']),
      project_id=str(bb_vars['gcp_project_id']),
      dag=dag,
  )


def _set_dataset_var(**context):
  """Sets the new AutoML dataset id as a variable in the airflow environment.

  Args:
    **context: Airflow context object that is passed by the PythonOperator.
  """
  dataset_id = context['task_instance'].xcom_pull(
      task_ids='create_dataset_task', key='dataset_id')
  models.Variable.set(blockbuster_constants.BLOCKBUSTER_RECENT_DATASET_VAL,
                      dataset_id)


def _add_update_airflow_variable_task(
    dag: models.DAG,) -> python_operator.PythonOperator:
  """Adds a airflow variable with the new dataset id.

  Args:
    dag: The dag that the task needs to be added to.

  Returns:
    PythonOperator used to update airflow variable within a DAG.
  """
  return python_operator.PythonOperator(
      task_id=_UPDATE_AIRFLOW_VARS_TASK,
      python_callable=_set_dataset_var,
      provide_context=True,
      dag=dag,
  )


def create_dag(
    args: Mapping[Text, Any],
    parent_dag_name: Optional[Text] = None,
) -> models.DAG:
  """Generates a DAG that loads data into an AutoML Dataset.

  Args:
    args: Arguments to provide to the AutoML operators as defaults.
    parent_dag_name: If this value is provided, the newly created dag object is
      made a subdag of the parent dag.

  Returns:
    The DAG object.
  """
  # Load params from Variables.
  bb_vars = airflow_utils.retrieve_airflow_variable_as_dict(
      blockbuster_constants.BLOCKBUSTER_GLOBAL_CONFIG)
  storage_vars = airflow_utils.retrieve_airflow_variable_as_dict(
      blockbuster_constants.BLOCKBUSTER_STORAGE_CONFIG)
  # Create dag.
  dag = airflow_utils.initialize_airflow_dag(
      dag_utils.get_dag_id(_DAG_NAME, parent_dag_name),
      None,  # schedule
      blockbuster_constants.DEFAULT_DAG_RETRY,
      blockbuster_constants.DEFAULT_DAG_RETRY_DELAY_MINS,
      blockbuster_constants.DEFAULT_START_DAYS_AGO,
      local_macros={
          'get_column_spec': _get_column_spec,
          'target': 'predictionLabel',
          'extract_object_id': automl_hook.AutoMLTablesHook.extract_object_id,
      },
      **args)
  dataset_creation_task = _add_dataset_creation_task(dag, bb_vars)
  dataset_id = (
      "{{ task_instance.xcom_pull('create_dataset_task', key='dataset_id') }}")
  import_data_task = _add_import_data_task(dag, dataset_id, bb_vars,
                                           storage_vars)
  list_table_specs_task = _add_list_table_specs_task(dag, dataset_id, bb_vars)
  list_column_specs_task = _add_list_column_specs_task(dag, dataset_id, bb_vars)
  update_dataset_task = _add_update_dataset_task(dag, bb_vars)
  update_airflow_variable_task = _add_update_airflow_variable_task(dag)
  helpers.chain(dataset_creation_task, import_data_task, list_table_specs_task,
                list_column_specs_task, update_dataset_task,
                update_airflow_variable_task)
  return dag
