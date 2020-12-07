# coding=utf-8
# Copyright 2020 Google LLC..
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

# python3
# coding=utf-8
# Copyright 2020 Google LLC.
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

"""Custom hook to monitor and log CC4D info into BigQuery."""

import datetime
import enum
import json
from typing import Any, Dict, List, Tuple, Generator

from airflow import exceptions
from airflow.contrib.hooks import bigquery_hook

from dependencies.utils import errors
from dependencies.utils import retry_utils


class MonitoringEntityMap(enum.Enum):
  RUN = -1
  BLOB = -2
  REPORT = -3
  RETRY = -4

_DEFAULT_MONITORING_DATASET_ID = 'cc4d_monitoring_dataset'
_DEFAULT_MONITORING_TABLE_ID = 'cc4d_monitoring_table'

_BASE_BQ_HOOK_PARAMS = ('delegate_to', 'use_legacy_sql', 'location')

_LOG_SCHEMA_FIELDS = [
    {'name': 'dag_name', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
    {'name': 'type_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
    {'name': 'location', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'position', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'info', 'type': 'STRING', 'mode': 'NULLABLE'}]


def _generate_zone_aware_timestamp() -> str:
  """Returns the current timezone aware timestamp."""
  return datetime.datetime.utcnow().isoformat() + 'Z'


class MonitoringHook(bigquery_hook.BigQueryHook):
  """Custom hook monitoring CC4D.

  Attributes:
    dataset_id: Unique name of the dataset.
    table_id: Unique location within the dataset.
  """

  def __init__(self,
               bq_conn_id: str,
               bq_dataset_id: str = _DEFAULT_MONITORING_DATASET_ID,
               bq_table_id: str = _DEFAULT_MONITORING_TABLE_ID,
               **kwargs) -> None:
    """Initializes the generator of a specified BigQuery table.

    Args:
      bq_conn_id: Connection id passed to airflow's BigQueryHook.
      bq_dataset_id: Dataset id of the target table.
      bq_table_id: Table name of the target table.
      **kwargs: Other arguments to pass through to Airflow's BigQueryHook.
    """
    self.dataset_id = bq_dataset_id
    self.table_id = bq_table_id

    init_params_dict = {}
    for param in _BASE_BQ_HOOK_PARAMS:
      if param in kwargs:
        init_params_dict[param] = kwargs[param]
    super().__init__(bigquery_conn_id=bq_conn_id, **init_params_dict)

    self._create_monitoring_dataset_and_table_if_not_exist()

  def _create_monitoring_dataset_and_table_if_not_exist(self) -> None:
    """Creates a monitoring dataset and table if doesn't exist."""
    bq_cursor = self.get_conn().cursor()
    try:
      bq_cursor.get_dataset(dataset_id=self.dataset_id,
                            project_id=bq_cursor.project_id)
    except exceptions.AirflowException:
      try:
        bq_cursor.create_empty_dataset(dataset_id=self.dataset_id,
                                       project_id=bq_cursor.project_id)
      except exceptions.AirflowException as error:
        raise errors.MonitoringDatabaseError(
            error=error, msg='Can\'t create new dataset named '
            '%s in project %s.' % (self.dataset_id, bq_cursor.project_id))

    if not self.table_exists(project_id=bq_cursor.project_id,
                             dataset_id=self.dataset_id,
                             table_id=self.table_id):
      try:
        bq_cursor.create_empty_table(
            project_id=bq_cursor.project_id, dataset_id=self.dataset_id,
            table_id=self.table_id, schema_fields=_LOG_SCHEMA_FIELDS)
      except exceptions.AirflowException as error:
        raise errors.MonitoringDatabaseError(
            error=error, msg='Can\'t create new table named '
            '%s in database %s in project %s.' % (
                self.table_id, self.dataset_id, bq_cursor.project_id))

  @retry_utils.logged_retry_on_retriable_http_airflow_exception
  def _store_monitoring_items_with_retries(
      self, rows: List[Dict[str, Any]]) -> None:
    """Stores a monitoring item in BigQuery.

    Args:
      rows: The rows to send to the monitoring DB.
    """
    if rows:
      bq_cursor = self.get_conn().cursor()
      bq_cursor.insert_all(project_id=bq_cursor.project_id,
                           dataset_id=self.dataset_id,
                           table_id=self.table_id, rows=rows)

  def _values_to_row(self, dag_name: str,
                     timestamp: str,
                     type_id: int,
                     location: str,
                     position: str,
                     information: str) -> Dict[str, Any]:
    """Prepares and formats a DB row.

    Args:
      dag_name: Airflow DAG ID that is associated with the current monitoring.
      timestamp: The log timestamp.
      type_id: The item or error number as listed in errors.MonitoringIDsMap.
      location: The specific object location of the events within the source.
      position: The events starting position within the source or any other
                additional textual information.
      information: JSON event or any other additional textual information.

    Returns:
      a JSON row of field names and their values.
    """
    values = [dag_name, timestamp, type_id, location, position, information]
    row = {}

    for value, field in zip(values, _LOG_SCHEMA_FIELDS):
      row[field['name']] = value

    row = {'json': row}

    return row

  def store_run(self, dag_name: str, location: str, timestamp: str = None,
                json_report_1: str = None, json_report_2: str = None) -> None:
    """Stores a run log-item into monitoring DB.

    Args:
      dag_name: Airflow DAG ID that is associated with the current monitoring.
      location: The run input resource location URL.
      timestamp: The log timestamp. If None, current timestamp will be used.
      json_report_1: Any run related report data in JSON format.
      json_report_2: Any run related report data in JSON format.
    """
    if timestamp is None:
      timestamp = _generate_zone_aware_timestamp()

    row = self._values_to_row(dag_name=dag_name,
                              timestamp=timestamp,
                              type_id=MonitoringEntityMap.RUN.value,
                              location=location,
                              position=json_report_1,
                              information=json_report_2)
    try:
      self._store_monitoring_items_with_retries([row])
    except exceptions.AirflowException as error:
      raise errors.MonitoringAppendLogError(error=error,
                                            msg='Failed to insert rows')

  def store_blob(self, dag_name: str, location: str,
                 position: int, num_rows: int, timestamp: str = None) -> None:
    """Stores all blobs log-item into monitoring DB.

    Args:
      dag_name: Airflow DAG ID that is associated with the current monitoring.
      location: The run input resource location URL.
      position: The events' starting position within the BigQuery table or
        Google Cloud Storage blob file.
      num_rows: Number of rows read in blob starting from start_id.
      timestamp: The log timestamp. If None, current timestamp will be used.
    """
    if timestamp is None:
      timestamp = _generate_zone_aware_timestamp()

    row = self._values_to_row(dag_name=dag_name,
                              timestamp=timestamp,
                              type_id=MonitoringEntityMap.BLOB.value,
                              location=location,
                              position=str(position),
                              information=str(num_rows))
    try:
      self._store_monitoring_items_with_retries([row])
    except exceptions.AirflowException as error:
      raise errors.MonitoringAppendLogError(error=error,
                                            msg='Failed to insert rows')

  def store_events(
      self, dag_name: str, location: str, timestamp: str = None,
      id_event_error_tuple_list: List[Tuple[int, Dict[str, Any], int]] = None
      ) -> None:
    """Stores all event log-items into monitoring DB.

    Args:
      dag_name: Airflow DAG ID that is associated with the current monitoring.
      location: The run input resource location URL.
      timestamp: The log timestamp. If None, current timestamp will be used.
      id_event_error_tuple_list: all (id, event, error_num) trupls to store.
        The tuples are a set of 3 fields:
         - id: Row IDs of events in BigQuery input table, or line numbers
           in a google cloud storage blob file.
         - event: the JSON event.
         - error: The errors.MonitoringIDsMap error ID.
    """
    if timestamp is None:
      timestamp = _generate_zone_aware_timestamp()

    rows = []
    for id_event_error_tuple in id_event_error_tuple_list:
      rows.append(self._values_to_row(
          dag_name=dag_name,
          timestamp=timestamp,
          type_id=id_event_error_tuple[2],
          location=location,
          position=str(id_event_error_tuple[0]),
          information=json.dumps(id_event_error_tuple[1])))

    try:
      self._store_monitoring_items_with_retries(rows)
    except exceptions.AirflowException as error:
      raise errors.MonitoringAppendLogError(error=error,
                                            msg='Failed to insert rows')

  def generate_processed_blobs_ranges(
      self, dag_name: str, location: str
      ) -> Generator[Tuple[Any, Any], None, None]:
    """Generates tuples of processed blobs from monitoring DB.

    Generates tuples of (position, info) for each blob with the same dag_id and
    location.

    Args:
      dag_name: Airflow DAG ID that is associated with the current monitoring.
      location: The run input resource location URL.

    Yields:
      Tuples of (position, info) of processed events id ranges.
    """
    sql = (
        f'SELECT [position], [info] '
        f'FROM [{self.dataset_id}.cc4d_monitoring_table] '
        f'WHERE [dag_name]=%(dag_name)s '
        f'  AND [location]=%(location)s AND [type_id]=%(type_id)s'
        f'ORDER BY [position];')
    bq_cursor = self.get_conn().cursor()
    bq_cursor.execute(
        sql, {
            'dag_name': dag_name,
            'location': location,
            'type_id': MonitoringEntityMap.BLOB.value
        })

    while True:
      row = bq_cursor.fetchone()
      if row is None:
        break
      yield row[0], row[1]
