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

"""Data Connector Operator to send data from input source to output source."""

from typing import Any, List, Dict, Optional

from airflow import models

from dependencies.hooks import monitoring_hook as monitoring
from dependencies.utils import hook_factory

# BigQuery connection ID. Refer to
# https://cloud.google.com/composer/docs/how-to/managing/connections
# for more details on Managing Airflow connections.
_BQ_CONN_ID = 'bigquery_default'


class DataConnectorOperator(models.BaseOperator):
  """Custom Operator to send data from an input hook to an output hook."""

  def __init__(self, *args,
               return_report: bool = False,
               enable_monitoring: bool = True,
               input_hook: hook_factory.InputHookType,
               output_hook: hook_factory.OutputHookType,
               dag_name: str,
               **kwargs) -> None:
    """Initiates the DataConnectorOperator.

    Args:
      *args: arguments for the operator.
      return_report: Indicates whether to return a run report or not.
      enable_monitoring: If enabled, data transfer monitoring log will be
          stored in Storage to allow for retry of failed events.
      input_hook: The type of the input hook.
      output_hook: The type of the output hook.
      dag_name: The ID of the current running dag.
      **kwargs: Other arguments to pass through to the operator or hooks.
    """
    super().__init__(*args, **kwargs)

    self.dag_name = dag_name
    self.input_hook = hook_factory.get_input_hook(input_hook, **kwargs)
    self.output_hook = hook_factory.get_output_hook(output_hook, **kwargs)
    self.return_report = return_report
    self.enable_monitoring = enable_monitoring

    if enable_monitoring:
      self.monitor = monitoring.MonitoringHook(
          bq_conn_id=kwargs.get('bq_conn_id', _BQ_CONN_ID))

  def execute(self, context: Dict[str, Any]) -> Optional[List[Any]]:
    """Executes this Operator.

    Retrieves all blobs with from input_hook and sends them to output_hook.
    Updates Storage with each blob's status upon success or failure.

    TODO: Catch all exceptions from input and output,
    log them and then reraise. Now the program failes without monitoring.

    Args:
      context: Unused.

    Returns:
      A list of tuples of any data returned from output_hook if return_report
      flag is set to True.
    """
    reports = []
    for blb in self.input_hook.events_blobs_generator():
      if blb:
        blb = self.output_hook.send_events(blb)
        reports.append(blb.reports)

        if self.enable_monitoring:
          self.monitor.store_blob(dag_name=self.dag_name,
                                  location=blb.location,
                                  position=blb.position,
                                  num_rows=blb.num_rows)
          self.monitor.store_events(dag_name=self.dag_name,
                                    location=blb.location,
                                    id_event_error_tuple_list=blb.failed_events)

    if self.return_report:
      return reports
