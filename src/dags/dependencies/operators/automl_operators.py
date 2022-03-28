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

"""This module contains custom Google AutoML operators.

TODO:(b/155625565): This module needs to be removed as the open source  automl
operator becomes available .
These are very light wrappers built on top of the AutoML Tables Api to enable
the blockbuster code to interact with automl apis in airflow environment. This
model does not use the publicly available google auto ml client library as it is
not available .
The code does not parse or try to validate the data structures that
are being sent as input against the models in the AutoML libraries and uses them
as is in the api, keeping the validation at the client level since this library
is only meant as a temporary substitute while the actual client libraries cannot
be used.
"""
from typing import List, Optional

from airflow import models
from dependencies.hooks import automl_hook

_DEFAULT_OPERATION_TIMEOUT = 86400
_DEFAULT_POLL_INTERVAL = 600


class AutoMLTrainModelOperator(models.BaseOperator):
  """Creates Google Cloud AutoML model.

  Creates a model. Returns a Model in the response field when it completes. When
  you create a model, several model evaluations are created for it: a global
  evaluation, and one evaluation for each annotation spec. See-
  https://cloud.google.com/automl/docs/reference/rest/v1beta1/projects.locations.models/create
  """

  template_fields = ("model", "location", "project_id")

  def __init__(self,
               model: automl_hook.GenericAutomlObject,
               project_id: str,
               location: str,
               operation_timeout: Optional[int] = _DEFAULT_OPERATION_TIMEOUT,
               poll_wait_time: Optional[int] = _DEFAULT_POLL_INTERVAL,
               gcp_conn_id: str = "google_cloud_default",
               **kwargs):
    """Constructor.

    Args:
      model: Model definition. It must be of the same form as the protobuf
        message.
          See:https://cloud.google.com/automl/docs/reference/rest/v1beta1/projects.locations.models.
      project_id: ID of the Google Cloud project where model will be created if
        None then default project_id is used.
      location: The location of the project.
      operation_timeout: Maximum time(seconds) to wait for AutoML prediction to
        complete.
      poll_wait_time: time in seconds to sleep between polling predict operation
        status.
      gcp_conn_id: The connection id used by AutoML hooks to get authorization
        tokens.
      **kwargs: Keyword aruguments passed to base class constructor.
    """
    super().__init__(**kwargs)

    self.model = model
    self.project_id = project_id
    self.location = location
    self.operation_timeout = operation_timeout
    self.poll_wait_time = poll_wait_time
    self.gcp_conn_id = gcp_conn_id

  def execute(self, context):
    hook = automl_hook.AutoMLTablesHook(gcp_conn_id=self.gcp_conn_id)
    self.log.info("Creating model.")
    operation = hook.create_model(
        model=self.model,
        project_id=self.project_id,
        location=self.location,
        operation_timeout=self.operation_timeout,
        poll_wait_time=self.poll_wait_time)
    operation_response = operation["response"]
    model_id = hook.extract_object_id(operation_response)
    self.log.info("Model created: %s", model_id)

    self.xcom_push(context, key="model_id", value=model_id)
    return operation_response


class AutoMLBatchPredictOperator(models.BaseOperator):
  """Perform a batch prediction on Google Cloud AutoML.

  Perform a batch prediction. This is a long running
  operation. Once the operation is done, BatchPredictResult is returned in the
  response field. See-
  https://cloud.google.com/automl/docs/reference/rest/v1beta1/projects.locations.models/batchPredict
  """

  template_fields = (
      "model_id",
      "input_config",
      "output_config",
      "location",
      "project_id",
  )

  def __init__(self,
               model_id: str,
               input_config: str,
               output_config: str,
               project_id: str,
               location: str,
               operation_timeout: Optional[int] = _DEFAULT_OPERATION_TIMEOUT,
               poll_wait_time: Optional[int] = _DEFAULT_POLL_INTERVAL,
               gcp_conn_id: str = "google_cloud_default",
               **kwargs):
    """Constructor.

    Args:
      model_id: AutoML model id.
      input_config: The input configuration for batch prediction. See:
        https://cloud.google.com/automl/docs/reference/rest/v1beta1/projects.locations.models/batchPredict#BatchPredictInputConfig
      output_config: The Configuration specifying where output predictions
        should be written. See:
        https://cloud.google.com/automl/docs/reference/rest/v1beta1/projects.locations.models/batchPredict#BatchPredictOutputConfig
      project_id: ID of the Google Cloud project where model will be created if
        None then default project_id is used.
      location: The location of the project.
      operation_timeout: Maximum time(seconds) to wait for AutoML prediction to
        complete.
      poll_wait_time: Time in seconds to sleep between polling predict operation
        status.
      gcp_conn_id: The connection id used by AutoML hooks to get authorization
        tokens.
      **kwargs: Keyword aruguments passed to base class constructor.
    """
    super().__init__(**kwargs)

    self.model_id = model_id
    self.input_config = input_config
    self.output_config = output_config
    self.project_id = project_id
    self.location = location
    self.operation_timeout = operation_timeout
    self.poll_wait_time = poll_wait_time
    self.gcp_conn_id = gcp_conn_id

  def execute(self, context):
    hook = automl_hook.AutoMLTablesHook(gcp_conn_id=self.gcp_conn_id)
    self.log.info("Fetch batch prediction.")
    operation = hook.batch_predict(
        model_id=self.model_id,
        input_config=self.input_config,
        output_config=self.output_config,
        project_id=self.project_id,
        location=self.location,
        operation_timeout=self.operation_timeout,
        poll_wait_time=self.poll_wait_time)

    self.log.info("Batch prediction ready.")
    output_info = operation["metadata"]["batchPredictDetails"]["outputInfo"]
    if "gcsOutputDirectory" in output_info:
      output_loc = output_info["gcsOutputDirectory"]
    elif "bigqueryOutputDataset" in output_info:
      output_loc = output_info["bigqueryOutputDataset"]
    else:
      raise RuntimeError(f"Output not found in prediction response:{operation}")

    self.xcom_push(
        context, key="prediction_result_output_loc", value=output_loc)
    return operation


class AutoMLCreateDatasetOperator(models.BaseOperator):
  """Creates a Google Cloud AutoML dataset.

     See -
      https://cloud.google.com/automl/docs/reference/rest/v1beta1/projects.locations.datasets/create
  """

  template_fields = ("dataset", "location", "project_id")

  def __init__(self,
               dataset: automl_hook.GenericAutomlObject,
               project_id: str,
               location: str,
               gcp_conn_id: str = "google_cloud_default",
               **kwargs):
    """Constructor.

    Args:
      dataset: The dataset to create. It must be of the same form as the
        protobuf message Dataset. See:
        https://cloud.google.com/automl/docs/reference/rest/v1beta1/projects.locations.datasets.
      project_id: ID of the Google Cloud project where dataset is located if
        None then default project_id is used.
      location: The location of the project.
      gcp_conn_id: The connection id used by AutoML hooks to get authorization
        tokens.
      **kwargs: Keyword aruguments passed to base class constructor.
    """
    super().__init__(**kwargs)

    self.dataset = dataset
    self.project_id = project_id
    self.location = location
    self.gcp_conn_id = gcp_conn_id

  def execute(self, context):
    hook = automl_hook.AutoMLTablesHook(gcp_conn_id=self.gcp_conn_id)
    self.log.info("Creating dataset")
    result = hook.create_dataset(
        dataset=self.dataset,
        project_id=self.project_id,
        location=self.location)
    dataset_id = hook.extract_object_id(result)
    self.log.info("Creating completed. Dataset id: %s", dataset_id)

    self.xcom_push(context, key="dataset_id", value=dataset_id)
    return result


class AutoMLImportDataOperator(models.BaseOperator):
  """Imports data to a Google Cloud AutoML dataset.

  Imports data into a dataset. For Tables this method can only be called on an
  empty Dataset. See-
  https://cloud.google.com/automl/docs/reference/rest/v1beta1/projects.locations.datasets/importData
  """

  template_fields = ("dataset_id", "input_config", "location", "project_id")

  def __init__(self,
               dataset_id: str,
               input_config: automl_hook.GenericAutomlObject,
               location: str,
               project_id: Optional[str] = None,
               operation_timeout: Optional[int] = _DEFAULT_OPERATION_TIMEOUT,
               poll_wait_time: Optional[int] = _DEFAULT_POLL_INTERVAL,
               gcp_conn_id: str = "google_cloud_default",
               **kwargs) -> None:
    """Constructor.

    Args:
      dataset_id: Name of the AutoML dataset.
      input_config: The desired input location and its domain specific
        semantics, if any. It must be of the same form as the protobuf message
        InputConfig. See:
        https://cloud.google.com/automl/docs/reference/rest/v1beta1/projects.locations.datasets/importData#InputConfig
      location: The location of the project.
      project_id: ID of the Google Cloud project where dataset is located if
        None then default project_id is used.
      operation_timeout: Maximum time(seconds) to wait for AutoML prediction to
        complete.
      poll_wait_time: time in seconds to sleep between polling predict operation
        status.
      gcp_conn_id: The connection id used by AutoML hooks to get authorization
        tokens.
      **kwargs: Keyword aruguments passed to base class constructor.
    """
    super().__init__(**kwargs)

    self.dataset_id = dataset_id
    self.input_config = input_config
    self.location = location
    self.project_id = project_id
    self.operation_timeout = operation_timeout
    self.poll_wait_time = poll_wait_time
    self.gcp_conn_id = gcp_conn_id

  def execute(self, context):
    hook = automl_hook.AutoMLTablesHook(gcp_conn_id=self.gcp_conn_id)
    self.log.info("Importing dataset")
    operation = hook.import_data(
        dataset_id=self.dataset_id,
        input_config=self.input_config,
        project_id=self.project_id,
        location=self.location,
        operation_timeout=self.operation_timeout,
        poll_wait_time=self.poll_wait_time)
    self.log.info("Import completed")
    return operation


class AutoMLTablesListColumnSpecsOperator(models.BaseOperator):
  """Lists column specs in a table.

  See-
  https://cloud.google.com/automl/docs/reference/rest/v1beta1/projects.locations.datasets.tableSpecs.columnSpecs/list
  """

  template_fields = (
      "dataset_id",
      "table_spec_id",
      "field_mask",
      "location",
      "project_id",
  )

  def __init__(self,
               dataset_id: str,
               table_spec_id: str,
               location: str,
               field_mask: Optional[List[str]] = None,
               project_id: Optional[str] = None,
               gcp_conn_id: str = "google_cloud_default",
               **kwargs):
    """Constructor.

    Args:
      dataset_id: Name of the AutoML dataset.
      table_spec_id: ID of the AutoML table.
      location: The location of the project.
      field_mask: Comma-separated list of fully qualified names of fields.See:
        https://cloud.google.com/automl/docs/reference/rest/v1beta1/projects.locations.datasets.tableSpecs.columnSpecs/list
      project_id: ID of the Google Cloud project where dataset is located if
        None then default project_id is used.
      gcp_conn_id: The connection id used by AutoML hooks to get authorization
        tokens.
      **kwargs: Keyword aruguments passed to base class constructor.
    """
    super().__init__(**kwargs)
    self.dataset_id = dataset_id
    self.table_spec_id = table_spec_id
    self.field_mask = field_mask
    self.location = location
    self.project_id = project_id
    self.gcp_conn_id = gcp_conn_id

  def execute(self, context):
    hook = automl_hook.AutoMLTablesHook(gcp_conn_id=self.gcp_conn_id)
    self.log.info("Requesting column specs.")
    result = hook.list_column_specs(
        dataset_id=self.dataset_id,
        table_spec_id=self.table_spec_id,
        field_mask=self.field_mask,
        location=self.location,
        project_id=self.project_id,
    )
    self.log.info("Columns specs obtained.")
    return result


class AutoMLTablesUpdateDatasetOperator(models.BaseOperator):
  """Updates a dataset.

  See -
  https://cloud.google.com/automl/docs/reference/rest/v1beta1/projects.locations.datasets/patch
  """

  template_fields = ("dataset", "location")

  def __init__(self,
               dataset: automl_hook.GenericAutomlObject,
               location: str,
               gcp_conn_id: str = "google_cloud_default",
               **kwargs):
    """Constructor.

    Args:
      dataset: The dataset parameters to update. It must be of the same form as
        the protobuf message Dataset. See:
        https://cloud.google.com/automl/docs/reference/rest/v1beta1/projects.locations.datasets.
          Example (to update the tablesDatasetMetadata field of a dataset)- {
            "tablesDatasetMetadata": {
              "mlUseColumnSpecId": "602945",
              "weightColumnSpecId": "602949" } }
      location: The location of the project.
      gcp_conn_id: The connection id used by AutoML hooks to get authorization
        tokens.
      **kwargs: Keyword aruguments passed to base class constructor.
    """
    super().__init__(**kwargs)

    self.dataset = dataset
    self.location = location
    self.gcp_conn_id = gcp_conn_id

  def execute(self, context):
    hook = automl_hook.AutoMLTablesHook(gcp_conn_id=self.gcp_conn_id)
    self.log.info("Updating AutoML dataset %s.", self.dataset["name"])
    result = hook.update_dataset(
        dataset=self.dataset,
        location=self.location)
    self.log.info("Dataset updated.")
    return result


class AutoMLTablesListTableSpecsOperator(models.BaseOperator):
  """Lists table specs in an AutoML dataset.

  See -
  https://cloud.google.com/automl/docs/reference/rest/v1beta1/projects.locations.datasets.tableSpecs/list
  """

  template_fields = ("dataset_id", "location", "project_id")

  def __init__(self,
               dataset_id: str,
               location: str,
               project_id: Optional[str] = None,
               gcp_conn_id: str = "google_cloud_default",
               **kwargs):
    """Constructor.

    Args:
      dataset_id: Name of the AutoML dataset.
      location: The location of the project.
      project_id: ID of the Google Cloud project where dataset is located if
        None then default project_id is used.
      gcp_conn_id: The connection id used by AutoML hooks to get authorization
        tokens.
      **kwargs: Keyword aruguments passed to base class constructor.
    """
    super().__init__(**kwargs)
    self.dataset_id = dataset_id
    self.location = location
    self.project_id = project_id
    self.gcp_conn_id = gcp_conn_id

  def execute(self, context):
    hook = automl_hook.AutoMLTablesHook(gcp_conn_id=self.gcp_conn_id)
    self.log.info("Requesting table specs for %s.", self.dataset_id)
    table_specs_list = hook.list_table_specs(
        dataset_id=self.dataset_id,
        location=self.location,
        project_id=self.project_id)
    self.log.info(table_specs_list)
    self.log.info("Table specs obtained.")
    return table_specs_list
