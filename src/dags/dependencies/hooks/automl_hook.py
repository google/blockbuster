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

"""Custom hook for Google AutoML Tables API (v1beta1).

TODO:(b/155625565): This module needs to be removed as the open source  automl
hook ( or even the google automl client library )becomes available .
These are very light wrappers built on top of the AutoML Tables Api to enable
the blockbuster code to interact with automl apis in airflow environment. This
model does not use the publicly available google auto ml client library as it is
not available .
The code does not parse or try to validate the data structures that
are being sent as input against the models in the AutoML libraries and uses them
as is in the api, keeping the validation at the client level as this library is
only meant as a temporary substitute while the actual client libraries cannot be
used.
"""
import http
import logging
import time
from typing import Dict, Optional, Any, Sequence
from airflow.contrib.hooks import gcp_api_base_hook
from google.auth.transport import requests

_AUTOML_BASE_URL = 'https://automl.googleapis.com/v1beta1'

_RETRY_ATTEMPTS = 3
_DEFAULT_POLL_SLEEP_TIME = 60
_DEFAULT_OPERATION_TIMEOUT = 3600

# akashchoudhury@ - A generic object definition is being used as the objects
# have a complex nested structure.
GenericAutomlObject = Dict[str, Any]


class AutoMLApiError(RuntimeError):
  pass


def retry_if_raises(times: int):
  """Returns a decorator to retry a function when runtime exception is raised.

  Args:
    times: Number of times to retry after the initial attempt fails. This number
    is exclusive of the first try.

  Returns:
    A decorator.
  """

  def decorator(func):
    """Decorates a function to retry when exception is raised.

    Raises internal error after maximum number of retries.

    Args:
      func: Function to be wrapped.

    Returns:
      The wrapped function.
    """

    def wrapper(*args, **kwargs):
      for trial in range(times + 1):
        try:
          return func(*args, **kwargs)
        except AutoMLApiError as e:
          # Retry in case of AutoMLApiError type errors
          logging.exception('Failed for trial %s', trial)
          if trial == times:
            raise e

    return wrapper

  return decorator


class AutoMLTablesHook(gcp_api_base_hook.GoogleCloudBaseHook):
  """Hook to call AutoML Tables API."""

  @staticmethod
  def extract_object_id(obj: GenericAutomlObject):
    """Returns unique id of the AutoML object."""
    return obj['name'].rpartition('/')[-1]

  @staticmethod
  def extract_object_project_id(obj: GenericAutomlObject):
    """Returns unique project id of the AutoML object."""
    return obj['name'].split('/')[1]

  def _get_authorized_session(self) -> requests.AuthorizedSession:
    """Get an authorized HTTP session for calling AutoML API.

    Returns:
      An AuthorizedSession object.
    """
    # Get credentials from airflow environment using the base hook
    credentials = self._get_credentials()
    return requests.AuthorizedSession(credentials=credentials)

  @gcp_api_base_hook.GoogleCloudBaseHook.fallback_to_default_project_id
  @retry_if_raises(times=_RETRY_ATTEMPTS)
  def create_model(
      self,
      model: GenericAutomlObject,
      location: str,
      project_id: Optional[str] = None,
      operation_timeout: Optional[int] = _DEFAULT_OPERATION_TIMEOUT,
      poll_wait_time: Optional[int] = _DEFAULT_POLL_SLEEP_TIME):
    """Creates a new AutoML model.

    Returns a Model in the `response` field when it
    completes. When you create a model, several model evaluations are
    created for it: a global evaluation, and one evaluation for each
    annotation spec.

    Args:
      model: Model definition. It must be of the same form as the protobuf
        message.
          See:https://cloud.google.com/automl/docs/reference/rest/v1beta1/projects.locations.models.
      location: The location of the project.
      project_id: ID of the Google Cloud project where model will be created if
        None then default project_id is used.
      operation_timeout: Maximum time(seconds) to wait for AutoML prediction to
        complete.
      poll_wait_time: time in seconds to sleep between polling predict operation
        status.

    Returns:
      Model creation result as a json dictionary from AutoML response.
        See:https://cloud.google.com/automl/docs/reference/rest/v1beta1/projects.locations.operations.

      Sample response:
      {
        "name":
        "projects/${PROJECT_NO}/locations/us-central1/operations/IOD5644417707978784777",
        "metadata": {
          "@type":
            "type.googleapis.com/google.cloud.automl.v1beta1.OperationMetadata",
          "createTime": "2018-10-24T22:08:23.327323Z",
          "updateTime": "2018-10-24T23:41:18.452855Z",
          "createModelDetails": {}
        },
        "done": true,
        "response": {
          "@type": "type.googleapis.com/google.cloud.automl.v1beta1.Model",
          "name":
            "projects/${PROJECT_NO}/locations/us-central1/models/IOD5644417707978784777"
        }
      }

    Raises:
      AutoMLApiError: When errors occurred calling AutoML API, or timeout
      waiting for model creation operation to complete.
    """
    url_suffix = 'models'
    submit_operation_response = self._make_api_call(
        method='POST',
        url_suffix=url_suffix,
        payload=model,
        project_id=project_id,
        compute_region=location)
    model_create_operation_done_response = self._wait_for_operation_done(
        submit_operation_response, operation_timeout, poll_wait_time)
    return model_create_operation_done_response

  @gcp_api_base_hook.GoogleCloudBaseHook.fallback_to_default_project_id
  @retry_if_raises(times=_RETRY_ATTEMPTS)
  def create_dataset(self,
                     dataset: GenericAutomlObject,
                     location: str,
                     project_id: Optional[str] = None):
    """Creates a dataset that can be used by the AutoML services.

    Args:
      dataset: The dataset to create. It must be of the same form as the
        protobuf message Dataset. See:
        https://cloud.google.com/automl/docs/reference/rest/v1beta1/projects.locations.datasets.
      location: The location of the project.
      project_id: ID of the Google Cloud project where dataset is located if
        None then default project_id is used.

    Returns:
      Dataset creation result as a json dictionary from AutoML response. See:
        https://cloud.google.com/automl/docs/reference/rest/v1beta1/projects.locations.datasets.

      Sample response:
        {
          "name": "projects/1234/locations/us-central1/datasets/TBL6543",
          "displayName": "sample_dataset",
          "createTime": "2019-12-23T23:03:34.139313Z",
          "updateTime": "2019-12-23T23:03:34.139313Z",
          "etag": "AB3BwFq6VkX64fx7z2Y4T4z-0jUQLKgFvvtD1RcZ2oikA=",
          "tablesDatasetMetadata": {
            "areStatsFresh": true
            "statsUpdateTime": "1970-01-01T00:00:00Z",
            "tablesDatasetType": "BASIC"
          }
        }

    Raises:
      AutoMLApiError: When errors occurred calling AutoML API, or timeout
      waiting for model creation operation to complete.
    """
    url_suffix = 'datasets'
    submit_operation_response = self._make_api_call(
        method='POST',
        url_suffix=url_suffix,
        payload=dataset,
        project_id=project_id,
        compute_region=location)
    return submit_operation_response

  @gcp_api_base_hook.GoogleCloudBaseHook.fallback_to_default_project_id
  @retry_if_raises(times=_RETRY_ATTEMPTS)
  def import_data(self,
                  dataset_id: str,
                  input_config: GenericAutomlObject,
                  location: str,
                  project_id: Optional[str] = None,
                  operation_timeout: Optional[int] = _DEFAULT_OPERATION_TIMEOUT,
                  poll_wait_time: Optional[int] = _DEFAULT_POLL_SLEEP_TIME):
    """Imports data into a dataset.

    For Tables this method can only be called on an empty Dataset.

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

    Returns:
      Operation completion status response. See:
      https://cloud.google.com/automl/docs/reference/rest/v1beta1/projects.locations.operations

      Sample response:
        {
          "name":
            "projects/${PROJECT_NO}/locations/us-central1/operations/IOD1555149246326374411",
          "metadata": {
            "@type":
              "type.googleapis.com/google.cloud.automl.v1beta1.OperationMetadata",
            "createTime": "2018-10-29T15:56:29.176485Z",
            "updateTime": "2018-10-29T16:10:41.326614Z",
            "importDataDetails": {}
          },
          "done": true,
          "response": {
            "@type": "type.googleapis.com/google.protobuf.Empty"
          }
        }

    Raises:
      AutoMLApiError: When errors occurred calling AutoML API, or timeout
      waiting for model creation operation to complete.
    """
    url_suffix = f'datasets/{dataset_id}:importData'
    payload = {'inputConfig': input_config}
    submit_operation_response = self._make_api_call(
        method='POST',
        url_suffix=url_suffix,
        payload=payload,
        project_id=project_id,
        compute_region=location)
    import_data_operation_done_response = self._wait_for_operation_done(
        submit_operation_response, operation_timeout, poll_wait_time)
    return import_data_operation_done_response

  @gcp_api_base_hook.GoogleCloudBaseHook.fallback_to_default_project_id
  @retry_if_raises(times=_RETRY_ATTEMPTS)
  def list_table_specs(self, dataset_id: str, project_id: str, location: str):
    """Lists table specs in a dataset_id.

    Args:
      dataset_id: Name of the AutoML dataset.
      project_id: ID of the Google Cloud project where dataset is located if
        None then default project_id is used.
      location: The location of the project.

    Returns:
      A list of instances of type TableSpec. See:
      https://cloud.google.com/automl/docs/reference/rest/v1beta1/projects.locations.datasets.tableSpecs#TableSpec

      Sample response:
        [
            {
              "name":
                "projects/292381/locations/us-central1/datasets/TBL6543/tableSpecs/370474",
              "rowCount": "1460",
              "validRowCount": "1460",
              "inputConfigs": [
                {
                  "gcsSource": {
                    "inputUris": [
                      "gs://datasets/housing_price.csv"
                    ]
                  }
                }
              ],
              "etag": "AB3BwFppc_H1J3MdRSzDs4nr_fgUUY1sz5g=",
              "columnCount": "81"
            }
        ]

    Raises:
      AutoMLApiError: When errors occurred calling AutoML API, or timeout
      waiting for model creation operation to complete.
    """
    url_suffix = f'datasets/{dataset_id}/tableSpecs'
    submit_operation_response = self._make_api_call(
        method='GET',
        url_suffix=url_suffix,
        payload=None,
        project_id=project_id,
        compute_region=location)
    return submit_operation_response['tableSpecs']

  @gcp_api_base_hook.GoogleCloudBaseHook.fallback_to_default_project_id
  @retry_if_raises(times=_RETRY_ATTEMPTS)
  def list_column_specs(self,
                        dataset_id: str,
                        table_spec_id: str,
                        location: str,
                        field_mask: Optional[Sequence[str]] = None,
                        project_id: Optional[str] = None):
    """Lists table specs in a dataset_id.

    Args:
      dataset_id: ID of the AutoML dataset.
      table_spec_id: ID of the AutoML table.
      location: The location of the project.
      field_mask:  Sequence of fully qualified names of fields.See:
        https://cloud.google.com/automl/docs/reference/rest/v1beta1/projects.locations.datasets.tableSpecs.columnSpecs/list
      project_id: ID of the Google Cloud project where dataset is located if
        None then default project_id is used.

    Returns:
      A list of instances of type ColumnSpec. See:
      https://cloud.google.com/automl/docs/reference/rest/v1beta1/projects.locations.datasets.tableSpecs.columnSpecs#ColumnSpec

      Sample response:
        [
            {
              "name":
                "projects/292381/locations/us-central1/datasets/TBL6543/tableSpecs/370474/columnSpecs/45948",
              "dataType": {
                "typeCode": "FLOAT64",
                "compatibleDataTypes": [
                  {
                    "typeCode": "FLOAT64"
                  },
                  {
                    "typeCode": "CATEGORY"
                  }
                ]
              },
              "displayName": "Id",
              "dataStats": {
              ...
              },
              "etag": "AB3BwFoaeD2X9CbCpGM8pWxNJ6S5L1_Rtnk="
            },
            ...
        ]

    Raises:
      AutoMLApiError: When errors occurred calling AutoML API, or timeout
      waiting for model creation operation to complete.
    """
    url_suffix = f'datasets/{dataset_id}/tableSpecs/{table_spec_id}/columnSpecs/'
    url_params = {}
    if field_mask:
      url_params['fieldMask'] = ','.join(field_mask)
    submit_operation_response = self._make_api_call(
        method='GET',
        url_suffix=url_suffix,
        payload=None,
        project_id=project_id,
        compute_region=location,
        url_params=url_params)
    return submit_operation_response['columnSpecs']

  @retry_if_raises(times=_RETRY_ATTEMPTS)
  def update_dataset(self, dataset: GenericAutomlObject, location: str):
    """Updates a dataset that can be used by the AutoML services.

    Args:
      dataset: The dataset parameters to update. It must be of the same form as
        the protobuf message Dataset. See:
        https://cloud.google.com/automl/docs/reference/rest/v1beta1/projects.locations.datasets.
          Example (to update the tablesDatasetMetadata field of a dataset)- {
            "tablesDatasetMetadata": {
              "mlUseColumnSpecId": "602945",
              "weightColumnSpecId": "602949" } }
      location: The location of the project.

    Returns:
      Dataset updation result as a json dictionary from AutoML response. See:
        https://cloud.google.com/automl/docs/reference/rest/v1beta1/projects.locations.datasets.

      Sample response:
        {
          "name": "projects/1234/locations/us-central1/datasets/TBL6543",
          "displayName": "sample_dataset",
          "createTime": "2019-12-23T23:03:34.139313Z",
          "updateTime": "2019-12-30T20:51:41.532594Z",
          "etag": "AB3BwFq6VkX64fx7z2Y4T4z-0jUQLKgFvvtD1RcZ2oikA=",
          "tablesDatasetMetadata": {
            "primaryTableSpecId": "370474",
            "mlUseColumnSpecId": "602945",
            "weightColumnSpecId": "602949"
          }
        }

    Raises:
      AutoMLApiError: When errors occurred calling AutoML API, or timeout
      waiting for model creation operation to complete.
    """
    dataset_id = self.extract_object_id(dataset)
    project_id = self.extract_object_project_id(dataset)
    url_suffix = f'datasets/{dataset_id}'
    submit_operation_response = self._make_api_call(
        method='PATCH',
        url_suffix=url_suffix,
        payload=dataset,
        project_id=project_id,
        compute_region=location)
    return submit_operation_response

  @gcp_api_base_hook.GoogleCloudBaseHook.fallback_to_default_project_id
  @retry_if_raises(times=_RETRY_ATTEMPTS)
  def batch_predict(
      self,
      model_id: str,
      input_config: str,
      output_config: str,
      project_id: str,
      location: str,
      operation_timeout: Optional[int] = _DEFAULT_OPERATION_TIMEOUT,
      poll_wait_time: Optional[int] = _DEFAULT_POLL_SLEEP_TIME):
    """Perform a batch prediction.

    Unlike the online `Predict`, batch prediction result won't be immediately
    available in the response. So in this method, the status of the operation is
    polled for operation timeout secs and then either the method returns
    successfully or an error is raised in case of a timeout.

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

    Returns:
      Prediction results json dictionary from AutoML response. See:
      https://cloud.google.com/automl-tables/docs/reference/rest/v1beta1/projects.locations.operations#Operation

      Sample response:
      {
        "name": ...,
        "done": true,
        "metadata": {
          "batchPredictDetails": {
            "inputConfig": {
              ...
            },
            "outputInfo": {
              "gcsOutputDirectory": "gs://output-dir" |
              "bigqueryOutputDataset": "bq://output-dataset"
            }
          }
        }
      }

    Raises:
      AutoMLApiError: When errors occurred calling AutoML API, or timeout
      waiting for prediction operation to complete.
    """
    url_suffix = f'models/{model_id}:batchPredict'
    payload = {'inputConfig': input_config, 'outputConfig': output_config}
    submit_operation_response = self._make_api_call(
        method='POST',
        url_suffix=url_suffix,
        payload=payload,
        project_id=project_id,
        compute_region=location)
    model_create_operation_done_response = self._wait_for_operation_done(
        submit_operation_response, operation_timeout, poll_wait_time)
    return model_create_operation_done_response

  @retry_if_raises(times=_RETRY_ATTEMPTS)
  def _make_api_call(self,
                     method: str,
                     url_suffix: str,
                     project_id: str,
                     compute_region: str,
                     payload: Optional[Dict[str, Any]] = None,
                     url_params: Optional[Dict[str, str]] = None):
    """Makes an authenticated api call to the AutoML services.

    Args:
      method: The HTTP method to be used while making the API call. eg.'GET',
        'POST' etc.
      url_suffix: The specific rest resource being called, leaving out the
        parent name.
      project_id: ID of the Google Cloud project where model will be created.
      compute_region: The location of the project.
      payload: The payload to be sent in the API call.
      url_params: Query string parameters to be sent in the url.

    Returns:
      The json response body.

    Raises:
      AutoMLApiError: When errors occurred calling the AutoML API.
    """
    session = self._get_authorized_session()
    url = (f'{_AUTOML_BASE_URL}/projects/{project_id}'
           f'/locations/{compute_region}/{url_suffix}')
    response = session.request(method, url, json=payload, params=url_params)
    if response.status_code != http.HTTPStatus.OK:
      raise AutoMLApiError(f'Error calling AutoML API url={url} status='
                           f'{response.status_code} message={response.text}')
    res = response.json()
    if 'error' in res:
      raise AutoMLApiError(f'Error calling AutoML API url={url} status='
                           f'{response.status_code} error={res["error"]}')
    return res

  @retry_if_raises(times=_RETRY_ATTEMPTS)
  def _wait_for_operation_done(self,
                               operation: Dict[str, Any],
                               operation_timeout=_DEFAULT_OPERATION_TIMEOUT,
                               poll_wait_time=_DEFAULT_POLL_SLEEP_TIME):
    """Waits for an operation to complete by polling the Operation Status API.

    It polls the status api every `poll_wait_time` seconds. It returns when the
    prediction task is finished. It raises an error when the total time polling
    exceeds operation_timeout or an error occurred when calling the AutoML API.
    See:
    https://cloud.google.com/automl-tables/docs/long-operations
    https://cloud.google.com/automl/docs/reference/rest/v1beta1/projects.locations.operations/get

    Args:
      operation: The operation instance to be polled. It must be of the same
        form as
        protobuf message Operation. See:
        https://cloud.google.com/automl/docs/reference/rest/v1beta1/projects.locations.operations.
      operation_timeout: Maximum time(seconds) to wait for AutoML prediction to
        complete.
      poll_wait_time: time in seconds to sleep between polling predict operation
        status.

    Returns:
      Operation completion result as a json dictionary from AutoML response.
      See:
      https://cloud.google.com/automl/docs/reference/rest/v1beta1/projects.locations.operations.

      Sample response:
        {
          "name": "projects/1234/locations/us-central1/operations/TBL2126",
          "metadata": {
        ...
          },
          "done": true,
          "response": {
        ...
          }
        }

    Raises:
      AutoMLApiError: When errors occurred calling AutoML API, or timeout
      waiting for operation to complete.
    """
    operation_name = operation['name']
    session = self._get_authorized_session()
    # Wait for prediction results
    start_time = time.time()
    while (time.time() - start_time) < operation_timeout:
      url = f'{_AUTOML_BASE_URL}/{operation_name}'
      response = session.get(url)
      if response.status_code != http.HTTPStatus.OK:
        raise AutoMLApiError('Error waiting for AutoML operation status='
                             f'{response.status_code} message={response.text}')
      res = response.json()
      self.log.info('batch predict status: %s', res)
      if res.get('done', False):
        if 'error' in res:
          raise AutoMLApiError(f'Error in operation result {res["error"]}')
        # All ok, return the json response
        return res
      time.sleep(poll_wait_time)
    time_wait = time.time() - start_time
    raise AutoMLApiError(
        f'Timeout waiting {time_wait}s for AutoML operation:{operation_name}.')
