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

"""Custom operator for deleting objects from GCS.

TODO:(b/151696655): This module needs to be removed as gcs_delete_operator
becomes available in g3 - //third_party/py/airflow/path/to/operators directory
"""

from typing import Optional
from airflow import models
from airflow.contrib.hooks import gcs_hook
from airflow.utils.decorators import apply_defaults


class GoogleCloudStorageDeleteOperator(models.BaseOperator):
  """Deletes a GCS directory."""

  @apply_defaults
  def __init__(self,
               *args,
               bucket: str,
               directory: str,
               google_cloud_storage_conn_id: str = 'google_cloud_default',
               delegate_to: Optional[str] = None,
               **kwargs):
    """Initializes a GoogleCloudStorageDeleteOperator.

    Args:
      *args: Additional args.
      bucket: GCS bucket of model.
      directory: Directory in the bucket.
      google_cloud_storage_conn_id: The connection ID to use when fetching
        connection info.
      delegate_to: The account to impersonate, if any. For this to work,
        the service account making the request must have domain-wide delegation
        enabled.
      **kwargs: Additional keyword args.
    """
    super(GoogleCloudStorageDeleteOperator, self).__init__(*args, **kwargs)
    self._bucket = bucket
    self._directory = directory

    self._google_cloud_storage_conn_id = google_cloud_storage_conn_id
    self._delegate_to = delegate_to

  # pytype: disable=attribute-error
  def execute(self, context):
    hook = gcs_hook.GoogleCloudStorageHook(
        google_cloud_storage_conn_id=self._google_cloud_storage_conn_id,
        delegate_to=self._delegate_to)

    objects = hook.list(self._bucket, prefix=self._directory)

    for obj in objects:
      if hook.exists(self._bucket, obj) and not hook.delete(self._bucket, obj):
        raise RuntimeError('Deleting object %s failed.' % obj)
  # pytype: enable=attribute-error
