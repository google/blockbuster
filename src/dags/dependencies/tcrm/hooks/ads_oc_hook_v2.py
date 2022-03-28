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

# python3
# coding=utf-8
# Copyright 2022 Google LLC.
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

"""Custom Hook for sending offline click conversions to Google Ads."""

from typing import Any, Dict, List, Tuple, Generator

from dependencies.tcrm.hooks import ads_hook_v2
from dependencies.tcrm.hooks import output_hook_interface
from dependencies.tcrm.utils import blob as blob_lib
from dependencies.tcrm.utils import errors
from dependencies.tcrm.utils import type_alias


class GoogleAdsOfflineConversionsHook(
    ads_hook_v2.GoogleAdsHook, output_hook_interface.OutputHookInterface):
  """Custom hook to send offline click conversions to Google Ads via Google Ads API."""

  def __init__(self, google_ads_yaml_credentials: str, **kwargs) -> None:
    """Initialises the class.

    Args:
      google_ads_yaml_credentials: A YAML string for authenticating to Ads API.
        Reference for desired format:
          https://developers.google.com/google-ads/api/docs/client-libs/python/configuration#configuration_fields
      **kwargs: Other optional arguments.
    """
    super().__init__(
        google_ads_yaml_credentials=google_ads_yaml_credentials, **kwargs)

  def send_events(self, blob: blob_lib.Blob) -> blob_lib.Blob:
    """Sends all events to Google Ads.

    Args:
      blob: A blob containing Customer Match data to send.

    Returns:
      A blob containing updated data about any failing events or reports.
    """
    invalid_indices_and_errors = []
    batches = self._generate_batches(blob.events)

    for customer_id, batch in batches:
      try:
        invalid_events = self.upload_click_conversions(customer_id, batch)
      except errors.DataOutConnectorError as error:
        for event in batch:
          event_index = event[0]
          invalid_indices_and_errors.append((event_index, error.error_num))
      else:
        invalid_indices_and_errors.extend(invalid_events)

    for event in invalid_indices_and_errors:
      event_index = event[0]
      error_num = event[1].value
      blob.append_failed_event(event_index + blob.position,
                               blob.events[event_index],
                               error_num)

    return blob

  def _generate_batches(
      self,
      events: List[Dict[str, Any]]
  ) -> Generator[Tuple[str, List[type_alias.Payload]], None, None]:
    """Creates a batch of events grouped by a customer_id.

    Args:
      events: Offline click conversion events to send.

    Yields:
      A batch of events grouped by a customer_id
    """
    batches = {}
    for index, event in enumerate(events):
      customer_id = event[ads_hook_v2.CUSTOMER_ID]
      batches.setdefault(customer_id, []).append((index, event))
    for customer_id in batches:
      yield customer_id, batches[customer_id]
