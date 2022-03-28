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

"""Custom hook for Google Ads Customer Match.

For customer match details refer to
https://developers.google.com/google-ads/api/docs/remarketing/audience-types/customer-match
"""
from typing import Any, Dict, Generator, List, Tuple

from dependencies.tcrm.hooks import ads_hook_v2
from dependencies.tcrm.hooks import output_hook_interface
from dependencies.tcrm.utils import blob as blob_lib
from dependencies.tcrm.utils import errors
from dependencies.tcrm.utils import type_alias


class GoogleAdsCustomerMatchHook(
    ads_hook_v2.GoogleAdsHook, output_hook_interface.OutputHookInterface):
  """Custom hook to upload customer match user list to Google Ads via Google Ads API."""

  def __init__(self,
               google_ads_yaml_credentials: str,
               ads_cm_user_list_name: str,
               ads_upload_key_type: str,
               ads_cm_create_list: bool = True,
               ads_cm_app_id: str = '',
               ads_cm_membership_lifespan_in_days: int = ads_hook_v2
               .DEFAULT_MEMBERSHIP_LIFESPAN_DAYS,
               **kwargs) -> None:
    """Initialize with a specified user_list_name.

    Args:
      google_ads_yaml_credentials: A YAML string for authenticating to Ads API.
        Reference for desired format:
          https://developers.google.com/google-ads/api/docs/client-libs/python/configuration#configuration_fields
      ads_cm_user_list_name: The name of the user list to add members to.
      ads_upload_key_type: The upload key type.
      ads_cm_create_list: The flag that indicates if new list should be created
        if no existing list with given user_list_name.
      ads_cm_app_id: Mobile app id for creating new user list when the
        upload_key_type is MOBILE_ADVERTISING_ID.
      ads_cm_membership_lifespan_in_days: NNumber of days a user's cookie stays.
      **kwargs: Other optional arguments.

    Raises:
      DataOutConnectorValueError if any of the following happens.
        - user_list_name is null.
        - membership_lifespan is negative or bigger than 10000.
        - upload_key_type is not supported by ads_hook_v2.
          - app_id is not specificed when create_list = True and upload_key_type
            is MOBILE_ADVERTISING_ID.
    """
    super().__init__(
        google_ads_yaml_credentials=google_ads_yaml_credentials, **kwargs)

    self._validate_init_params(ads_cm_user_list_name,
                               ads_cm_membership_lifespan_in_days,
                               ads_upload_key_type,
                               ads_cm_create_list,
                               ads_cm_app_id)

    self.user_list_name = ads_cm_user_list_name.strip()
    self.membership_lifespan = ads_cm_membership_lifespan_in_days
    self.upload_key_type = ads_upload_key_type.strip()
    self.create_list = ads_cm_create_list
    self.app_id = ads_cm_app_id.strip()

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
        self.upload_customer_match_user_list(
            customer_id=customer_id,
            user_list_name=self.user_list_name,
            create_new_list=self.create_list,
            payloads=batch,
            upload_key_type=self.upload_key_type,
            app_id=self.app_id,
            membership_life_span_days=self.membership_lifespan)
      except errors.DataOutConnectorError as error:
        for event in batch:
          event_index = event[0]
          invalid_indices_and_errors.append((event_index, error.error_num))

    for event in invalid_indices_and_errors:
      event_index = event[0]
      error_num = event[1].value
      blob.append_failed_event(event_index + blob.position,
                               blob.events[event_index],
                               error_num)

    return blob

  def _validate_init_params(
      self, user_list_name: str,
      membership_lifespan: int,
      upload_key_type: str,
      create_list: bool,
      app_id: str
  ) -> None:
    """Validates user_list_name and membership_lifespan parameters.

    Args:
      user_list_name: The name of the user list to add members to.
      membership_lifespan: Number of days a user's cookie stays.
      upload_key_type: The upload key type.
      create_list: A flag to enable a new list creation if a list called
        user_list_name doesn't exist.
      app_id: The mobile app id for creating new user list when the
        upload_key_type is MOBILE_ADVERTISING_ID.

    Raises:
      DataOutConnectorValueError if any init argument is invalid.
    """
    if not user_list_name:
      raise errors.DataOutConnectorValueError(
          'User list name is empty.',
          errors.ErrorNameIDMap.ADS_CM_HOOK_ERROR_EMPTY_USER_LIST_NAME)

    if membership_lifespan < 0 or (membership_lifespan > 540 and
                                   membership_lifespan != 10000):
      raise errors.DataOutConnectorValueError(
          ('Membership lifespan in days should be between 0 and 540 inclusive '
           'or 10000 for no expiration.'),
          errors.ErrorNameIDMap.ADS_CM_HOOK_ERROR_INVALID_MEMBERSHIP_LIFESPAN)

    if upload_key_type not in (
        ads_hook_v2.CUSTOMER_MATCH_UPLOAD_KEY_CONTACT_INFO,
        ads_hook_v2.CUSTOMER_MATCH_UPLOAD_KEY_MOBILE_ADVERTISING_ID,
        ads_hook_v2.CUSTOMER_MATCH_UPLOAD_KEY_CRM_ID):
      raise errors.DataOutConnectorValueError(
          ('Invalid upload key type. Valid upload key type: CONTACT_INFO, '
           'MOBILE_ADVERTISING_ID or CRM_ID'),
          errors.ErrorNameIDMap.ADS_CM_HOOK_ERROR_INVALID_UPLOAD_KEY_TYPE)

    if (upload_key_type
        == ads_hook_v2.CUSTOMER_MATCH_UPLOAD_KEY_MOBILE_ADVERTISING_ID and
        create_list and not app_id):
      raise errors.DataOutConnectorValueError(
          'app_id needs to be specified for '
          'MOBILE_ADVERTISING_ID when create_list is True.',
          errors.ErrorNameIDMap.ADS_CM_HOOK_ERROR_MISSING_APPID)

  def _generate_batches(
      self,
      events: List[Dict[str, Any]]
  ) -> Generator[Tuple[str, List[type_alias.Payload]], None, None]:
    """Creates a batch of events that grouped by a customer_id.

    Args:
      events: Customer match user list to send.

    Yields:
      A batch of events that grouped by a customer_id
    """
    batches = {}
    for index, event in enumerate(events):
      customer_id = event[ads_hook_v2.CUSTOMER_ID]
      batches.setdefault(customer_id, []).append((index, event))
    for customer_id in batches:
      yield customer_id, batches[customer_id]
