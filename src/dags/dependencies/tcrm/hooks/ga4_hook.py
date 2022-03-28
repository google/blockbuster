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
"""Custom hook for Google Analytics 4 Measurement Protocol 4 API.

Details for Measurement Protocol (Google Analytics 4).
https://developers.google.com/analytics/devguides/collection/protocol/ga4/reference

"""

import enum
import json
import logging
import typing
from typing import Any, Dict, List, Tuple

import immutabledict
import requests

from dependencies.tcrm.hooks import output_hook_interface
from dependencies.tcrm.utils import blob as blob_lib
from dependencies.tcrm.utils import errors

_GA_EVENT_POST_URL = 'https://www.google-analytics.com/mp/collect'
_GA_EVENT_VALIDATION_URL = 'https://www.google-analytics.com/debug/mp/collect'

_ERROR_TYPES = immutabledict.immutabledict({
    'client_id':
        errors.ErrorNameIDMap.GA4_HOOK_ERROR_VALUE_REQUIRED_CLIENT_ID,
    'user_id':
        errors.ErrorNameIDMap.GA4_HOOK_ERROR_VALUE_INVALID_USER_ID,
    'timestamp_micros':
        errors.ErrorNameIDMap.GA4_HOOK_ERROR_VALUE_INVALID_TIMESTAMP_MICROS,
    'user_properties':
        errors.ErrorNameIDMap.GA4_HOOK_ERROR_VALUE_INVALID_USER_PROPERTIES,
    'non_personalized_ads':
        errors.ErrorNameIDMap.GA4_HOOK_ERROR_VALUE_INVALID_NON_PERSONALIZED_ADS,
    'events':
        errors.ErrorNameIDMap.GA4_HOOK_ERROR_VALUE_INVALID_EVENTS,
    'events.params':
        errors.ErrorNameIDMap.GA4_HOOK_ERROR_VALUE_INVALID_EVENTS_PARAMS,
    'events.params.items':
        errors.ErrorNameIDMap.GA4_HOOK_ERROR_VALUE_INVALID_EVENTS_PARAMS_ITEMS,
})


class PayloadTypes(enum.Enum):
  """GA4 Measurememt Protocol supported payload types."""

  FIREBASE = 'firebase'
  GTAG = 'gtag'


class GoogleAnalyticsV4Hook(output_hook_interface.OutputHookInterface):
  """Custom hook for Measurement Protocol (Google Analytics 4)."""

  def __init__(self,
               api_secret: str,
               payload_type: str,
               measurement_id: typing.Optional[str] = None,
               firebase_app_id: typing.Optional[str] = None,
               dry_run: typing.Optional[bool] = False,
               **kwargs) -> None:
    self.dry_run = dry_run
    self.api_secret = api_secret
    self.payload_type = payload_type
    self.measurement_id = measurement_id
    self.firebase_app_id = firebase_app_id
    self._validate_credentials()
    self.post_url = self._build_api_url(True)
    self.validate_url = self._build_api_url(False)

  def _validate_credentials(self) -> None:
    """Validate credentials.

    Raises:
      DataOutConnectorValueError: If credential combination does not meet
        criteria.
    """
    if not self.api_secret:
      raise errors.DataOutConnectorValueError(
          'Missing api secret. Please check you have api_secret set as a Cloud '
          'Composer variable as specified in the TCRM documentation.')

    valid_payload_types = (PayloadTypes.FIREBASE.value, PayloadTypes.GTAG.value)
    if self.payload_type not in valid_payload_types:
      raise errors.DataOutConnectorValueError(
          f'Unsupport payload_type: {self.payload_type}. Supported '
          'payload_type is gtag or firebase.')

    if (self.payload_type == PayloadTypes.FIREBASE.value and
        not self.firebase_app_id):
      raise errors.DataOutConnectorValueError(
          'Wrong payload_type or missing firebase_app_id. Please make sure '
          'firebase_app_id is set when payload_type is firebase.')

    if (self.payload_type == PayloadTypes.GTAG.value and
        not self.measurement_id):
      raise errors.DataOutConnectorValueError(
          'Wrong payload_type or missing measurement_id. Please make sure '
          'measurement_id is set when payload_type is gtag.')

  def _build_api_url(self, is_post: bool) -> str:
    """Builds the url for sending the payload.

    Args:
      is_post: true for building post url, false for building validation url.
    Returns:
      url: Full url that can be used for sending the payload
    """

    if self.payload_type == PayloadTypes.GTAG.value:
      query_url = 'api_secret={}&measurement_id={}'.format(
          self.api_secret, self.measurement_id)
    else:
      query_url = 'api_secret={}&firebase_app_id={}'.format(
          self.api_secret, self.firebase_app_id)
    if is_post:
      built_url = f'{_GA_EVENT_POST_URL}?{query_url}'
    else:
      built_url = f'{_GA_EVENT_VALIDATION_URL}?{query_url}'
    return built_url

  def _ensure_payload_is_dict(self, event: Dict[str, Any]) -> None:
    """Ensures the event contains key of payload and the value of payload is dict.

       When the event is loaded from BigQuery, the value of payload is string,
       whereas the value of payload is dict when event is loaded from Google
       Storage because the raw data is stored as JSON files.

    Args:
      event: Event to ensure.

    Returns:
      The same event as input that is ensured.
    """
    if 'payload' not in event:
      raise errors.DataOutConnectorValueError(
          error_num=errors.ErrorNameIDMap
          .GA4_HOOK_ERROR_MISSING_PAYLOAD_IN_EVENT)

    payload = event['payload']

    # Payload is from Google Cloud Storage.
    if isinstance(payload, dict):
      return

    try:
      # Payload is coming from BQ and must be converted to a dictionary
      event['payload'] = json.loads(payload)
    except json.decoder.JSONDecodeError:
      raise errors.DataOutConnectorValueError(
          error_num=errors.ErrorNameIDMap.GA4_HOOK_ERROR_INVALID_JSON_STRUCTURE)

  def _send_validate_request(self, payload: Dict[str,
                                                 Any]) -> requests.Response:
    """Sends the GA4 payload to the debug API for data validating.

       By adding the key-value pair
       (validationBehavior: ENFORCE_RECOMMENDATIONS), the API will check the
       payload thoroughly, this is recommended because the Measurement Protocol
       API won't check the data and it fails silently, you might not know what
       happened to your data.

    Args:
       payload: the JSON payload of the GA4 event.

    Returns:
       The response from the debug API.
    """
    validating_payload = dict(payload)
    validating_payload['validationBehavior'] = 'ENFORCE_RECOMMENDATIONS'
    try:
      response = requests.post(self.validate_url, json=validating_payload)
    except requests.ConnectionError:
      raise errors.DataOutConnectorValueError(
          error_num=errors.ErrorNameIDMap.RETRIABLE_GA4_HOOK_ERROR_HTTP_ERROR)

    return response

  def _parse_validate_result(self, event: Dict[str, Any],
                             response: requests.Response):
    """Parses the response returned from the debug API.

       The response body contains a JSON to indicate the validated result.
       For example:
       {
         "validationMessages": [
          {
            "fieldPath": "timestamp_micros"
            "description": "Measurement timestamp_micros has timestamp....",
            "validationCode": "VALUE_INVALID"
          }]
       }

       The fieldPath indicates which part of your payload JSON contains invalid
       value, when fieldPath doesn't exist, the fieldPath can be found in
       description as well.

    Args:
      event: The event that contains the index and the payload.
      response: The HTTP response from the debug API.
    """
    if response.status_code >= 500:
      raise errors.DataOutConnectorValueError(
          error_num=errors.ErrorNameIDMap.RETRIABLE_GA4_HOOK_ERROR_HTTP_ERROR)
    elif response.status_code != 200:
      raise errors.DataOutConnectorValueError(
          error_num=errors.ErrorNameIDMap.NON_RETRIABLE_ERROR_EVENT_NOT_SENT)

    try:
      validation_result = response.json()
    except json.JSONDecodeError:
      raise errors.DataOutConnectorValueError(
          error_num=errors.ErrorNameIDMap.RETRIABLE_GA4_HOOK_ERROR_HTTP_ERROR
          .value)

    # Payload is valid: validation messages are only returned if there is a
    # problem with the payload.
    if not validation_result['validationMessages']:
      return

    # The validation API only ever returns one message.
    message = validation_result['validationMessages'][0]
    field_path = message['fieldPath']
    description = message['description']

    for property_name in _ERROR_TYPES:
      if field_path == property_name or property_name in description:
        raise errors.DataOutConnectorValueError(
            error_num=_ERROR_TYPES[property_name])

    # Prevent from losing error message if it is undefined due to API change.
    logging.error('id: %s, fieldPath: %s, description: %s', event['id'],
                  message['fieldPath'], message['description'])
    raise errors.DataOutConnectorValueError(
        error_num=errors.ErrorNameIDMap.GA4_HOOK_ERROR_INVALID_VALUES)

  def _get_valid_and_invalid_events(
      self, events: List[Dict[str, Any]]
  ) -> Tuple[List[Tuple[int, Dict[str, Any]]], List[Tuple[
      int, errors.ErrorNameIDMap]]]:
    """Prepares index-event tuples to keep order while sending.

       Validation of the payload is done by posting it to the validation API
       provided by the product of Google Analytics 4

       A valid payload format in event should comply with rules described below.
       https://developers.google.com/analytics/devguides/collection/protocol/ga4/reference/events

       For the limitations of the payload, please refer to:
       https://developers.google.com/analytics/devguides/collection/protocol/ga4/sending-events?client_type=gtag#limitations

    Args:
      events: Events to prepare for sending.

    Returns:
      A list of index-event tuples for the valid events, and a list of
      index-error for the invalid events.
    """

    valid_events = []
    invalid_indices_and_errors = []

    for i, event in enumerate(events):
      try:
        self._ensure_payload_is_dict(event)
        response = self._send_validate_request(event['payload'])
        self._parse_validate_result(event, response)
        valid_events.append((i, event))
      except errors.DataOutConnectorValueError as error:
        invalid_indices_and_errors.append((i, error.error_num))

    return valid_events, invalid_indices_and_errors

  def _send_payload(
      self, payload: Dict[str, Any]) -> None:
    """Sends payload to GA via Measurement Protocol REST API.

    Args:
      payload: Parameters containing required data for app conversion tracking.

    Returns:
      results: Includes request body, status_code, error_msg, response body and
      dry_run flag.
      The response refers to the definition of conversion tracking response in
      https://developers.google.com/analytics/devguides/collection/protocol/ga4/reference?client_type=firebase
    """

    if self.dry_run:
      self.log.info(
          """Dry run mode: Dry run mode: Simulating sending event to GA4 (data
          will not actually be sent). URL:{}. payload data:{}.""".format(
              self.post_url, payload))
      return

    try:
      response = requests.post(self.post_url, json=payload)
      if response.status_code < 200 or response.status_code >= 300:
        raise errors.DataOutConnectorSendUnsuccessfulError(
            msg='Sending payload to GA did not complete successfully.',
            error_num=errors.ErrorNameIDMap.RETRIABLE_GA_HOOK_ERROR_HTTP_ERROR)
    except requests.ConnectionError:
      raise errors.DataOutConnectorSendUnsuccessfulError(
          msg='Sending payload to GA did not complete successfully.',
          error_num=errors.ErrorNameIDMap.RETRIABLE_GA_HOOK_ERROR_HTTP_ERROR)

  def send_events(self, blob: blob_lib.Blob) -> blob_lib.Blob:
    """Sends all events in the blob to the GA API.

    Args:
      blob: A blob containing Google Analytics data to send.

    Returns:
      A blob containing updated data about any failing events or reports.
    """
    valid_events, invalid_indices_and_errors = (
        self._get_valid_and_invalid_events(blob.events))

    for valid_event in valid_events:
      try:
        event = valid_event[1]
        self._send_payload(event['payload'])
      except (errors.DataOutConnectorSendUnsuccessfulError,
              errors.DataOutConnectorValueError) as error:
        index = valid_event[0]
        invalid_indices_and_errors.append((index, error.error_num))

    for invalid_event in invalid_indices_and_errors:
      event_index = invalid_event[0]
      error_num = invalid_event[1]
      blob.append_failed_event(event_index + blob.position,
                               blob.events[event_index], error_num.value)

    return blob
