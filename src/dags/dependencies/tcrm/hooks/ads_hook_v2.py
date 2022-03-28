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

"""Custom hook for Google Ads API."""
import json
from typing import Any, Dict, List, Optional

from airflow.hooks import base_hook
import requests
import tenacity
import yaml

from dependencies.tcrm.utils import errors
from dependencies.tcrm.utils import type_alias

DEFAULT_MEMBERSHIP_LIFESPAN_DAYS = 8
DEFAULT_CURRENCY_CODE = 'JPY'

GOOGLE_ADS_API_BASE_URL = 'https://googleads.googleapis.com'

SEARCH_URL = (GOOGLE_ADS_API_BASE_URL + '/{api_version}'
              '/customers/{customer_id}/googleAds:search')

USER_LIST_MUTATE_URL = (GOOGLE_ADS_API_BASE_URL + '/{api_version}'
                        '/customers/{customer_id}/userLists:mutate')

OFFLINE_USER_DATA_JOBS_CREATE_URL = (
    GOOGLE_ADS_API_BASE_URL + '/{api_version}'
    '/customers/{customer_id}/offlineUserDataJobs:create')

USER_ADD_OPERATION_URL = (GOOGLE_ADS_API_BASE_URL + '/{api_version}'
                          '/{resource_name}:addOperations')

OFFLINE_USER_DATA_JOBS_RUN_URL = (GOOGLE_ADS_API_BASE_URL + '/{api_version}'
                                  '/{resource_name}:run')

# https://developers.google.com/google-ads/api/reference/rpc/v10/CustomerMatchUploadKeyTypeEnum.CustomerMatchUploadKeyType
# There are many types of user list Google Ads provides for customer match.
# TCRM supports the CrmBasedUserList which has 3 sub types.

# Members are matched from customer info such as email address and so on.
CUSTOMER_MATCH_UPLOAD_KEY_CONTACT_INFO = 'CONTACT_INFO'
# Members are matched from a user id generated and assigned by the advertiser.
CUSTOMER_MATCH_UPLOAD_KEY_CRM_ID = 'CRM_ID'
# Members are matched from mobile advertising id
CUSTOMER_MATCH_UPLOAD_KEY_MOBILE_ADVERTISING_ID = 'MOBILE_ADVERTISING_ID'

UPLOAD_KEY_TYPE = {
    CUSTOMER_MATCH_UPLOAD_KEY_CONTACT_INFO:
        CUSTOMER_MATCH_UPLOAD_KEY_CONTACT_INFO,
    CUSTOMER_MATCH_UPLOAD_KEY_CRM_ID:
        CUSTOMER_MATCH_UPLOAD_KEY_CRM_ID,
    CUSTOMER_MATCH_UPLOAD_KEY_MOBILE_ADVERTISING_ID:
        CUSTOMER_MATCH_UPLOAD_KEY_MOBILE_ADVERTISING_ID
}

CUSTOMER_ID = 'customerId'

# The record of CrmBasedUserList with type CONTACT_INFO can be one of the fields
# hashedEmail, hashedPhoneNumber, addressInfo (object)
HASHED_EMAIL = 'hashedEmail'
HASHED_PHONE_NUMBER = 'hashedPhoneNumber'

ADDRESS_INFO = 'addressInfo'
HASHED_FIRST_NAME = 'hashedFirstName'
HASHED_LAST_NAME = 'hashedLastName'
COUNTRY_CODE = 'countryCode'
POSTAL_CODE = 'postalCode'

ADDRESS_INFO_FIELDS = {
    HASHED_FIRST_NAME: HASHED_FIRST_NAME,
    HASHED_LAST_NAME: HASHED_LAST_NAME,
    COUNTRY_CODE: COUNTRY_CODE,
    POSTAL_CODE: POSTAL_CODE
}

# The record of CrmBasedUserList with type CONTACT_INFO can only be
# thirdPartyUserId
THIRD_PARTY_USER_ID = 'thirdPartyUserId'

# The record of CrmBasedUserList with type MOBILE_ADVERTISING_ID can only be
# mobileId
MOBILE_ID = 'mobileId'

USER_IDENTIFIER_FIELDS = {
    CUSTOMER_MATCH_UPLOAD_KEY_CONTACT_INFO: {
        HASHED_EMAIL: HASHED_EMAIL,
        HASHED_PHONE_NUMBER: HASHED_PHONE_NUMBER
    },
    CUSTOMER_MATCH_UPLOAD_KEY_CRM_ID: {
        THIRD_PARTY_USER_ID: THIRD_PARTY_USER_ID
    },
    CUSTOMER_MATCH_UPLOAD_KEY_MOBILE_ADVERTISING_ID: {
        MOBILE_ID: MOBILE_ID
    }
}

# https://developers.google.com/google-ads/api/rest/reference/rest/v10/customers/uploadClickConversions
CONVERSION_ACTION_RESOURCE = ('customers/{customer_id}/conversionActions/'
                              '{conversion_action_id}')

UPLOAD_CLICK_CONVERSIONS_URL = (
    GOOGLE_ADS_API_BASE_URL + '/{api_version}'
                              '/customers/{customer_id}:uploadClickConversions')

# https://developers.google.com/google-ads/api/rest/reference/rest/v10/customers/uploadClickConversions#ClickConversion
CONVERSION_ACTION = 'conversionAction'
CONVERSION_ACTION_ID = 'conversionActionId'
GCLID = 'gclid'
CONVERSION_DATE_TIME = 'conversionDateTime'
CONVERSION_VALUE = 'conversionValue'
CURRENCY_CODE = 'currencyCode'

CLICK_CONVERSION_FIELDS = {
    GCLID: GCLID,
    CONVERSION_DATE_TIME: CONVERSION_DATE_TIME,
    CONVERSION_VALUE: CONVERSION_VALUE,
    CURRENCY_CODE: CURRENCY_CODE
}

_API_VERSION = '10'
_MAX_RETRIES = 3
_LOG_FORMAT = 'blob_index=%s field=%s error_code=%s message=%s'

_OAUTH2_TOKEN_URL = 'https://www.googleapis.com/oauth2/v3/token'

_CREDENTIAL_REQUIRED_KEYS = ('developer_token', 'client_id', 'client_secret',
                             'refresh_token', 'login_customer_id',)

_PARTIAL_FAILURE = 'partialFailure'


class GoogleAdsHook(base_hook.BaseHook):
  """Custom hook for Google Ads API.

  The original ads_hook module used the AdWords API, which is being deprecated.
  """

  def __init__(
      self,
      google_ads_yaml_credentials: str,
      api_version: str = _API_VERSION,
      **kwargs
  ) -> None:
    """Initializes an Google Ads client with specified configurations.

    Args:
      google_ads_yaml_credentials: A dict with Credentials for Google Ads
        Authentication.
      api_version: The Google Ads API version.
      **kwargs: Other optional arguments.
    """

    self.api_version = f'v{api_version}'
    self.config_data = yaml.safe_load(google_ads_yaml_credentials) or {}

    # The access_token be lazily loaded and cached when the API is called to
    # ensure token is fresh and won't be retrieved repeatedly.
    self.access_token = None

    self._validate_credential(self.config_data)

  @tenacity.retry(
      retry=tenacity.retry_if_exception_type(
          errors.DataOutConnectorSendUnsuccessfulError),
      wait=tenacity.wait.wait_exponential(max=_MAX_RETRIES),
      stop=tenacity.stop.stop_after_attempt(_MAX_RETRIES),
      reraise=True)
  def upload_click_conversions(
      self,
      customer_id: str,
      payloads: List[type_alias.Payload]
  ) -> List[type_alias.PayloadError]:
    """Uploads offline click conversions.

    Args:
      customer_id: The Ads customer ID where the offline conversions are
        uploaded.
      payloads: The offline click conversions to upload.

    Returns:
      The index and error code of the events that failed to upload.

    Raises:
      DataOutConnectorError raised when errors occurred.
    """

    try:
      conversions = []

      for _, payload in payloads:
        conversion = {}
        if CONVERSION_ACTION_ID in payload:
          resource_name = CONVERSION_ACTION_RESOURCE.format(
              customer_id=customer_id,
              conversion_action_id=payload[CONVERSION_ACTION_ID])
          conversion[CONVERSION_ACTION] = resource_name

        if GCLID in payload:
          conversion[GCLID] = payload[GCLID]

        if CONVERSION_DATE_TIME in payload:
          conversion[CONVERSION_DATE_TIME] = payload[CONVERSION_DATE_TIME]

        if CONVERSION_VALUE in payload:
          conversion[CONVERSION_VALUE] = payload[CONVERSION_VALUE]

        conversion[CURRENCY_CODE] = payload.get(
            CURRENCY_CODE, DEFAULT_CURRENCY_CODE)

        conversions.append(conversion)

      request_params = {
          'conversions': conversions,
          _PARTIAL_FAILURE: True
      }

      url = UPLOAD_CLICK_CONVERSIONS_URL.format(
          api_version=self.api_version, customer_id=customer_id)

      response = self._send_api_request(url, request_params)
      return self._check_response(payloads, response)

    except json.JSONDecodeError as json_decode_error:
      raise errors.DataOutConnectorError(
          msg=str(json_decode_error), error=json_decode_error
      ) from json_decode_error
    except requests.RequestException as request_exception:
      self._handle_request_exception(request_exception)

  @tenacity.retry(
      retry=tenacity.retry_if_exception_type(
          errors.DataOutConnectorSendUnsuccessfulError),
      wait=tenacity.wait.wait_exponential(max=_MAX_RETRIES),
      stop=tenacity.stop.stop_after_attempt(_MAX_RETRIES),
      reraise=True)
  def upload_customer_match_user_list(
      self,
      customer_id: str,
      user_list_name: str,
      create_new_list: bool,
      payloads: List[type_alias.Payload],
      upload_key_type: str,
      app_id: Optional[str] = None,
      membership_life_span_days: int = DEFAULT_MEMBERSHIP_LIFESPAN_DAYS
  ) -> None:
    """Uploads customer match user list.

    Args:
      customer_id: The customer id.
      user_list_name: The name of the user list.
      create_new_list: The flag that indicates if new list should be created if
        there is no existing list with given user_list_name.
      payloads: The list of data for the customer match user list.
      upload_key_type: The type of customer match user list.
      app_id: The mobile app id for creating new user list when the
        upload_key_type is MOBILE_ADVERTISING_ID.
      membership_life_span_days: Number of days a user's cookie stays.

    Raises:
      DataOutConnectorError raised when errors occurred.
    """

    if not user_list_name:
      raise errors.DataOutConnectorValueError(
          msg='user_list_name is empty.')

    # Raises error because create new list requested, but no app id
    # provided.
    if (create_new_list and
        upload_key_type == CUSTOMER_MATCH_UPLOAD_KEY_MOBILE_ADVERTISING_ID and
        not app_id):
      raise errors.DataOutConnectorValueError(
          msg=('When upload_key_type is MOBILE_ADVERTISING_ID, new list '
               'cannot be created without an app_id.'))

    try:
      user_list_resource_name = self._get_user_list_resource_name(
          user_list_name, customer_id)

      # Raises error because no list exists and create new list not requested.
      if not user_list_resource_name and not create_new_list:
        raise errors.DataOutConnectorValueError(
            msg=(f'Customer match user list: {user_list_name} does not exist.'
                 ' and create_new_list = False'))

      if not user_list_resource_name:
        user_list_resource_name = self._create_customer_match_user_list(
            customer_id, user_list_name,
            upload_key_type, app_id, membership_life_span_days)

      offline_user_data_job_operations = self._build_user_data_job_operations(
          payloads, upload_key_type)

      offline_user_data_job_resource_name = self._create_offline_user_data_job(
          user_list_resource_name, customer_id)

      self.log.info(f'offline_user_data_job_resource_name='
                    f'{offline_user_data_job_resource_name}')

      self._add_users_to_customer_match_user_list(
          offline_user_data_job_resource_name, offline_user_data_job_operations)

      self._run_offline_user_data_job(offline_user_data_job_resource_name)

      self._get_offline_user_data_job_status(
          offline_user_data_job_resource_name, customer_id)

    except json.JSONDecodeError as json_decode_error:
      raise errors.DataOutConnectorError(
          msg=str(json_decode_error), error=json_decode_error
      ) from json_decode_error
    except requests.RequestException as request_exception:
      self._handle_request_exception(request_exception)

  def _validate_credential(self, config_data: Dict[str, str]):
    """Validate required fields are in the credential yaml file."""
    if not all(key in config_data for key in _CREDENTIAL_REQUIRED_KEYS):
      raise errors.DataOutConnectorAuthenticationError(
          msg=f'Missing required field. The required fields are: '
              f'{str(_CREDENTIAL_REQUIRED_KEYS)}',
          error_num=errors.ErrorNameIDMap.ADS_HOOK_ERROR_BAD_YAML_FORMAT
      )

  def _refresh_access_token(self):
    """Request OAUTH2 access token."""
    payload = {
        'grant_type': 'refresh_token',
        'client_id': self.config_data['client_id'],
        'client_secret': self.config_data['client_secret'],
        'refresh_token': self.config_data['refresh_token']
    }
    response = requests.post(_OAUTH2_TOKEN_URL, params=payload)
    response.raise_for_status()
    data = response.json()
    self.access_token = data.get('access_token', None)

  def _get_http_header(self):
    """Get the Authorization HTTP header.

    Returns:
      The authorization HTTP header.
    """
    if not self.access_token:
      self._refresh_access_token()
    return {
        'authorization': f'Bearer {self.access_token}',
        'developer-token': self.config_data['developer_token'],
        'login-customer-id': str(self.config_data['login_customer_id'])
    }

  def _send_api_request(
      self,
      url: str,
      params: Dict[str, Any],
      method: str = 'POST'
  ) -> Dict[Any, Any]:
    """Call the requested API endpoint with the given parameters.

    Args:
      url: The API endpoint to call.
      params: The parameters to pass into the API call.
      method: The request method to use.

    Returns:
      The JSON data from the response.
    """

    headers = self._get_http_header()
    response = requests.request(
        url=url, method=method, json=params, headers=headers)
    response.raise_for_status()
    return response.json()

  def _get_offline_user_data_job_status(
      self,
      resource_name: str,
      customer_id: str
  ) -> None:
    """Gets the status of the offline user data job.

    Args:
      resource_name: The resource name of the offline user data job.
      customer_id: The Google Ads customer id
    """
    query = (f'SELECT '
             'offline_user_data_job.resource_name,'
             'offline_user_data_job.id,'
             'offline_user_data_job.status,'
             'offline_user_data_job.type,'
             'offline_user_data_job.failure_reason '
             'FROM offline_user_data_job '
             'WHERE offline_user_data_job.resource_name='
             f"'{resource_name}' "
             'LIMIT 1')
    payload = {
        'query': query
    }
    url = SEARCH_URL.format(
        api_version=self.api_version, customer_id=customer_id)
    response = self._send_api_request(url, payload)

    # Log the status for diagnostic
    self.log.info(response)

  def _get_user_list_resource_name(
      self,
      name: str,
      customer_id: str
  ) -> Optional[str]:
    """Gets the resource name of the customer match user list.

    Args:
      name: The name of the user list.
      customer_id: The Google Ads customer id

    Returns:
      The resource name of the customer match user list if it exists. None
        if it doesn't exist.
    """
    query = ('SELECT user_list.resource_name FROM user_list '
             f"WHERE user_list.name='{name}' AND customer.id = {customer_id} "
             'LIMIT 1')
    payload = {
        'query': query
    }
    url = SEARCH_URL.format(
        api_version=self.api_version, customer_id=customer_id)
    response = self._send_api_request(url, payload)

    try:
      return response['results'][0]['userList']['resourceName']
    except (KeyError, IndexError) as error:
      self.log.exception(error)

    return None

  def _create_customer_match_user_list(
      self,
      customer_id: str,
      user_list_name: str,
      upload_key_type: str,
      app_id: Optional[str] = None,
      membership_life_span: int = DEFAULT_MEMBERSHIP_LIFESPAN_DAYS
  ) -> str:
    """Creates a new customer match user list.

    Args:
      customer_id: The customer id.
      user_list_name: The name of the user list.
      upload_key_type: The type of customer match user list.
      app_id: Mobile app id for creating new user list when the upload_key_type
        is MOBILE_ADVERTISING_ID.
      membership_life_span: Number of days a user's cookie stays.

    Returns:
      The resource name of the user list.
    """
    crm_based_user_list = {
        'uploadKeyType': upload_key_type
    }

    if upload_key_type == CUSTOMER_MATCH_UPLOAD_KEY_MOBILE_ADVERTISING_ID:
      crm_based_user_list['app_id'] = app_id

    payload = {
        'operations': [
            {
                'create': {
                    'name': user_list_name,
                    'membershipLifeSpan': membership_life_span,
                    'crmBasedUserList': crm_based_user_list
                }
            }
        ],
        _PARTIAL_FAILURE: False
    }

    url = USER_LIST_MUTATE_URL.format(
        api_version=self.api_version, customer_id=customer_id)
    response = self._send_api_request(url, payload)

    try:
      return response['results'][0]['resourceName']
    except (KeyError, IndexError) as error:
      raise errors.DataOutConnectorError(
          msg='Response returned from user list mutate endpoint is invalid.',
          error=error
      ) from error

  def _create_offline_user_data_job(
      self, resource_name: str, customer_id: str) -> str:
    """Creates offline user data job.

    To upload user data, an offline job needs to be created first. Data is
      uploaded to the job for being processed at the server side.

    Args:
      resource_name: The resource name of the user list.
      customer_id: The customer id.

    Returns:
      The resource name of the offline user data job
    """
    job = {
        'job': {
            'type': 'CUSTOMER_MATCH_USER_LIST',
            'customerMatchUserListMetadata': {
                'userList': resource_name
            }
        }
    }
    url = OFFLINE_USER_DATA_JOBS_CREATE_URL.format(
        api_version=self.api_version, customer_id=customer_id)

    response = self._send_api_request(url, job)

    try:
      return response['resourceName']
    except KeyError as error:
      raise errors.DataOutConnectorError(
          msg=('Response returned from offline user data job endpoint is '
               'invalid.'),
          error=error
      ) from error

  def _run_offline_user_data_job(self, resource_name):
    """Run the offline user data job with the resource_name.

    Args:
      resource_name: The resource name of the user list.
    """
    url = OFFLINE_USER_DATA_JOBS_RUN_URL.format(
        api_version=self.api_version, resource_name=resource_name
    )

    self._send_api_request(url, {})

  def _add_users_to_customer_match_user_list(
      self,
      resource_name: str,
      operations: List[Any]
  ) -> None:
    """Adds users to the customer match user list.

    Args:
      resource_name: The ID of the user list to upload.
      operations: A list of user creation operations.
    """
    add_user_operations = {
        'operations': operations,
        'enablePartialFailure': True,
        'enableWarnings': True,
    }
    url = USER_ADD_OPERATION_URL.format(
        api_version=self.api_version, resource_name=resource_name)

    self._send_api_request(url, add_user_operations)

  def _build_user_data_job_operations(
      self,
      payloads: List[type_alias.Payload],
      upload_key_type: str
  ) -> List[Any]:
    """Builds user creation operation list.

    Args:
      payloads: The list of data for the customer match user list.
      upload_key_type: The type of customer match user list.

    Returns:
      The list of user creation operations..
    """
    offline_user_data_job_operations = []

    for _, payload in payloads:
      user_identifier = {}

      if upload_key_type == CUSTOMER_MATCH_UPLOAD_KEY_CONTACT_INFO:
        if HASHED_EMAIL in payload:
          user_identifier[HASHED_EMAIL] = payload[HASHED_EMAIL]
        elif HASHED_PHONE_NUMBER in payload:
          user_identifier[HASHED_PHONE_NUMBER] = payload[HASHED_PHONE_NUMBER]
        else:
          for field in ADDRESS_INFO_FIELDS:
            if field in payload:
              if ADDRESS_INFO not in user_identifier:
                user_identifier[ADDRESS_INFO] = {}
              user_identifier[ADDRESS_INFO][field] = payload[field]
      elif upload_key_type == CUSTOMER_MATCH_UPLOAD_KEY_MOBILE_ADVERTISING_ID:
        if MOBILE_ID in payload:
          user_identifier[MOBILE_ID] = payload[MOBILE_ID]
      elif upload_key_type == CUSTOMER_MATCH_UPLOAD_KEY_CRM_ID:
        if THIRD_PARTY_USER_ID in payload:
          user_identifier[THIRD_PARTY_USER_ID] = payload[THIRD_PARTY_USER_ID]

      offline_user_data_job_operation = {
          'create': {
              'userIdentifiers': [user_identifier]
          }
      }

      offline_user_data_job_operations.append(offline_user_data_job_operation)

    return offline_user_data_job_operations

  def _check_response(
      self, payloads: List[type_alias.Payload], response: Any
  ) -> List[type_alias.PayloadError]:
    """Converts partial errors returned from the API to TCRM errors.

    Args:
      payloads: The list of data for upload.
      response: A response message instance.

    Returns:
      List of TCRM errors
    """
    partial_failure = response.get('partialFailureError', None)
    if not partial_failure:
      return []

    code = partial_failure.get('code', None)
    if not code or code == 0:
      return []

    details = partial_failure.get('details', None)
    if not details:
      return []

    index_errors = []
    for detail in details:
      error_list = detail.get('errors', None)
      if not error_list:
        continue

      for error in error_list:
        error_message = error.get('message', '')
        error_code = ''
        for k, v in error.get('errorCode', {}).items():
          error_code += f'{k}: {v}\n'

        index = 0
        field_name = ''

        location = error.get('location', None)
        if location:
          field_path_elements = location.get('fieldPathElements', None)
          if field_path_elements:
            field_path_element = field_path_elements[0]
            index = field_path_element.get('index', 0)
            field_name = field_path_element.get('fieldName', '')

        payload_index = payloads[index][0]
        self.log.error(_LOG_FORMAT % (payload_index, field_name, error_code,
                                      error_message))

        if 'internalError' in error_code:
          index_errors.append(
              (payload_index,
               errors.ErrorNameIDMap.RETRIABLE_ERROR_EVENT_NOT_SENT))
        else:
          index_errors.append(
              (payload_index,
               errors.ErrorNameIDMap.NON_RETRIABLE_ERROR_EVENT_NOT_SENT))

    return index_errors

  def _handle_http_error(self, error) -> None:
    status_code = error.response.status_code

    try:
      json_data = error.response.json()
      self.log.error(json_data)
    except json.JSONDecodeError:
      raise errors.DataOutConnectorError(
          msg=str(error), error=error
      ) from error

    if status_code == 400:
      raise errors.DataOutConnectorError(
          msg=str(error), error=error
      ) from error

    if status_code in (401, 403):
      error_dict = json_data.get('error', {})

      if not isinstance(error_dict, Dict):
        raise errors.DataOutConnectorAuthenticationError(
            msg=str(error), error=error
        ) from error

      detail = error_dict.get('details', [{}])[0].get('detail', '')
      if status_code == 401 and 'Token expired' in detail:
        self.access_token = None
      else:
        raise errors.DataOutConnectorAuthenticationError(
            msg=str(error), error=error
        ) from error

    raise errors.DataOutConnectorSendUnsuccessfulError(
        msg=str(error), error=error
    ) from error

  def _handle_request_exception(
      self, request_exception: requests.RequestException) -> None:
    """Check if failed API call can be retried.

    Args:
      request_exception: The request exception

    Raises:
      DataOutConnectorSendUnsuccessfulError: if retriable error happens
      DataOutConnectorError: if non-retriable error happens
    """
    if isinstance(request_exception, requests.HTTPError):
      self._handle_http_error(request_exception)

    self.log.exception(request_exception)

    raise errors.DataOutConnectorSendUnsuccessfulError(
        msg=str(request_exception), error=request_exception
    ) from request_exception
