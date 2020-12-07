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

# python3

"""Errors file for this data connector component.

All exceptions defined by the library should be in this file.
"""
import enum
import frozendict

# A dictionary with error numbers and error descriptions to use for consistent
# logging and error handling across CC4D.
_ERROR_ID_DESCRIPTION_MAP = frozendict.frozendict({
    10: 'General error occurred.',
    11: 'Event not sent due to authentication error. Event is due for retry.',
    12: 'Event not sent. Event is due for retry.',
    50: 'Event not sent. Event will not be retried.'})


# An enum with error names and error numbers to use for consistent logging and
# error handling across CC4D.
class ErrorNameIDMap(enum.Enum):
  # Retriable error numbers start from 10
  ERROR = 10
  RETRIABLE_ERROR_OUTPUT_AUTHENTICATION_FAILED = 11
  RETRIABLE_ERROR_EVENT_NOT_SENT = 12

  # Non retriable error numbers start from 50
  NON_RETRIABLE_ERROR_EVENT_NOT_SENT = 50


class Error(Exception):
  """Base error class for all Exceptions.

  Can store a custom message and a previous error, if exists, for more
  details and stack tracing use.
  """

  def __init__(self, msg: str = '',
               error_num: ErrorNameIDMap = ErrorNameIDMap.ERROR,
               error: Exception = None) -> None:
    super(Error, self).__init__()
    self.error_num = error_num
    self.msg = msg
    self.prev_error = error

  def __repr__(self) -> str:
    reason = 'Error %d - %s' % (self.error_num.value, type(self).__name__)
    if self.msg:
      reason += ': %s' % self.msg
    if self.prev_error:
      reason += '\nSee causing error:\n%s' % str(self.prev_error)
    return reason

  __str__ = __repr__


# Monitoring related errors
class MonitoringError(Error):
  """Raised when monitoring returns an error."""


class MonitoringRunQueryError(MonitoringError):
  """Raised when querying monitoring DB returns an error."""


class MonitoringDatabaseError(MonitoringError):
  """Error occurred while Creating DB or table."""


class MonitoringAppendLogError(MonitoringError):
  """Error occurred while inserting log info into monitoring DB."""


# Data in connector related errors
class DataInConnectorError(Error):
  """Raised when an input data source connector returns an error."""


class DataInConnectorBlobParseError(DataInConnectorError):
  """Error occurred while parsing blob contents."""


class DataInConnectorValueError(DataInConnectorError):
  """Error occurred due to a wrong value being passed on."""


# Data out connector related errors
class DataOutConnectorError(Error):
  """Raised when an output data source connector returns an error."""


class DataOutConnectorValueError(DataOutConnectorError):
  """Error occurred due to a wrong value being passed on."""


class DataOutConnectorInvalidPayloadError(DataOutConnectorError):
  """Error occurred constructing or handling payload."""


class DataOutConnectorSendUnsuccessfulError(DataOutConnectorError):
  """Error occurred while sending data to data out source."""


class DataOutConnectorBlobReplacedError(DataOutConnectorError):
  """Error occurred while sending blob contents and Blob was replaced."""


class DataOutConnectorBlobProcessError(DataOutConnectorError):
  """Error occurred while sending some parts of blob contents."""


class DataOutConnectorAuthenticationError(DataOutConnectorError):
  """Error occurred while authenticating against the output resource."""
