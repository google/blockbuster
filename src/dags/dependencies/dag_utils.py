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

"""Utility functions for Blockbuster DAGs."""

import json
import re
from typing import List, Optional, Tuple, Union, Dict

from airflow import models

FeatureConfig = Dict[str, Union[str, List[str]]]
FeatureConfigListMapping = Dict[str, List[FeatureConfig]]
# Type of the config variables loaded from Airflow environment for BlockBuster.
AirflowVarsConfig = Dict[str, Union[str, int, bool]]

_FEATURE_PARAMS = {
    'recentValueFromVariables': 'recent',
    'countValueFromVariables': 'count',
    'mostFreqValueFromVariables': 'mostFreq',
    'proportionsValueFromVariables': 'proportions',
    'sumValueFromVariables': 'sum',
    'averageValueFromVariables': 'average',
    'averageByTenureValueFromVariables': 'averageByTenure'
}
_OTHER_TEXT = 'Other'


def get_dag_id(this_dag_name: str,
               parent_dag_name: Optional[str] = None) -> str:
  """Generates the name for a DAG, accounting for whether it is a SubDAG.

  Args:
    this_dag_name: A name for this individual DAG.
    parent_dag_name: The name of the parent DAG of this DAG, if it exists.

  Returns:
    Proper name for thie DAG or SubDAG.
  """
  if parent_dag_name:
    return f'{parent_dag_name}.{this_dag_name}'
  return this_dag_name


def extract_bucket_parts(
    path_uri: str) -> Tuple[Union[str, None], Union[str, None]]:
  """Extracts bucket_name and path from a GCS uri.

  Args:
    path_uri: like "gs://${GCS_BUCKET}/${GCS_PATH}"

  Returns:
    A tuple of:
    bucket_name: The GCS Bucket Name ( eg. for a path like gs://blockbuster/temp
    GCS Bucket will be blockbuster)
    bucket_prefix: GCS Path ( eg. for a path like gs://blockbuster/temp/data the
    prefix will be temp/data)
  """
  if not re.match(r'^gs:\/\/.*[\/.*]*\/?$', path_uri):
    return None, None

  path_breaks = path_uri.split('/')
  # gs://blockbuster/temp/ will be split to ['gs:','','blockbuster', 'temp']
  bucket_name = path_breaks[2]
  # The bucket prefix is formed by concatenating the individual folder names
  # again with a slash to form the prefix
  bucket_prefix = ''
  if len(path_breaks) >= 3:
    bucket_prefix = '/'.join(path_breaks[3:])

  return bucket_name, bucket_prefix


def get_select_field_array(
    feature_objects: FeatureConfigListMapping) -> List[str]:
  """Gets SQL representing each element of a SELECT statement from Variables.

  Args:
    feature_objects: List of all possible features from the configuration.

  Returns:
    A list of fields to include in a SELECT statement for all matching features.
  """
  result = []

  for feature in get_features(feature_objects):
    if 'sql' in feature:
      result.append(f'{feature.get("sql")} AS {get_feature_name(feature)}')
    else:
      result.append(f'{feature.get("fact")} AS {get_feature_name(feature)}')

  return result


def get_features(feature_objects: FeatureConfigListMapping,
                 feat_key: Optional[str] = None,
                 feat_val: Optional[str] = None) -> List[FeatureConfig]:
  """Gets the list of features that match the provided feature type.

  Args:
    feature_objects: List of all possible features from the configuration.
    feat_key: If present, include features with this key matching the value.
    feat_val: If present, only include features with key matching this value.

  Returns:
    List of features that should be included after filters are applied.
  """
  missing_key_or_val = not (feat_key and feat_val)

  return list(
      filter(
          lambda x: missing_key_or_val or re.match(feat_val, x.get(feat_key)),
          feature_objects['features']))


def get_feature_name(feature: FeatureConfig) -> str:
  """Generates a BigQuery-safe feature name, given a BQ-safe fact name.

  Replaces all periods (indicating the column was in an array or struct)
  with underscores.

  Args:
    feature: Parameters for configuring a Feature, including a fact field.

  Returns:
    A name for this fields safe to be used as a BigQuery Column Name.

  Raises:
    ValueError: The Feature value did not have a field called 'fact'.
  """
  return re.sub(r'[^a-zA-Z0-9_]', '_', feature['fact'])


def get_feature_config_val(feature_config_key: str) -> FeatureConfigListMapping:
  """Returns the feature config list object from airflow env variable.

  Args:
    feature_config_key: Airflow variable key for the feature config list.

  Returns:
    The parsed feature config list after retrieving from airflow environment.
  """
  value = models.Variable.get(feature_config_key)
  try:
    value_parsed = json.loads(value)
  except json.decoder.JSONDecodeError as error:
    raise Exception('Provided key "{}" cannot be decoded. {}'.format(
        feature_config_key, error))
  return value_parsed


def _generate_feature_pipeline_parameter(feature: FeatureConfig,
                                         parameter: str) -> Optional[str]:
  """Generates formatted text from this feature - parameter combination.

  This function generates the output in the format expected by the Generate
  Feature Pipeline. You can read more about the pipeline on the github link.
  https://github.com/GoogleCloudPlatform/cloud-for-marketing/tree/master/marketing-analytics/predicting/ml-data-windowing-pipeline#to-run-this-directly-using-the-examples-from-options-above-1

  Args:
    feature: Parameters for configuring a Feature.
    parameter: The parameter we are looking to configure this feature for.

  Returns:
    Formatted text for this specific feature/parameter combination, if the
    feature should be included in this accumulator. If the feature should not
    include this accumulator, returns None.
  """
  # If accumulators is a single value, make it a list.
  accumulators = feature.get('accumulators')
  if isinstance(accumulators, str):
    accumulators = [accumulators]

  # We've been requested to add this accumulator for this feature
  if parameter in accumulators:
    top_items = feature.get('topN')
    if top_items:
      join_str = ','.join(top_items)
      feature_name = get_feature_name(feature)
      return f'{feature_name}:[{join_str}]:[{_OTHER_TEXT}]'
    return f'{get_feature_name(feature)}:[]:[]'
  return None


def generate_feature_pipeline_parameters(
    feature_objects: FeatureConfigListMapping) -> Dict[str, str]:
  """Generates all feature-based parameters for the Generate Feature Pipeline.

  Args:
    feature_objects: List of all possible features from the configuration.

  Returns:
    Dict of all feature based parameter names and values based on the
    accumulators in _FEATURE_PARAMS.
  """
  result = {}
  for param, key in _FEATURE_PARAMS.items():
    current_param = []

    for feature in feature_objects['features']:
      if feature.get('type', '') in ['Categorical', 'Numeric']:
        if feature.get('accumulators'):
          param_text = _generate_feature_pipeline_parameter(feature, key)
          if param_text:
            current_param.append(param_text)

    result[param] = ','.join(current_param)
  return result


def construct_bq_table_path(project: str, dataset: str, table_name: str) -> str:
  """Constructs BQ table path from path components for BQ airflow operator.

  Args:
    project: BigQuery project id.
    dataset: BigQuery dataset name.
    table_name: BigQuery table name.

  Returns:
    Full path to BigQuery table in the format:
    <project>.<dataset>.<table_name>

  Raises:
    ValueError if incorrect names is provided.
  """
  if (not re.match(r'^[\w\-]+$', project)) or (not re.match(
      r'^\w+$', dataset)) or (not re.match(r'^\w+$', table_name)):
    raise ValueError(
        f'Project={project}, Dataset={dataset} and Table_Name = {table_name} '
        'should contain only letters, numbers and underscore.')

  return '{}.{}.{}'.format(project, dataset, table_name)
