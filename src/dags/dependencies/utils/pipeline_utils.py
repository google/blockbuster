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

"""Utility functions for data pipelines."""
import ast
import configparser
import datetime
import os
from typing import Any, Dict, List, Union

import jinja2
import yaml


class Error(Exception):
  """Base error for this module."""


class ParseError(Error):
  """An exception to be raised when config file is invalid."""


def read_file(file_path: str) -> str:
  """Reads and returns contents of the file.

  Args:
    file_path: File path.

  Returns:
    content: File content.

  Raises:
      FileNotFoundError: If the provided file is not found.
  """
  try:
    with open(file_path, 'r') as stream:
      content = stream.read()
  except FileNotFoundError:
    raise FileNotFoundError(f'The file "{file_path}" could not be found.')
  else:
    return content


def load_configuration(yaml_path: str) -> Dict[str, Union[Dict[str, Any], Any]]:
  """Loads YAML configuration file.

  Args:
    yaml_path: YAML configuration file name.

  Returns:
    config: YAML configuration content.
  """
  try:
    content = read_file(yaml_path)
    config = yaml.safe_load(content)
  except (yaml.scanner.ScannerError, yaml.parser.ParserError,
          yaml.reader.ReaderError) as exception:
    raise ValueError('File could not be parsed: {file}. {msg}'.format(
        file=yaml_path, msg=str(exception)))
  else:
    return config


def parse_configs(
    config_file: str) -> Dict[str, Union[List[Any], int, float, str]]:
  """Parses configuration file.

  Args:
    config_file: A path to configuration file.

  Returns:
    config_dict: Config content mapped to key:value pairs.
  """
  parser = configparser.ConfigParser()
  try:
    config_content = read_file(config_file)
    parser.read_string(config_content)
    raw_config = dict(parser['config'])
  except (configparser.ParsingError) as error:
    raise ParseError(error)

  config_dict = {
      key: ast.literal_eval(value) for key, value in raw_config.items()
  }
  return config_dict


def configure_sql(sql_path: str, query_params: Dict[str, Union[str, int,
                                                               float]]) -> str:
  """Configures parameters of SQL script with variables supplied from Airflow.

  Args:
    sql_path: Path to SQL script.
    query_params: Configuration containing query parameter values.

  Returns:
    sql_script: String representation of SQL script with parameters assigned.
  """
  sql_script = read_file(sql_path)

  params = {}
  for param_key, param_value in query_params.items():
    # If given value is list of strings (ex. 'a,b,c'), create tuple of
    # strings (ex. ('a', 'b', 'c')) to pass to SQL IN operator.
    if isinstance(param_value, str) and ',' in param_value:
      params[param_key] = tuple(param_value.split(','))
    else:
      params[param_key] = param_value

  return sql_script.format(**params)


def get_sql_template_file_path(template_dir: str,
                               template_basename: str) -> str:
  """Returns full path to SQL template file.

  Args:
    template_dir: Directory containing SQL jinja2 template files.
    template_basename: Base name of the SQL jinja2 file.

  Returns:
    Full path to SQL jinja2 template.
  """

  return os.path.join(
      os.path.abspath(f'{template_dir}'), f'{template_basename}.sql.jinja2')


def render_sql_from_template(template_basename: str,
                             template_dir: str = 'gcs/dags/sql',
                             **kwargs) -> str:
  """Renders the template sql file into a formatted sql statement.

  gcs/dags/sql is considered as the default directory (correspoding to Google
  Cloud Composer directory structure).

  Args:
    template_basename: Base name of the SQL jinja2 file.
    template_dir: Directory containing SQL jinja2 template files.
     **kwargs: Parameters to insert into the template.

  Returns:
     Parametrized SQL query.
  """
  template_file_path = get_sql_template_file_path(template_dir,
                                                  template_basename)
  sql_template_str = read_file(template_file_path)
  template = jinja2.Template(sql_template_str)
  return template.render(**kwargs)


def append_date_suffix_to_table(table_name: str) -> str:
  """Appends suffix (today's date) to table name.

  Args:
    table_name: the name of the table that the suffix is appended.

  Returns:
    Suffixed table name.
  """
  return table_name + '_' + datetime.datetime.now().strftime('%Y%m%d')

