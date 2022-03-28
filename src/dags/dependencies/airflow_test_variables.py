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

"""Test constants that will be used as airflow configuration."""
# TODO: Add the reference to setup up file explaining variables.

import datetime

TEST_VARS = {
    'ga_source': 'ga_project.dataset.table',
    'bq_working_project': 'AA',
    'bq_working_dataset': 'BB',
    'training_start_date': datetime.datetime(2019, 1, 1),
    'training_end_date': datetime.datetime(2019, 3, 1),
    'prediction_start_date': '20190301',
    'prediction_end_date': '20190401',
    'template_location': 'gs://blockbuster/mlwp_templates',
    'temp_path': 'gs://blockbuster/temp',
    'predictionFactName': 'target',
    'predictionFactValues': 'STP_All_Clicks- payment_success',
    'userIdColumn': 'client_id',
}

BASE_FEATURES = {
    'features': [{
        'fact': 'fullVisitorId',
        'type': 'Id'
    }, {
        'fact': 'accumulator.test_num_feature',
        'type': 'Numeric',
        'accumulators': ['concat']
    }, {
        'fact': 'accumulator.test_cat_feature',
        'type': 'Categorical',
        'accumulators': ['sum']
    }]
}

AIRFLOW_TEST_VARS = {
    'bb_project': {
        'gcp_project': 'A',
        'gcp_project_id': 'B',
        'gcp_project_number': 666397429207,
        'gcp_region': 'us-central1',
        'gcp_zone': 'us-central1-a'
    },
    'bb_storage': {
        'bq_working_project': TEST_VARS['bq_working_project'],
        'bq_working_dataset': TEST_VARS['bq_working_dataset'],
        'gcs_dataflow_path': TEST_VARS['template_location'],
        'gcs_output_path': 'gs://blockbuster-output-files/output',
        'gcs_temp_path': TEST_VARS['temp_path']
    },
    'bb_preprocess': {
        'ga_source': TEST_VARS['ga_source'],
        'start_date': TEST_VARS['training_start_date'].strftime('%Y%m%d'),
        'lookback_days': '5',
        'prediction_days': '7',
        'end_date': TEST_VARS['training_end_date'].strftime('%Y%m%d'),
        'lookbackGapInDays': 1,
        'minimumLookaheadTimeInSeconds': 86400,
        'maximumLookaheadTimeInSeconds': 604800,
        'stopOnFirstPositiveLabel': False,
        'slideTimeInSeconds': 604800,
        'windowTimeInSeconds': 2592000,
        'userIdColumn': TEST_VARS['userIdColumn'],
    },
    'bb_training': {
        'model_storage_project': 'blockbuster',
        'start_date': TEST_VARS['training_start_date'].strftime('%Y%m%d'),
        'end_date': TEST_VARS['training_end_date'].strftime('%Y%m%d'),
        'predictionFactName': TEST_VARS['predictionFactName'],
        'predictionFactValues': TEST_VARS['predictionFactValues'],
        'model_training_hours': '3'
    },
    'bb_prediction_activation': {
        'start_date': TEST_VARS['prediction_start_date'],
        'end_date': TEST_VARS['prediction_end_date'],
        'ga_tracking_id': 'UA-000000000-1',
        'leads_submission_window': '7',
        'event_label': 'score',
        'event_action': 'scored',
        'event_category': 'ventile'
    },
    'bb_features': BASE_FEATURES,
    'recent_dataset': 'TBL-111111',
    'recent_model': 'TBL-0000000'
}

TEST_DAG_ARGS = {
    'start_date': TEST_VARS['training_start_date'].strftime('%Y%m%d'),
    'dataflow_default_options': {
        'project': AIRFLOW_TEST_VARS['bb_project']['gcp_project'],
        'region': AIRFLOW_TEST_VARS['bb_project']['gcp_region'],
        'zone': AIRFLOW_TEST_VARS['bb_project']['gcp_zone'],
        'tempLocation': AIRFLOW_TEST_VARS['bb_storage']['gcs_temp_path']
    },
}
