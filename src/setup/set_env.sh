#!/bin/bash

#======================================================
# USER-CONFIGURABLE MODEL OPTIONS
#======================================================

# Which BigQuery Dataset Tables hold the GA data we will use?
export GA_SOURCE_PROJECT="bigquery-public-data"
export GA_SOURCE_DATASET="google_analytics_sample"
export GA_SOURCE_TABLE="ga_sessions_*"


# Which column contains the userId to use when predicting?
export USERID_COLUMN="clientId"

# TRAINING:
# The earliest date to consider for training? (in YYYYMMDD format)?
export FIRST_DAY_OF_TRAINING="20170601"
# The latest date to consider for training? (in YYYYMMDD format)?
export LAST_DAY_OF_TRAINING="20170801"

# PREDICTION:
# The latest window from which leads will be picked up for prediction
export LAST_N_DAYS_LATEST_LEADS_FOR_PREDICTION=7

# How many days before the current day should be considered when making
#   a prediction? This must be *less than* the number of days between
#   FIRST_DAY_OF_TRAINING and LAST_DAY_OF_TRAINING
export MODEL_LOOKBACK_PERIOD_DAYS=30

# How many days in the future should the model make predictions for?
export MODEL_PREDICTION_PERIOD_DAYS=7

# Which field contains the fact we will check for a successful prediction?
export MODEL_PREDICTION_FACT_NAME="target"

# Which text should be checked for a match (uses String Equals)?
export MODEL_PREDICTION_FACT_VALUE=true

# Which Google Analytics property will be sent hits for successful predictions?
export GOOGLE_ANALYTICS_TRACKING_ID="UA-*******-1"

#======================================================
# EXPERT-ONLY CONFIGURATION OPTIONS
#======================================================

GCP_PROJECT_ID=$(gcloud config list --format "value(core.project)")
export GCP_PROJECT_ID
GCP_PROJECT_NUMBER=$(gcloud projects describe "${GCP_PROJECT_ID}" --format='get(projectNumber)')
export GCP_PROJECT_NUMBER

# See https://cloud.google.com/about/locations for options. Must support
# Bigquery, Storage, Composer, AI Platform Training and Prediction, & Dataflow
export BQ_LOCATION="US"
export GCP_REGION="us-central1"
export GCP_ZONE="${GCP_REGION}-a"

export THIS_PROJECT="blockbuster" #  NOT the project_id; Just a unique name.
export DATA_STORAGE_PROJECT=$GCP_PROJECT_ID
export GCP_COMPOSER_ENV_NAME="${THIS_PROJECT}-composer-test-alpha"
export GCP_BUCKET="${DATA_STORAGE_PROJECT}-${THIS_PROJECT}-files-alpha"
export GCP_BQ_WORKING_DATASET="${THIS_PROJECT}_working_alpha"

export MODEL_WINDOW_SLIDE_TIME_DAYS=1
export MODEL_TRAINING_TIME_HOURS=3

# Export these variables to a .json file for upload to the server
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cat > "${DIR}"/variables.json << EOF
{
    "bb_project": {
      "gcp_project": "${THIS_PROJECT}",
      "gcp_project_id": "${GCP_PROJECT_ID}",
      "gcp_project_number": ${GCP_PROJECT_NUMBER},
      "gcp_region": "${GCP_REGION}",
      "gcp_zone": "${GCP_ZONE}"
    },
    "bb_storage": {
      "bq_working_project": "$DATA_STORAGE_PROJECT",
      "bq_working_dataset": "$GCP_BQ_WORKING_DATASET",
      "gcs_dataflow_path": "gs://${GCP_BUCKET}/mlwp_templates",
      "gcs_output_path": "gs://${GCP_BUCKET}/output",
      "gcs_temp_path": "gs://${GCP_BUCKET}/temp"
    },
    "bb_preprocess": {
      "ga_source": "$GA_SOURCE_PROJECT.$GA_SOURCE_DATASET.$GA_SOURCE_TABLE",
      "start_date": "$FIRST_DAY_OF_TRAINING",
      "lookback_days": $MODEL_LOOKBACK_PERIOD_DAYS,
      "prediction_days": "$MODEL_PREDICTION_PERIOD_DAYS",
      "end_date": "$LAST_DAY_OF_TRAINING",
      "lookbackGapInDays": 1,
      "minimumLookaheadTimeInSeconds": 0,
      "maximumLookaheadTimeInSeconds": $((86400*$MODEL_PREDICTION_PERIOD_DAYS)),
      "stopOnFirstPositiveLabel": true,
      "slideTimeInSeconds": $((86400*$MODEL_WINDOW_SLIDE_TIME_DAYS)),
      "windowTimeInSeconds": $((86400*$MODEL_LOOKBACK_PERIOD_DAYS)),
      "userIdColumn": "$USERID_COLUMN"
    },
    "bb_training": {
      "model_storage_project": "$DATA_STORAGE_PROJECT",
      "start_date": "$FIRST_DAY_OF_TRAINING",
      "end_date": "$LAST_DAY_OF_TRAINING",
      "predictionFactName": "$MODEL_PREDICTION_FACT_NAME",
      "predictionFactValues": "$MODEL_PREDICTION_FACT_VALUE",
      "model_training_hours": "$MODEL_TRAINING_TIME_HOURS"
    },
    "bb_prediction_activation": {
      "ga_tracking_id": "$GOOGLE_ANALYTICS_TRACKING_ID",
      "leads_submission_window": "$LAST_N_DAYS_LATEST_LEADS_FOR_PREDICTION",
      "event_action":"event_action",
      "event_label":"event",
      "event_category":"activation_vars"
    },
    "recent_dataset" : " ",
    "recent_model" : " "
}
EOF
