# Blockbuster

## Initial Setup

### Enable APIs

Enable the following APIs & Services in your GCP Project:

-   [Dataflow](https://console.developers.google.com/apis/api/dataflow.googleapis.com/overview)
-   [Cloud Composer](https://console.developers.google.com/apis/api/composer.googleapis.com/overview)
-   [Cloud AutoML](https://console.developers.google.com/apis/api/automl.googleapis.com/overview)
-   [Datastore](https://console.cloud.google.com/datastore/welcome): Select
    "Datastore Mode".

You can ignore the prompt to create credentials for each API.

### Environment Setup

1.  Edit `setup/set_env.sh` and configure with desired properties.

1.  Setup required environment variables in your console session:

    ```bash
    source setup/set_env.sh
    ```

1.  Setup "Private IP Google Access" on the network in the region to be used.
    This allows Dataflow to spin up CPUs without assigning external IPs.

    ```bash
    gcloud compute networks subnets update default \
        --region ${GCP_REGION} \
        --enable-private-ip-google-access
    ```

1.  Create the GCS Bucket and BQ Dataset:

    ```bash
    gsutil ls -L "gs://${GCP_BUCKET}" 2>/dev/null \
        || gsutil mb -c regional -l "${GCP_REGION}" "gs://${GCP_BUCKET}"
    bq --location=${BQ_LOCATION} mk --dataset \
        --description "${THIS_PROJECT} working dataset." \
        ${GCP_PROJECT_ID}:${GCP_BQ_WORKING_DATASET}
    ```

1.  Create a Cloud Composer environment (This step will take ~40min):

    ```bash
    gcloud composer environments create $GCP_COMPOSER_ENV_NAME \
        --location=$GCP_REGION \
        --disk-size=20 \
        --python-version=3 \
        --image-version="composer-1.10.4-airflow-1.10.3"
    ```

1.  Update the installed dependencies with `requirements.txt` (This step will
    take ~45min):

    ```bash
      gcloud composer environments update $GCP_COMPOSER_ENV_NAME \
      --location=$GCP_REGION \
      --update-pypi-packages-from-file="requirements.txt"
    ```

### Grant service account permissions

1.  Grant Owner permissions to the default compute service account:

    ```bash
    gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
      --member="serviceAccount:${GCP_PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
      --role='roles/owner'
    ```

1.  Grant Data Viewer permissions to the AutoML Tables service account.

    ```bash
    gcloud projects add-iam-policy-binding $DATA_STORAGE_PROJECT \
      --member="serviceAccount:service-${GCP_PROJECT_NUMBER}@gcp-sa-automl.iam.gserviceaccount.com" \
      --role='roles/bigquery.dataEditor'
    ```

### Setup Cloud Composer Variables

1.  If kubectl is not installed run the following command :

    ```bash
    sudo-apt get install kubectl
    ```

2.  Copy the generated `variables.json` file to the environment and import it:

    ```bash
    gcloud composer environments storage data import \
      --environment=$GCP_COMPOSER_ENV_NAME \
      --location=$GCP_REGION \
      --source="setup/variables.json"
    gcloud composer environments run $GCP_COMPOSER_ENV_NAME \
      --location $GCP_REGION \
      variables  -- --i /home/airflow/gcs/data/variables.json
    ```

3.  If necessary, modify the `features.json` file, then import it as well:

    ```bash
    gcloud composer environments storage data import \
      --environment=$GCP_COMPOSER_ENV_NAME \
      --location=$GCP_REGION \
      --source="setup/features.json"
    gcloud composer environments run $GCP_COMPOSER_ENV_NAME \
      --location $GCP_REGION \
      variables -- --i /home/airflow/gcs/data/features.json
    ```

### Generate ML Windowing Pipeline Templates:

1.  In a working directory on your local machine, clone the "Cloud for
    Marketing" git repository and initiate the build (This step will take ~5
    min):

    ```bash
      git clone https://github.com/GoogleCloudPlatform/cloud-for-marketing.git
      cd cloud-for-marketing/marketing-analytics/predicting/ml-data-windowing-pipeline/ && \
      gcloud builds submit \
        --config=cloud_build.json \
        --substitutions=_BUCKET_NAME=${GCP_BUCKET} && \
      cd ../../../../
    ```

### Update DAGs

1.  Get the GCS location associated with your Composer instance:

    ```bash
    export BB_DAG_BUCKET=$(
      gcloud composer environments describe $GCP_COMPOSER_ENV_NAME \
      --location $GCP_REGION \
      --format "value(config.dagGcsPrefix)")
    ```

1.  Copy the contents of the `dags` folder to that location:

    ```bash
    gsutil -m cp -r ./dags/* ${BB_DAG_BUCKET}
    ```

1.  Verify the DAGS loaded correctly. Run the following:

    ```bash
    gcloud composer environments run $GCP_COMPOSER_ENV_NAME \
      --location $GCP_REGION \
      list_dags
    ```

    If you completed all above steps correctly, the result will include:

    ```bash
    -------------------------------------------------------------------
    DAGS
    -------------------------------------------------------------------
    0_BB_Prepare_Source
    0_BB_Prepare_Source.prepare_source_data
    1_BB_Analysis
    1_BB_Analysis.analyze
    2_BB_Preprocessing
    2_BB_Preprocessing.preprocess
    3_BB_Data_load_and_train
    3_BB_Data_load_and_train.load_data
    3_BB_Data_load_and_train.train_model
    4_BB_Predict_and_activate
    4_BB_Predict_and_activate.activate_ga
    4_BB_Predict_and_activate.analyze
    4_BB_Predict_and_activate.predict
    4_BB_Predict_and_activate.prepare_source_data
    4_BB_Predict_and_activate.preprocess
    ```

1.  Open the Airflow UX. You can find the URL in the Cloud Composer page in GCP,
    or run the following to get the URL (Note it may take 1-2 minutes for the UX
    to show the new DAGs):

    ```bash
    gcloud composer environments describe $GCP_COMPOSER_ENV_NAME \
      --location $GCP_REGION \
      --format "value(config.airflowUri)"
    ```
