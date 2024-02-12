import os

from datetime import datetime

from airflow import DAG

from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from ingest_script import upload_to_gcs


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


gcs_workflow = DAG(
    "GCSIngestionDAG_HW3",
    catchup=True,
    max_active_runs=1,
    start_date=datetime(2022, 1, 1, 8, 0, 0),
    schedule_interval='0 8 2 * *',
    end_date=datetime(2023, 1, 1, 8, 0, 0)
)

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

#https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-01.parquet
GREEN_URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_'
GREEN_URL_TEMPLATE = GREEN_URL_PREFIX + '{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
GREEN_DATASET_PATH_TEMPLATE = AIRFLOW_HOME + '/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
GREEN_OUTPUT_FILE_TEMPLATE = 'green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'


with gcs_workflow:


    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSL {GREEN_URL_TEMPLATE} > {GREEN_DATASET_PATH_TEMPLATE}"
    )

    load_to_gcs_task = PythonOperator(
        task_id='load_multiple',
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "gcs_path": f'Green_2022/{GREEN_OUTPUT_FILE_TEMPLATE}',
            "src_file": GREEN_DATASET_PATH_TEMPLATE
        }
    )


    download_dataset_task >> load_to_gcs_task


