import os

from datetime import datetime

from airflow import DAG

from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from ingest_script import transform_to_parquet
from ingest_script import get_data
from ingest_script import upload_to_gcs
from ingest_script import ingest_parquet_to_postgres


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')

gcs_workflow = DAG(
    #"LocalIngestionDag",
    "GCSIngestionDAG",
    #schedule_interval="0 6 2 * *",
    schedule_interval="@daily",
    catchup=False,
    start_date=days_ago(1),
    max_active_runs=1
    #start_date=datetime(2021, 1, 1)
)

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

URL_PREFIX = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
URL_TEMPLATE = URL_PREFIX + 'yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz'
TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'

GREEN_URL_TEMPLATE = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-'
GREEN_DATASET_PATH_TEMPLATE = AIRFLOW_HOME + '/green_tripdata_2020-'
GREEN_OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output.parquet'

with gcs_workflow:

    multiple_wget_task = PythonOperator(
        task_id="multiple_wget",
        python_callable=get_data,
        op_kwargs={
            "url_template": GREEN_URL_TEMPLATE,
            "start": 10,
            "end": 13,
            "output_path": GREEN_DATASET_PATH_TEMPLATE
        }
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform_to_parquet,
        op_kwargs={
            "src_file": GREEN_DATASET_PATH_TEMPLATE,
            "start": 10,
            "end": 13,
            "output_path": GREEN_OUTPUT_FILE_TEMPLATE
        }
    )

    load_to_gcs_task = PythonOperator(
       task_id="load",
       python_callable=upload_to_gcs,
       op_kwargs={
           "bucket": BUCKET,
           "src_file": GREEN_OUTPUT_FILE_TEMPLATE
       }
    )

    load_to_postgres_task = PythonOperator(
        task_id="load_postgres",
        python_callable=ingest_parquet_to_postgres,
        op_kwargs={
            "src_file": GREEN_OUTPUT_FILE_TEMPLATE,
            "user": PG_USER,
            "password": PG_PASSWORD,
            "host": PG_HOST,
            "port": PG_PORT,
            "db": PG_DATABASE,
            "table_name": "green_taxi"
        }
    )

    multiple_wget_task >> transform_task >> load_to_gcs_task

    multiple_wget_task >> transform_task >> load_to_postgres_task

