import os

from datetime import datetime

from airflow import DAG

from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from ingest_script import transform_to_parquet
from ingest_script import get_data
from ingest_script import upload_to_gcs_partitioned
from ingest_script import upload_to_gcs
from ingest_script import normalize_parquet
from ingest_script import ingest_parquet_to_postgres


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')

gcs_workflow = DAG(
    #"LocalIngestionDag",
    "GCSIngestionDAG_W4_fhv2019_12",
    #schedule_interval="0 6 2 * *",
    #schedule_interval="@daily",
    #schedule_interval="@monthly",
    catchup=True,
    #start_date=days_ago(1),
    max_active_runs=1,
    start_date=datetime(2019, 1, 1, 8, 0, 0),
    schedule_interval='0 8 2 * *',
    end_date=datetime(2020, 1, 1, 8, 0, 0)
)

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

URL_PREFIX = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
URL_TEMPLATE = URL_PREFIX + 'yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz'
TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'

# GREEN_URL_TEMPLATE = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-'
# GREEN_DATASET_PATH_TEMPLATE = AIRFLOW_HOME + '/green_tripdata_2020-'
# GREEN_OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output.parquet'

SET = 'fhv'
YEAR = '{{ execution_date.strftime(\'%Y\') }}'

#https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-01.parquet
YELLOW_URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_'
YELLOW_URL_TEMPLATE = YELLOW_URL_PREFIX + '{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
YELLOW_DATASET_PATH_TEMPLATE = AIRFLOW_HOME + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
YELLOW_OUTPUT_FILE_TEMPLATE = 'yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'


#https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-01.parquet
GREEN_URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_'
GREEN_URL_TEMPLATE = GREEN_URL_PREFIX + '{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
GREEN_DATASET_PATH_TEMPLATE = AIRFLOW_HOME + '/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
GREEN_OUTPUT_FILE_TEMPLATE = 'green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
green_dtype = [("VendorID", "uint64"), ("lpep_pickup_datetime", "timestamp[ms]"), ("lpep_dropoff_datetime", "timestamp[ms]"), ("store_and_fwd_flag", "string"), ("RatecodeID", "uint64")
               , ("PULocationID", "uint64"), ("DOLocationID", "uint64"), ("passenger_count", "uint64"), ("trip_distance", "uint64"), ("fare_amount", "uint64")
               , ("extra", "uint64"), ("mta_tax", "uint64"), ("tip_amount", "uint64"), ("tolls_amount", "uint64"), ("ehail_fee", "uint64")
               , ("improvement_surcharge", "uint64"), ("total_amount", "uint64"), ("payment_type", "uint64"), ("", "uint64"), ("", "uint64")]


#https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2019-01.parquet
fhv_URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_'
fhv_URL_TEMPLATE = fhv_URL_PREFIX + '{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
fhv_DATASET_PATH_TEMPLATE = AIRFLOW_HOME + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
fhv_OUTPUT_FILE_TEMPLATE = 'fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
fhv_dtype = [("dispatching_base_num", "string"), ("pickup_datetime", "timestamp[ms]"), ("dropOff_datetime", "timestamp[ms]"), 
             ("PUlocationID", "int64"), ("DOlocationID", "int64"), ("SR_Flag", "int64"), ("Affiliated_base_number", "string")]

# fhv_dtype = {'dispatching_base_num':'string', 'pickup_datetime':'datetime64[s]', 'dropOff_datetime':'datetime64[s]', 
#              'PUlocationID':'Int64', 'DOlocationID':'Int64', 'SR_Flag':'Int64', 'Affiliated_base_number':'string',}

with gcs_workflow:

    # multiple_wget_task = PythonOperator(
    #     task_id="multiple_wget",
    #     python_callable=get_data,
    #     op_kwargs={
    #         "url_template": GREEN_URL_TEMPLATE ,
    #         "format": ".csv.gz",
    #         "start": 10,
    #         "end": 13,
    #         "output_path": GREEN_DATASET_PATH_TEMPLATE
    #     }
    # )

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSL {fhv_URL_TEMPLATE} > {fhv_DATASET_PATH_TEMPLATE}"
    )

    # transform_task = PythonOperator(
    #     task_id="transform",
    #     python_callable=transform_to_parquet,
    #     op_kwargs={
    #         "src_file": GREEN_DATASET_PATH_TEMPLATE,
    #         "start": 10,
    #         "end": 13,
    #         "output_path": GREEN_OUTPUT_FILE_TEMPLATE
    #     }
    # )
    normalize_parquet_task = PythonOperator(
        task_id='normalize',
        python_callable=normalize_parquet,
        op_kwargs={
            "src_file": fhv_DATASET_PATH_TEMPLATE,
            "dtype_dict": fhv_dtype,
            "output_path": fhv_OUTPUT_FILE_TEMPLATE
        }
    )

    # load_to_gcs_partitioned_task = PythonOperator(
    #    task_id="load_partition",
    #    python_callable=upload_to_gcs_partitioned,
    #    op_kwargs={
    #        "gcs_path": f'{BUCKET}/HW_Week2_1',
    #        "src_file": GREEN_OUTPUT_FILE_TEMPLATE,
    #        "partition_cols": ['lpep_pickup_date']
    #    }
    # )

    load_to_gcs_task = PythonOperator(
        task_id='load_multiple',
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "gcs_path": f'{SET}_{YEAR}_1/{fhv_OUTPUT_FILE_TEMPLATE}',
            "src_file": fhv_DATASET_PATH_TEMPLATE
        }
    )

    # load_to_postgres_task = PythonOperator(
    #     task_id="load_postgres",
    #     python_callable=ingest_parquet_to_postgres,
    #     op_kwargs={
    #         "src_file": GREEN_OUTPUT_FILE_TEMPLATE,
    #         "user": PG_USER,
    #         "password": PG_PASSWORD,
    #         "host": PG_HOST,
    #         "port": PG_PORT,
    #         "db": PG_DATABASE,
    #         "table_name": "green_taxi"
    #     }
    # )

    download_dataset_task >> normalize_parquet_task >> load_to_gcs_task


    # multiple_wget_task >> transform_task >> load_to_gcs_partitioned_task


    # multiple_wget_task >> transform_task >> load_to_postgres_task

