import os

from time import time

import pandas as pd
from sqlalchemy import create_engine

import pyarrow.parquet as pq
import pyarrow as pa
import gcsfs
from google.cloud import storage


def ingest_callable(user, password, host, port, db, table_name, csv_file, execution_date):
    print(table_name, csv_file, execution_date)
    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    print('connection established successfully, inserting data...')

    t_start = time()
    df_iter = pd.read_csv(csv_file, compression='gzip', iterator=True, chunksize=100000, low_memory=False) #added only low_memory=False to remove dtype warning
    
    df = next(df_iter)
    
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    
    df.to_sql(name=table_name, con=engine, if_exists='append')

    t_end = time()
    print('inserted the first chunck, took %.3f' % (t_end - t_start))
    
    while True:
        t_start = time()
        
        df = next(df_iter)
        
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        
        df.to_sql(name=table_name, con=engine, if_exists='append')
        
        t_end = time()
        
        print('inserted another chunck... %.3f' % (t_end - t_start))


def get_data(url_template, format, start, end, output_path):
    for i in range (start, end):
        url= url_template + f'{str(i).zfill(2)}{format}'
        os.system(f'curl -sSL {url} > {output_path}{i}{format}')


def transform_to_parquet(src_file, start, end, output_path):
    dfs = []
    taxi_dtypes = {
       'VendorID': 'Int64',
       'store_and_fwd_flag': 'str',
       'RatecodeID': 'Int64',
       'PULocationID': 'Int64',
       'DOLocationID': 'Int64',
       'passenger_count': 'Int64',
       'trip_distance': 'float64',
       'fare_amount': 'float64',
       'extra': 'float64',
       'mta_tax': 'float64',
       'tip_amount': 'float64',
       'tolls_amount': 'float64',
       'ehail_fee': 'float64',
       'improvement_surcharge': 'float64',
       'total_amount': 'float64',
       'payment_type': 'float64',
       'trip_type': 'float64',
       'congestion_surcharge': 'float64'
   }
    datetime_green_taxi = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

    for i in range (start, end):
        dfi = pd.read_csv(f'{src_file}{i}.csv.gz', compression='gzip', dtype=taxi_dtypes, parse_dates=datetime_green_taxi)
        dfs.append(dfi)
    
    df = pd.concat(dfs, ignore_index=True)

    non_zero_passengers_df = df[df['passenger_count'] > 0]
    transformed_df = non_zero_passengers_df[non_zero_passengers_df['trip_distance'] > 0]

    transformed_df['lpep_pickup_date'] = transformed_df['lpep_pickup_datetime'].dt.date

    transformed_df.rename(columns={'VendorID': 'vendor_id', 
                                     'RatecodeID': 'ratecode_id', 
                                     'PULocationID': 'pulocation_id', 
                                     'DOLocationID': 'dolocation_id'})
    
    table = pa.Table.from_pandas(transformed_df)
    pq.write_table(table, output_path)


def normalize_parquet(src_file, dtype_dict, output_path):
    dtype = pa.schema(dtype_dict)
    table = pq.read_table(src_file, schema=dtype)
    #df = pq.read_table(src_file).to_pandas(safe=False)
    # df = pd.read_parquet(src_file)
    # df.astype(dtype_dict)
    # parquet = pa.Table.from_pandas(df)
    pq.write_table(table, output_path)


def upload_to_gcs_partitioned(gcs_path, src_file, partition_cols):
    root_path = gcs_path

    table = pq.read_table(src_file)

    gcs = gcsfs.GCSFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=partition_cols,
        filesystem=gcs
    )


def upload_to_gcs(bucket, gcs_path, src_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(src_file)



def ingest_parquet_to_postgres(src_file, user, password, host, port, db, table_name):
    table = pq.read_table(src_file)
    df = table.to_pandas()

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    
    df.to_sql(name=table_name, con=engine, if_exists='replace')