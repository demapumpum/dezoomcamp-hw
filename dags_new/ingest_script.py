import os

from time import time

import pandas as pd
from sqlalchemy import create_engine

import pyarrow.parquet as pq
import pyarrow as pa
import gcsfs


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


def get_data(url_template, start, end, output_path):
    for i in range (start, end):
        url= url_template + f'{i}.csv.gz'
        os.system(f'curl -sSL {url} > {output_path}{i}.csv.gz')


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


def upload_to_gcs(bucket, src_file):
    root_path = f'{bucket}/HW_Week2'

    table = pq.read_table(src_file)

    gcs = gcsfs.GCSFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['lpep_pickup_date'],
        filesystem=gcs
    )


def ingest_parquet_to_postgres(src_file, user, password, host, port, db, table_name):
    table = pq.read_table(src_file)
    df = table.to_pandas()

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    
    df.to_sql(name=table_name, con=engine, if_exists='replace')