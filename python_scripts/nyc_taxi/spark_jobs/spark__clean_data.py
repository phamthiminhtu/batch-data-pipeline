from pyspark.sql import SparkSession
import pyspark.sql.functions as Function
from pyspark.sql import SQLContext

GCS_BUCKET_NAME = 'tototus-1'
COLOR = 'green' 
DATA_YEARS = ['2022', '2021'] 
INPUT_DATA_PATH = f'gs://{GCS_BUCKET_NAME}/nyc_taxi/input_data' 
OUTPUT_DATA_PATH = f'gs://{GCS_BUCKET_NAME}/nyc_taxi/output_data'

q6_query_base = """
    WITH
    raw_data AS (
        SELECT
            *,
            (UNIX_TIMESTAMP({dropoff_datetime})-UNIX_TIMESTAMP({pickup_datetime}))/60 AS duration_minute,
            (UNIX_TIMESTAMP({dropoff_datetime})-UNIX_TIMESTAMP({pickup_datetime}))/3600 AS duration_hour,
            trip_distance*1.60934 AS trip_distance_km,
            {year} AS year
        FROM {file_name}_tmp_view
    ),

    data AS (
        SELECT
            *,
            CASE 
                WHEN duration_hour = 0 THEN NULL 
                ELSE trip_distance_km/duration_hour
            END AS speed_kph
        FROM raw_data
    )

    SELECT 
        *
    FROM data
    WHERE
        -- 6.a.
        NOT({dropoff_datetime} <= {pickup_datetime}) 
        -- 6.b.
        AND NOT (
            YEAR({pickup_datetime}) < {year}
            OR YEAR({dropoff_datetime}) < {year}
            OR YEAR({pickup_datetime}) > {year}
            OR DATE({dropoff_datetime}) > '{drop_off_date_limit}'
        )
        -- 6.c. and 6.d.
        AND NOT(
            speed_kph IS NULL -- NULL when duration = 0
            OR speed_kph <= 0
            OR speed_kph > 100 -- NYC statewise speed limit is 55 mph ~ 88.5 kph
        ) 
        -- 6.e.
        AND NOT (
            duration_minute < 2
            OR duration_minute > 180
        )
        -- 6.f.
        AND NOT (
            trip_distance_km < 0.3 -- trip_distance_km < 300m
            OR trip_distance_km > 100
        )
        -- 6.g.
        -- the maximum number of passengers of a yellow/ green taxi is 5 + 1 child
        -- don't remove if passenger_count is null 
        AND (passenger_count IS NULL OR passenger_count BETWEEN 0 AND 6)
                
"""

def create_spark_session():
    spark = SparkSession.builder \
        .master("yarn") \
        .appName('dataproc-udacity') \
        .getOrCreate()
    return spark

def load_data(path, temp_view_name, spark):
    print(f'Reading file {path} ...')
    df_tmp = spark.read.parquet(path)
    df_tmp.createOrReplaceTempView(temp_view_name)
    
    # count number of rows before cleaning
    row_count_query = f'''SELECT COUNT(*) FROM {temp_view_name}'''
    
    row_count = spark.sql(row_count_query).first()
    
    print(f'Row count: {row_count}')
    print(f'Done reading file {path}.')
    print(f'Output temp view: {temp_view_name}')

def run_cleaning_query(q6_query_base, color, year, file_name, output_data_path, spark):
    '''
        Run the data cleaning query. Then create a table to store the query result.
        - Input: 
            - q6_query_base: A query containing data cleaning logics.
            - color: taxi color
            - year: year of the data
            - file_name: file name prefix
        - Output: A table after applying the cleaning logic on the temp view.
    '''
    # define variables used for cleaning logic
    # yellow taxi and green taxi have different schema
    print(f'Cleaning data {file_name}...')
    if color == 'green':
        pickup_datetime, dropoff_datetime = 'lpep_pickup_datetime', 'lpep_dropoff_datetime'
    else:
        pickup_datetime, dropoff_datetime = 'tpep_pickup_datetime', 'tpep_dropoff_datetime'
    
    # for the logic of 6b
    next_year = int(year) + 1
    drop_off_date_limit = f'{next_year}-01-01'

    q6_query = q6_query_base.format(
        file_name=file_name,
        pickup_datetime=pickup_datetime,
        dropoff_datetime=dropoff_datetime,
        year=year,
        drop_off_date_limit=drop_off_date_limit
    )
    df_tmp = spark.sql(q6_query)
    df_tmp.write.parquet(f'{output_data_path}/cleaned__{file_name}.parquet', mode='overwrite')
    print(f'Done. Output: {output_data_path}.')

def clean_data(color, data_years, input_data_path, output_data_path, spark):
    '''
        Wrap the load_data() and run_cleaning_query() functions into a clean_data function.
        - Input: 
            - year: year of the data
            - color: taxi color
        - Output: A table.
    '''
    for year in data_years:
        # get file_name
        file_name = f'{color}_taxi_{year}'
        file_path = f"{input_data_path}/{file_name}.parquet"
        temp_view_name = f'{file_name}_tmp_view'
        
        # load data into spark dataframe
        load_data(
            path=file_path,
            temp_view_name=temp_view_name,
            spark=spark
        )
        
        # run cleaning query
        run_cleaning_query(
            q6_query_base=q6_query_base,
            color=color,
            year=year,
            file_name=file_name,
            output_data_path=output_data_path,
            spark=spark
        )

def run_spark_job():
    spark = create_spark_session()
    clean_data(
        color=COLOR, 
        data_years=DATA_YEARS, 
        input_data_path=INPUT_DATA_PATH, 
        output_data_path=OUTPUT_DATA_PATH, 
        spark=spark)
    
run_spark_job()