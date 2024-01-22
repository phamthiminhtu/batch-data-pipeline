import os
import logging
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from airflow import AirflowException
# from airflow import DAG
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from cosmos import DbtTaskGroup, DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from cosmos.constants import ExecutionMode
from airflow.settings import AIRFLOW_HOME

dag_default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(days=2),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

#########################################################
#
#   Load Environment Variables
#
#########################################################
AIRFLOW_DATA = "data/airbnb"
POSTGRES_SCHEMA = 'airbnb_raw'
CENSUS_LGA_G01_COLUMNS = Variable.get('CENSUS_LGA_G01_COLUMNS')
CENSUS_LGA_G02_COLUMNS = Variable.get('CENSUS_LGA_G02_COLUMNS')
LISTINGS_COLUMNS = Variable.get('LISTINGS_COLUMNS')
NSW_LGA_CODE_COLUMNS = Variable.get('NSW_LGA_CODE_COLUMNS')
NSW_LGA_SUBURB_COLUMNS = Variable.get('NSW_LGA_SUBURB_COLUMNS')
DBT_PROJECT_PATH = f"{AIRFLOW_HOME}/dags/dbt/airbnb_bigquery"
GCP_PROJECT = "kafka-408805"
BIGQUERY_DATASET = "airbnb_bigquery"
# The path where Cosmos will find the dbt executable
# in the virtual environment created in the Dockerfile
DBT_EXECUTABLE_PATH = f"{AIRFLOW_HOME}/dbt_venv/bin/dbt"

DATA_DIRS = {
    "census_lga_g01": CENSUS_LGA_G01_COLUMNS,
    "census_lga_g02": CENSUS_LGA_G02_COLUMNS,
    "listings": LISTINGS_COLUMNS,
    "NSW_LGA_CODE": NSW_LGA_CODE_COLUMNS,
    "NSW_LGA_SUBURB": NSW_LGA_SUBURB_COLUMNS
}

BIGQUERY_CONN_ID = "gcp_connection"

profile_config = ProfileConfig(
        profile_name="bde",
        target_name="dev",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id=BIGQUERY_CONN_ID,
            profile_args={
                "type": "bigquery",
                "method": "oauth",
                "project": GCP_PROJECT,
                "dataset": BIGQUERY_DATASET,
                "threads": 4
            }
        ),
    )

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)

@dag(
    dag_id='airbnb',
    default_args=dag_default_args,
    schedule_interval=None,
    max_active_runs=1,
    concurrency=5
)
def airbnb_dbt_cosmos():

    def load_one_csv_file_to_posgres_table(
            data_path,
            file_name,
            dest_schema,
            dest_table,
            columns,
            **kwargs
        ):

        # set up pg connection
        ps_pg_hook = PostgresHook(postgres_conn_id=BIGQUERY_CONN_ID)
        conn_ps = ps_pg_hook.get_conn()
        file_path = f'{data_path}/{file_name}'

        # generate dataframe by combining files
        df = pd.read_csv(file_path)

        # replace NaN with None
        # need to cast to object before converting NaN to None
        # https://github.com/pandas-dev/pandas/issues/42423
        df = df.astype(object).where(df.notnull(), None)

        # add ingestion_timestamp
        df['ingestion_timestamp'] = pd.Timestamp("now")
        columns += ',ingestion_timestamp'

        row_count = df.shape[0]
        logging.info(f"Loading file {file_path} - {row_count} records found.")
        dest_table = f"{dest_schema}.{dest_table}"

        if len(df) > 0:
            df_columns = columns.split(',')
            values = df[df_columns].to_dict('split')
            values = values['data']
            insert_sql_template ="""
                INSERT INTO {dest_table}({columns})
                VALUES %s
            """

            insert_sql = insert_sql_template.format(
                dest_table=dest_table,
                columns=columns
            )

            logging.info(insert_sql)

            execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
            conn_ps.commit()

            logging.info(f"Finished loading file {file_path}.")
            logging.info(f"Output table: `{dest_table}`")

            return dest_table, row_count
        return "", "Empty file."

    def load_csv_files_to_postgres_table(
        folder_name,
        dest_schema,
        dest_table,
        columns,
        **kwargs
    ):
        data_path = f'{AIRFLOW_DATA}/{folder_name}'

        #get all files with filename including the string '.csv'
        filelist = [f for f in os.listdir(data_path) if '.csv' in f]

        logging.info(f"Loading files {filelist}...")
        total_row_count = 0

        for file in filelist:
            output_table, row_count = load_one_csv_file_to_posgres_table(
                data_path=data_path,
                file_name=file,
                dest_schema=dest_schema,
                dest_table=dest_table,
                columns=columns)
            total_row_count += row_count

        logging.info(f"Finish loading files {filelist}. Total {total_row_count} records inserted.")

        return output_table 

    transform_data = DbtTaskGroup(
        group_id="transform_data",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
        default_args={"retries": 2},
    )

    folders_to_load = list(DATA_DIRS.keys())

    for folder in folders_to_load:
        columns = DATA_DIRS.get(folder)
        task_name = f"load_csv_files_to_postgres_table__{folder}"
        tmp_task = PythonOperator(
            task_id=task_name,
            python_callable=load_csv_files_to_postgres_table,
            op_kwargs={
                "folder_name": folder,
                "dest_schema": POSTGRES_SCHEMA,
                "dest_table": folder,
                "columns": columns
            },
            provide_context=True
        )
        tmp_task >> transform_data

airbnb_dbt_cosmos()
