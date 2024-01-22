import os
import logging
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from airflow import AirflowException
from airflow import DAG
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from cosmos import DbtTaskGroup, DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import GoogleCloudServiceAccountFileProfileMapping
from cosmos.constants import ExecutionMode
from airflow.settings import AIRFLOW_HOME
from operators.gcp.LocalFilesystemToGCSOperator import LocalFilesystemToGCSOperator


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
GCP_PROJECT = "kafka-408805"
BIGQUERY_DATASET = "airbnb_bigquery"
GCS_BUCKET_NAME = "batch-airbnb"
GCP_KEY_PATH = f"{AIRFLOW_HOME}/config/credentials/kafka-408805-key.json"
DBT_PROJECT_PATH = f"{AIRFLOW_HOME}/dags/dbt/airbnb_bigquery"

CENSUS_LGA_G01_COLUMNS = Variable.get('CENSUS_LGA_G01_COLUMNS')
CENSUS_LGA_G02_COLUMNS = Variable.get('CENSUS_LGA_G02_COLUMNS')
LISTINGS_COLUMNS = Variable.get('LISTINGS_COLUMNS')
NSW_LGA_CODE_COLUMNS = Variable.get('NSW_LGA_CODE_COLUMNS')
NSW_LGA_SUBURB_COLUMNS = Variable.get('NSW_LGA_SUBURB_COLUMNS')
DATA_DIRS = {
    "census_lga_g01": CENSUS_LGA_G01_COLUMNS,
    "census_lga_g02": CENSUS_LGA_G02_COLUMNS,
    "listings": LISTINGS_COLUMNS,
    "NSW_LGA_CODE": NSW_LGA_CODE_COLUMNS,
    "NSW_LGA_SUBURB": NSW_LGA_SUBURB_COLUMNS
}

# The path where Cosmos will find the dbt executable
# in the virtual environment created in the Dockerfile
DBT_EXECUTABLE_PATH = f"{AIRFLOW_HOME}/dbt_venv/bin/dbt"

BIGQUERY_CONN_ID = "gcp_connection"

profile_config = ProfileConfig(
    profile_name="bde",
    target_name="dev",
    profile_mapping=GoogleCloudServiceAccountFileProfileMapping(
        conn_id=BIGQUERY_CONN_ID,
        profile_args={
            "project": GCP_PROJECT,
            "dataset": BIGQUERY_DATASET,
            "threads": 4,
            "keyfile": GCP_KEY_PATH,
            "location": "australia-southeast1"
        },
    ),
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)

@dag(
    dag_id='airbnb_bigquery_dbt_cosmos',
    default_args=dag_default_args,
    schedule_interval=None,
    max_active_runs=1,
    concurrency=5
)
def airbnb_dbt_cosmos():

    transform_data = DummyOperator(
        task_id="dbt"
    )

    dbt_transform_data = DbtTaskGroup(
        group_id="transform_data",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
        default_args={"retries": 2},
        operator_args={
            "install_deps": True,
        },
    )

    for folder in DATA_DIRS:
        local_to_gcs = LocalFilesystemToGCSOperator(
            task_id=f"{folder}__upload_data_from_local_to_gcs",
            src=f"{AIRFLOW_HOME}/data/airbnb/{folder}",
            bucket=GCS_BUCKET_NAME,
            gcp_conn_id=BIGQUERY_CONN_ID,
            dst=''
        )

        local_to_gcs >> transform_data >> dbt_transform_data

airbnb_dbt_cosmos()
