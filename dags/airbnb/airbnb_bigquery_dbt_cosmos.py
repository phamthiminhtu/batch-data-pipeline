import os
import logging
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
# from airflow import AirflowException
from airflow import DAG
from airflow.decorators import dag, task_group
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook
from cosmos import DbtTaskGroup, DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import GoogleCloudServiceAccountFileProfileMapping
# from cosmos.constants import ExecutionMode
from airflow.settings import AIRFLOW_HOME
from operators.gcp.LocalFilesystemToGCSOperator import LocalFilesystemToGCSOperator
from operators.gcp.GCSToBigQueryOperator import GCSToBigQueryOperator



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
BIGQUERY_DATASET = "airbnb_raw"
BIGQUERY_CONN_ID = "google_cloud_connection"
GCS_BUCKET_NAME = "batch-data-pipeline-airbnb"
GCP_KEY_PATH = f"{AIRFLOW_HOME}/config/credentials/kafka-408805-key.json"
DBT_PROJECT_PATH = f"{AIRFLOW_HOME}/dags/dbt/airbnb_bigquery"
DATA_SCHEMA_RELATIVE_PATH = "config/schemas/airbnb"

CENSUS_LGA_G01_COLUMNS = Variable.get('CENSUS_LGA_G01_COLUMNS')
CENSUS_LGA_G02_COLUMNS = Variable.get('CENSUS_LGA_G02_COLUMNS')
LISTINGS_COLUMNS = Variable.get('LISTINGS_COLUMNS')
NSW_LGA_CODE_COLUMNS = Variable.get('NSW_LGA_CODE_COLUMNS')
NSW_LGA_SUBURB_COLUMNS = Variable.get('NSW_LGA_SUBURB_COLUMNS')
DATA_DIRS = {
    "census_lga_g01": CENSUS_LGA_G01_COLUMNS,
    "census_lga_g02": CENSUS_LGA_G02_COLUMNS,
    "listings": LISTINGS_COLUMNS,
    "nsw_lga_code": NSW_LGA_CODE_COLUMNS,
    "nsw_lga_suburb": NSW_LGA_SUBURB_COLUMNS
}

# The path where Cosmos will find the dbt executable
# in the virtual environment created in the Dockerfile
DBT_EXECUTABLE_PATH = f"{AIRFLOW_HOME}/dbt_venv/bin/dbt"



@dag(
    dag_id='airbnb_bigquery_dbt_cosmos',
    default_args=dag_default_args,
    schedule_interval=None,
    max_active_runs=1,
    concurrency=5
)
def airbnb_dbt_cosmos():
    """
        Involve 3 parts:
            - Part 1: Ingest data:

    """

    # Part 1: Ingest data: Local -> GCS (landing zone) -> BigQuery
    @task_group()
    def ingest_data():
        for folder in DATA_DIRS:
            task_dict = {
                    "refine_source_data": f"{folder}__refine_source_data",
                    "local_data_to_gcs": f"{folder}__upload_data_from_local_to_gcs",
                    "local_schema_to_gcs": f"{folder}__upload_schema_from_local_to_gcs",
                    "gcs_to_bigquery": f"{folder}__gcs_to_bigquery",
                }

            local_data_to_gcs = LocalFilesystemToGCSOperator(
                task_id=task_dict.get("local_data_to_gcs"),
                src=f"{AIRFLOW_HOME}/data/airbnb/source/{folder}",
                bucket=GCS_BUCKET_NAME,
                gcp_conn_id=BIGQUERY_CONN_ID,
                dst=''
            )

            local_schema_to_gcs = LocalFilesystemToGCSOperator(
                task_id=task_dict.get("local_schema_to_gcs"),
                src=[f"{AIRFLOW_HOME}/{DATA_SCHEMA_RELATIVE_PATH}/{folder}.json"],
                bucket=GCS_BUCKET_NAME,
                gcp_conn_id=BIGQUERY_CONN_ID,
                dst='',
                is_hive_partitioned=False
            )

            gcs_to_bigquery = GCSToBigQueryOperator(
                task_id=task_dict.get("gcs_to_bigquery"),
                bucket=GCS_BUCKET_NAME,
                source_objects=local_data_to_gcs.output, # XCOM now can be accessed via output https://www.astronomer.io/blog/advanced-xcom-configurations-and-trigger-rules-tips-and-tricks-to-level-up-your-airflow-dags/
                destination_project_dataset_table=f"{GCP_PROJECT}.{BIGQUERY_DATASET}.{folder}",
                gcp_conn_id=BIGQUERY_CONN_ID,
                schema_object=f"{DATA_SCHEMA_RELATIVE_PATH}/{folder}/*.json",
                write_disposition="WRITE_TRUNCATE",
                ignore_unknown_values=True
            )

            local_data_to_gcs >> local_schema_to_gcs >> gcs_to_bigquery

    # Part 2: Transform data with dbt
    transform_data = DummyOperator(
        task_id="dbt"
    )

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

    execution_config = ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE_PATH)

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

    ingest_data() >> transform_data >> dbt_transform_data

airbnb_dbt_cosmos()
