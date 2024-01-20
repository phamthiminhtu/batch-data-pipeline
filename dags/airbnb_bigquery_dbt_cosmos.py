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

DBT_PROJECT_PATH = f"{AIRFLOW_HOME}/dags/dbt/airbnb_bigquery"
# The path where Cosmos will find the dbt executable
# in the virtual environment created in the Dockerfile
DBT_EXECUTABLE_PATH = f"{AIRFLOW_HOME}/dbt_venv/bin/dbt"

BIGQUERY_CONN_ID = "gcp_connection"

profile_config = ProfileConfig(
    profile_name="bde",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id=BIGQUERY_CONN_ID,
        profile_args={
            "schema": "airbnb_raw",
            "host": "localhost",
            "dbname": "airflow",
            "user": "airflow",
            "password": "airflow",
            "port":5432
        },
    ),
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)

my_cosmos_dag = DbtDag(
    project_config=ProjectConfig(
        DBT_PROJECT_PATH
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=DBT_EXECUTABLE_PATH,
        execution_mode=ExecutionMode.VIRTUALENV
    ),
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="airbnb_bigquery_dbt_cosmos",
)

