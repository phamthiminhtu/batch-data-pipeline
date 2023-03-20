import json
from datetime import datetime, timedelta
from airflow import DAG
from operators.aws.LocalFilesystemToS3Operator import LocalFilesystemToS3Operator
# from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.models import Variable, Connection
from airflow.settings import AIRFLOW_HOME
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.postgres_operator import PostgresOperator
# from airflow.operators.python import PythonOperator

# Config
S3_BUCKET_NAME = Variable.get("S3_BUCKET_NAME")

# EMR_ID = Variable.get("EMR_ID")

EMR_STEPS = {}
# with open("./data/scripts/emr/clean_movie_review.json") as json_file:
#     EMR_STEPS = json.load(json_file)

# DAG definition
default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "wait_for_downstream": True,
    "start_date": datetime(2023, 3, 19),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
    "user_behaviour",
    default_args=default_args,
    schedule_interval="0 0 * * *",
    max_active_runs=1,
) as dag:

    # extract_user_purchase_data = PostgresOperator(
    #     dag=dag,
    #     task_id="extract_user_purchase_data",
    #     sql="./scripts/sql/unload_user_purchase.sql",
    #     postgres_conn_id="postgres_default",
    #     params={"user_purchase": "/temp/user_purchase.csv"},
    #     depends_on_past=True,
    #     wait_for_downstream=True,
    # )

    movie_review_to_raw_data_lake = LocalFilesystemToS3Operator(
        task_id="movie_review_to_raw_data_lake",
        filename=f"{AIRFLOW_HOME}/aws-pipeline/data/movie_review.csv",
        dest_key="movie_review",
        dest_bucket=S3_BUCKET_NAME,
        replace=True,
        aws_conn_id='aws'
    )