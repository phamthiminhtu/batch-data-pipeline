from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
    ClusterGenerator
)
from airflow.models import Variable, Connection
from airflow.settings import AIRFLOW_HOME

GCS_BUCKET_NAME = 'tototus-1'
GCP_CONN_ID = 'gcp_conn_id'
CLUSTER_NAME = 'nyctaxicluster'
PROJECT_ID = 'batch-data-pipeline-398705'
REGION = 'us-central1'
PYSPARK_URI_PREFIX = f'gs://{GCS_BUCKET_NAME}/nyc_taxi/spark-job'

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": f'{PYSPARK_URI_PREFIX}/spark__clean_data.py'},
}

CLUSTER_CONFIG = ClusterGenerator(
    project_id=PROJECT_ID,
    zone="us-central1-a",
    master_machine_type="n1-standard-2",
    worker_machine_type="n1-standard-2",
    num_workers=2,
    worker_disk_size=300,
    master_disk_size=300,
    storage_bucket=GCS_BUCKET_NAME,
).make()

# DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'wait_for_downstream': True,
    'start_date': datetime(2023, 10, 1),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'nyc_taxi',
    default_args=default_args,
    schedule_interval='0 0 * * *',
    max_active_runs=1
) as dag:
    
    taxi_data_to_gcs = LocalFilesystemToGCSOperator(
        task_id="taxi_data_to_gcs",
        src="data/nyc_taxi/*.parquet",
        dst=f"nyc_taxi/input_data/",
        bucket=GCS_BUCKET_NAME,
        gcp_conn_id=GCP_CONN_ID
    )
    
    sync_spark_jobs_to_gcs = LocalFilesystemToGCSOperator(
        task_id="sync_spark_jobs_to_gcs",
        src="python_scripts/nyc_taxi/spark_jobs/*.py",
        dst=f"nyc_taxi/spark-job/",
        bucket=GCS_BUCKET_NAME,
        gcp_conn_id=GCP_CONN_ID
    )

    create_spark_cluster = DataprocCreateClusterOperator(
        task_id="create_spark_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        gcp_conn_id=GCP_CONN_ID
    )

    spark_clean_data = DataprocSubmitJobOperator(
        task_id="spark_clean_data", 
        job=PYSPARK_JOB, 
        region=REGION, 
        project_id=PROJECT_ID,
        gcp_conn_id=GCP_CONN_ID
    )

    [taxi_data_to_gcs, sync_spark_jobs_to_gcs] >> create_spark_cluster >> spark_clean_data
