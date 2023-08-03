from datetime import datetime, timedelta
from airflow import DAG
from operators.microsoft.LocalFilesystemToAzureBlobStorageOperator import LocalFilesystemToAzureBlobStorageOperator
from airflow.models import Variable, Connection
from airflow.settings import AIRFLOW_HOME

# DAG definition
default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "wait_for_downstream": True,
    "start_date": datetime(2023, 8, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
    "youtube_trend",
    default_args=default_args,
    schedule_interval="0 0 * * *",
    max_active_runs=1,
) as dag:

    youtube_trending_to_azure_blob_storage = LocalFilesystemToAzureBlobStorageOperator(
        task_id="youtube_trending_to_azure_blob_storage",
        file_path=f"{AIRFLOW_HOME}/batch-data-pipeline/data/youtube-trending",
        container_name='youtube_trending',
        blob_name='',
        wasb_conn_id="sas_token",
        create_container=False
    )

    youtube_category_to_azure_blob_storage = LocalFilesystemToAzureBlobStorageOperator(
        task_id="youtube_category_to_azure_blob_storage",
        file_path=f"{AIRFLOW_HOME}/batch-data-pipeline/data/youtube-category",
        container_name='youtube_category',
        blob_name='',
        wasb_conn_id="sas_token",
        create_container=False
    )



