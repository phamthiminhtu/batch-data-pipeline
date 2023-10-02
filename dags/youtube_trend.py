from datetime import datetime, timedelta
from airflow import DAG
from operators.snowflake.SnowflakeOperator import SnowflakeOperator
from operators.microsoft.LocalFilesystemToAzureBlobStorageOperator import LocalFilesystemToAzureBlobStorageOperator
from airflow.models import Variable, Connection
from airflow.settings import AIRFLOW_HOME

QUERY_PATH = './sql/youtube/youtube_trend'
SNOWFLAKE_CONN_ID = 'snowflake_conn_id'
STORAGE_INTEGRATION_NAME = Variable.get('STORAGE_INTEGRATION_NAME')
AZURE_TENANT_ID = Variable.get('AZURE_TENANT_ID')
YOUTUBE_STORAGE_ALLOWED_LOCATIONS = Variable.get('YOUTUBE_STORAGE_ALLOWED_LOCATIONS')
YOUTUBE_STAGE_NAME = Variable.get('YOUTUBE_STAGE_NAME')
SCHEMA_NAME = Variable.get('SCHEMA_NAME')
DATABASE_NAME = Variable.get('DATABASE_NAME')
SNOWFLAKE_SOURCE_DATABASE_NAME = Variable.get('SNOWFLAKE_SOURCE_DATABASE_NAME')
SNOWFLAKE_SOURCE_SHEMA_NAME = Variable.get('SNOWFLAKE_SOURCE_SHEMA_NAME')
INSERT_OVERWRITE_INTERVAL = Variable.get('INSERT_OVERWRITE_INTERVAL')

DAG_PARAMETERS= {
    'schema_name': SCHEMA_NAME,
    'storage_integration_name': STORAGE_INTEGRATION_NAME,
    'azure_tenant_id': AZURE_TENANT_ID,
    'youtube_stage_name': YOUTUBE_STAGE_NAME,
    'youtube_storage_allowed_locations': YOUTUBE_STORAGE_ALLOWED_LOCATIONS,
    'database_name': DATABASE_NAME,
    'source_database_name': SNOWFLAKE_SOURCE_DATABASE_NAME,
    'source_schema_name': SNOWFLAKE_SOURCE_SHEMA_NAME,
    'insert_overwrite_interval': INSERT_OVERWRITE_INTERVAL
}

# DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'wait_for_downstream': True,
    'start_date': datetime(2023, 9, 8),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'youtube_trend',
    default_args=default_args,
    schedule_interval='0 0 * * *',
    max_active_runs=1,
    template_searchpath=[QUERY_PATH]
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

    youtube_trending = SnowflakeOperator(
        task_id='youtube_trending',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql='youtube_trending.sql',
        params=DAG_PARAMETERS
    )

    youtube_final = SnowflakeOperator(
        task_id='youtube_final',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql='youtube_final.sql',
        params=DAG_PARAMETERS
    )

    [youtube_trending_to_azure_blob_storage, youtube_category_to_azure_blob_storage] >> youtube_trending >> youtube_final
