from datetime import datetime, timedelta
from airflow import DAG
from operators.snowflake.SnowflakeOperator import SnowflakeOperator
from airflow.models import Variable, Connection
from airflow.settings import AIRFLOW_HOME

QUERY_PATH = './sql/youtube'
SNOWFLAKE_CONN_ID = 'snowflake_conn_id'
STORAGE_INTEGRATION_NAME = Variable.get('STORAGE_INTEGRATION_NAME')
AZURE_TENANT_ID = Variable.get('AZURE_TENANT_ID')
YOUTUBE_STORAGE_ALLOWED_LOCATIONS = Variable.get('YOUTUBE_STORAGE_ALLOWED_LOCATIONS')
YOUTUBE_STAGE_NAME = Variable.get('YOUTUBE_STAGE_NAME')
SCHEMA_NAME = Variable.get('SCHEMA_NAME')
DATABASE_NAME = Variable.get('DATABASE_NAME')

DAG_PARAMETERS= {
    'schema_name': SCHEMA_NAME,
    'storage_integration_name': STORAGE_INTEGRATION_NAME,
    'azure_tenant_id': AZURE_TENANT_ID,
    'youtube_stage_name': YOUTUBE_STAGE_NAME,
    'youtube_storage_allowed_locations': YOUTUBE_STORAGE_ALLOWED_LOCATIONS,
    'database_name': DATABASE_NAME
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
    'youtube_trend_init',
    default_args=default_args,
    schedule_interval='@once',
    max_active_runs=1,
    template_searchpath=[QUERY_PATH]
) as dag:

    create_schema = SnowflakeOperator(
        task_id='create_schema',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql='create_schema.sql',
        params=DAG_PARAMETERS
    )

    create_storage_integration = SnowflakeOperator(
        task_id='create_storage_integration',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql='create_storage_integration.sql',
        params=DAG_PARAMETERS
    )

    create_stage = SnowflakeOperator(
        task_id='create_stage',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql='create_stage.sql',
        params=DAG_PARAMETERS,
        schema=SCHEMA_NAME,
        database=DATABASE_NAME
    )

    create_schema >> create_storage_integration >> create_stage

