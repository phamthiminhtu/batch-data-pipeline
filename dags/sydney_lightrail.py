from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable, Connection
from airflow.settings import AIRFLOW_HOME
from operators.gcp.PostgresToGCSOperator import PostgresToGCSOperator

POSTGRES_CONNECTION_ID = 'postgres_conn_id'

GCS_BUCKET_NAME = Variable.get('GCS_BUCKET_NAME')
POSTGRES_TABLE_ID = 'b_tfnsw_lightrail_opendata_march'
POSTGRES_SCHEMA_ID = 'lightrail' 
POSTGRES_DATABASE = 'sydney_transport'
QUERY = f'''
    SELECT * FROM {{ POSTGRES_DATABASE }}.{{ POSTGRES_SCHEMA_ID }}.{{ POSTGRES_TABLE_ID }}
    WHERE updated_at BETWEEN DATE('{{ dag_run.logical_date }}') - INTERVAL '1 DAY' AND DATE('{{ dag_run.logical_date }}')
'''

# DAG definition
default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "wait_for_downstream": True,
    "start_date": datetime(2023, 9, 10),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
    'sydney_lightrail',
    default_args=default_args,
    schedule_interval='0 0 * * *',
    max_active_runs=1
) as dag:
    
    get_data = PostgresToGCSOperator(
        task_id=POSTGRES_TABLE_ID,
        postgres_conn_id=POSTGRES_CONNECTION_ID,
        sql=QUERY,
        bucket=GCS_BUCKET_NAME,
        filename=f'{POSTGRES_DATABASE}__{POSTGRES_SCHEMA_ID}__{POSTGRES_TABLE_ID}',
        gzip=False
    )