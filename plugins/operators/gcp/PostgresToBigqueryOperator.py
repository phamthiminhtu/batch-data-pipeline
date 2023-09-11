from hooks.BigQueryHook import BigQueryHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable, Connection
from airflow.settings import AIRFLOW_HOME
from airflow.operators.python import PythonOperator


class PostgresToBigqueryOperator(PythonOperator):

    def __init__(
            self,
            sql,
            postgres_conn_id,
            bigquery_project_id,
            bigquery_dataset_id,
            bigquery_table_id,
            gcp_conn_id='gcp_conn_id',
            create_new_table_if_not_existing=True
        ) -> None:

        super().__init__(**kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.postgres_conn_id = postgres_conn_id
        self.sql = sql
        self.create_new_table_if_not_existing = create_new_table_if_not_existing
        self.bigquery_hook = BigQueryHook(gcp_conn_id=self.gcp_conn_id)
        self.postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        self.bigquery_project_id = bigquery_project_id
        self.bigquery_dataset_id = bigquery_dataset_id
        self.bigquery_table_id = bigquery_table_id
    
    def query_postgres(self):
        df = self.postgres_hook.get_records(sql=self.sql)
        return df

    def check_table_exists(self):
        is_existing = self.bigquery_hook.table_exists(
            dataset_id=self.bigquery_dataset_id,
            table_id=self.bigquery_table_id,
            project_id=self.bigquery_project_id
        )
        return is_existing

    def transfer_to_bigquery(self, rows):
        is_existing = self.check_table_exists()
        if is_existing:
            self.self.bigquery_hook.insert_all(
                dataset_id=self.bigquery_dataset_id,
                table_id=self.bigquery_table_id,
                project_id=self.bigquery_project_id,
                rows=rows
            )
        
        
       

