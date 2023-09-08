from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator as SnowflakeOperatorBase

class SnowflakeOperator(SnowflakeOperatorBase):
    
    def __init__(
        self,
        **kwargs
    ):
        super().__init__(**kwargs)
