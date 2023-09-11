from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook as BigQueryHookBase

class BigQueryHook(BigQueryHookBase):
    
    def __init__(
        self,
        **kwargs
    ):
        super().__init__(**kwargs)
