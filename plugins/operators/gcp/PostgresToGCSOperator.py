from datetime import datetime
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator as PostgresToGCSOperatorBase
from airflow.utils.context import Context
from airflow.providers.postgres.hooks.postgres import PostgresHook


class PostgresToGCSOperator(PostgresToGCSOperatorBase):

    def __init__(
            self,
            **kwargs
        ):
        
        # ts = datetime.fromisoformat(context['ts'])
        # self.filename = f'{ts.year}/{ts.month}/{ts.day}/{ts.hour}/{filename}'
        super().__init__(**kwargs)
        
        


