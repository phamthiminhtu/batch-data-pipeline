from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator as GCSToBigQueryOperatorBase
from airflow.operators.python import get_current_context

class GCSToBigQueryOperator(GCSToBigQueryOperatorBase):

    def __init__(
        self,
        schema_object=None,
        **kwargs
    ):
        super().__init__(**kwargs)