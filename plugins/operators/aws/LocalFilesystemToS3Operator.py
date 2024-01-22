from __future__ import annotations
from typing import TYPE_CHECKING, Sequence
from datetime import datetime
import glob
from utils import TusUtils
from airflow.settings import AIRFLOW_HOME
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator as LocalFilesystemToS3OperatorBase
from airflow.utils.context import Context


class LocalFilesystemToS3Operator(LocalFilesystemToS3OperatorBase):
    """
    source: https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_modules/airflow/providers/amazon/aws/transfers/local_to_s3.html#LocalFilesystemToS3Operator

    """

    def __init__(
        self,
        file_format='csv',
        **kwargs
    ):
        super().__init__(**kwargs)
        self.file_format = file_format

    def execute(self, context: Context):
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
        s3_bucket, s3_key = s3_hook.get_s3_bucket_key(
            self.dest_bucket, self.dest_key, "dest_bucket", "dest_key"
        )
        ts = datetime.fromisoformat(context['ts'])
        
        tmp_file_path = f'./tmp/{s3_key}/'
        utilities = TusUtils()

        if self.file_format == 'csv':
            utilities.split_csv_file(infile_path=self.filename, output_path=tmp_file_path)
            glob_list = glob.glob(f'{tmp_file_path}*.{self.file_format}')
        else:
            glob_list = [self.filename] #TODO: support other file formats
        
        for i, path in enumerate(glob_list):
            s3_key_with_hive_partition = f'{ts.year}/{ts.month}/{ts.day}/{ts.hour}/{s3_key}/{i}.{self.file_format}'
            s3_hook.load_file(
                path,
                s3_key_with_hive_partition,
                s3_bucket,
                self.replace,
                self.encrypt,
                self.gzip,
                self.acl_policy,
            )