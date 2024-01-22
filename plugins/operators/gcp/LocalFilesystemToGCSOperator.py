import glob
import os
from utils import TusUtils
from datetime import datetime
from airflow.utils.context import Context
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator as LocalFilesystemToGCSOperatorBase

class LocalFilesystemToGCSOperator(LocalFilesystemToGCSOperatorBase):
    """
        source: https://airflow.apache.org/docs/apache-airflow-providers-google/1.0.0/_modules/airflow/providers/google/cloud/transfers/local_to_gcs.html#LocalFilesystemToGCSOperator
    """
    def __init__(
        self,
        connection_timeout=100000000,
        is_hive_partitioned=True,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.connection_timeout = connection_timeout
        self.is_hive_partitioned = is_hive_partitioned

    def execute(self, context: Context) -> None:
        """Upload a file to Google Cloud Storage."""
        hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
        self.log.info(f'Loading files from source {self.src}...')
        ts = datetime.fromisoformat(context['ts'])

        glob_list = glob.glob(f'{self.src}/*')
        filepaths = self.src if isinstance(self.src, list) else glob_list
        self.log.info(f'Total {len(filepaths)} files found:')
        self.log.info(filepaths)

        if os.path.basename(self.dst):  # path to a file
            if len(filepaths) > 1:  # multiple file upload
                raise ValueError(
                    "'dst' parameter references filepath. Please specify "
                    "directory (with trailing backslash) to upload multiple "
                    "files. e.g. /path/to/directory/"
                )
            object_paths = [self.dst]
        else:  # directory is provided
            object_paths = [os.path.join(self.dst, filepath) for filepath in filepaths]

        success_log = []
        # modify self.dst into hive partition
        for filename in object_paths:
            blob_name = TusUtils.format_blob_name(
                file_path=filename,
                datetime_var=ts,
                is_hive_partitioned=self.is_hive_partitioned
            )
            full_gcs_path = f"gcs://{self.bucket}/{blob_name}"
            self.log.info(f"Uploading {filename} to {full_gcs_path}")
            hook.upload(
                filename=filename,
                timeout=self.connection_timeout,
                bucket_name=self.bucket,
                object_name=blob_name
            )

            success_log.append(blob_name)
        self.log.info(f"Finish loading files from source {self.src}.")
        return success_log
