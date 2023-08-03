import glob
from utils import TusUtils
from datetime import datetime
from airflow.settings import AIRFLOW_HOME
from  airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.microsoft.azure.transfers.local_to_wasb import LocalFilesystemToWasbOperator as LocalFilesystemToWasbOperatorBase
from airflow.utils.context import Context

class LocalFilesystemToAzureBlobStorageOperator(LocalFilesystemToWasbOperatorBase):
    
    def __init__(
        self,
        connection_timeout=14400,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.connection_timeout = connection_timeout

    def execute(self, context: Context) -> None:
        """Upload a file to Azure Blob Storage."""
        hook = WasbHook(wasb_conn_id=self.wasb_conn_id)
        
        glob_list = glob.glob(f'{self.file_path}/*')
        self.log.info(f'Total {len(glob_list)} files found.')
        ts = datetime.fromisoformat(context['ts'])

        success_log = []
        
        for file_path in glob_list:
            blob_name = file_path.split('/')[-1]
            blob_with_hive_partition = f'{ts.year}/{ts.month}/{ts.day}/{ts.hour}/{blob_name}'
            self.log.info(
                "Uploading %s to wasb://%s as %s",
                file_path,
                self.container_name,
                blob_with_hive_partition,
            )
            hook.load_file(
                file_path=file_path,
                container_name=self.container_name,
                blob_name=blob_with_hive_partition,
                create_container=self.create_container,
                connection_timeout=self.connection_timeout,
                **self.load_options,
            )

            success_log.append(file_path)
        
        return success_log
        
