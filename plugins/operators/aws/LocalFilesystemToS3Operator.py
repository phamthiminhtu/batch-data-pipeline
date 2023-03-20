from __future__ import annotations
from typing import TYPE_CHECKING, Sequence
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator as LocalFilesystemToS3OperatorBase
if TYPE_CHECKING:
    from airflow.utils.context import Context


class LocalFilesystemToS3Operator(LocalFilesystemToS3OperatorBase):
    """
    Uploads a file from a local filesystem to Amazon S3.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:LocalFilesystemToS3Operator`

    :param filename: Path to the local file. Path can be either absolute
            (e.g. /path/to/file.ext) or relative (e.g. ../../foo/*/*.csv). (templated)
    :param dest_key: The key of the object to copy to. (templated)

        It can be either full s3:// style url or relative path from root level.

        When it's specified as a full s3:// url, please omit `dest_bucket`.
    :param dest_bucket: Name of the S3 bucket to where the object is copied. (templated)
    :param aws_conn_id: Connection id of the S3 connection to use
    :param verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.

        You can provide the following values:

        - False: do not validate SSL certificates. SSL will still be used,
                 but SSL certificates will not be
                 verified.
        - path/to/cert/bundle.pem: A filename of the CA cert bundle to uses.
                 You can specify this argument if you want to use a different
                 CA cert bundle than the one used by botocore.
    :param replace: A flag to decide whether or not to overwrite the key
            if it already exists. If replace is False and the key exists, an
            error will be raised.
    :param encrypt: If True, the file will be encrypted on the server-side
        by S3 and will be stored in an encrypted form while at rest in S3.
    :param gzip: If True, the file will be compressed locally
    :param acl_policy: String specifying the canned ACL policy for the file being
        uploaded to the S3 bucket.
    """

    def __init__(
        self,
        **kwargs
    ):
        super().__init__(**kwargs)

    def execute(self, context: Context):
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
        s3_bucket, s3_key = s3_hook.get_s3_bucket_key(
            self.dest_bucket, self.dest_key, "dest_bucket", "dest_key"
        )
        ts = datetime.fromisoformat(context['ts'])
        s3_key_with_hive_partition = f'{ts.year}/{ts.month}/{ts.day}/{ts.hour}/{s3_key}'

        s3_hook.load_file(
            self.filename,
            s3_key_with_hive_partition,
            s3_bucket,
            self.replace,
            self.encrypt,
            self.gzip,
            self.acl_policy,
        )