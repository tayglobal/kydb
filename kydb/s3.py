from .base import BaseDB
from botocore.exceptions import ClientError
import io
import boto3


class S3DB(BaseDB):

    def __init__(self, url: str):
        super().__init__(url)
        self.s3 = boto3.client('s3')

    def get_raw(self, key: str):
        try:
            buf = io.BytesIO()
            self.s3.download_fileobj(self.db_name, key, buf)
        except ClientError:
            raise KeyError(key)

    def set_raw(self, key: str, value):
        buf = io.BytesIO(value)
        self.s3.upload_fileobj(buf, self.db_name, key)

    def _get_s3_path(self, key: str):
        return 's3://' + self.db_name + self._get_full_path(key)

    def delete_raw(self, key: str):
        self.s3.delete_object(
            Bucket=self.db_name,
            Key=key
        )
