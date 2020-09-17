from kydb.base import BaseDB
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
            return buf.getvalue()
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

    def is_dir(self, folder: str) -> bool:
        return any(self.list_dir(folder, page_size=1))

    def list_dir(self, folder: str, include_dir=True, page_size=200):
        """ List the folder

        :param folder: The folder to lsit
        :parm include_dir: include subfolders
        :parm page_size: The number of items to fetch at a time from DB
                         The result would be identical, only controls
                         performance

        Note Folders always ends with ``/``
        Objects does not
        """
        folder = self._ensure_slashes(folder)

        res = {}

        while True:
            kwargs = {
                'Bucket': self.db_name,
                'Delimiter': '/',
                'MaxKeys': page_size,
                'Prefix': folder
            }

            token = res.get('NextContinuationToken')

            if token:
                kwargs['ContinuationToken'] = token

            res = self.s3.list_objects_v2(**kwargs)

            if include_dir:
                for item in res.get('CommonPrefixes', []):
                    yield item['Prefix'].rsplit('/', 2)[1] + '/'

            for item in res.get('Contents', []):
                yield item['Key'].rsplit('/', 1)[1]

            if not res['IsTruncated']:
                break

    def rmdir(self, key: str):
        """Do nothing. S3 doesn't have folders"""
