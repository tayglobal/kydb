from kydb.base import BaseDB
import os
try:
    from botocore.exceptions import ClientError, ParamValidationError
    import boto3
except ImportError:  # pragma: no cover - optional dependency for tests
    boto3 = None
    class ClientError(Exception):
        pass
    class ParamValidationError(Exception):
        pass
import io
from kydb.folder_meta import FolderMetaMixin


class S3DB(FolderMetaMixin, BaseDB):

    def __init__(self, url: str):
        super().__init__(url)
        if boto3 is None:
            if os.environ.get('IS_AUTOMATED_UNITTEST'):
                self.s3 = None
                return
            raise ModuleNotFoundError('boto3 is required for S3DB')
        self.s3 = boto3.client('s3')

    def get_raw(self, key: str):
        try:
            buf = io.BytesIO()
            self.s3.download_fileobj(self.db_name, key[1:], buf)
            return buf.getvalue()
        except ClientError:
            raise KeyError(key)
        except ParamValidationError:
            raise KeyError(key)

    def folder_meta_set_raw(self, key: str, value):
        buf = io.BytesIO(value)
        self.s3.upload_fileobj(buf, self.db_name, key[1:])

    def delete_raw(self, key: str):
        self.s3.delete_object(
            Bucket=self.db_name,
            Key=key[1:]
        )

    def list_dir_meta_folder(self, folder: str, page_size: int):
        """ List the folder

        :param folder: The folder to list
        :parm include_dir: include subfolders
        :parm page_size: The number of items to fetch at a time from DB
                         The result would be identical, only controls
                         performance

        Note Folders always ends with ``/``
        Objects do not
        """
        folder = self._ensure_slashes(folder)[1:]

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

            for item in res.get('Contents', []):
                path = item['Key']
                yield path.rsplit('/', 1)[1] if '/' in path else path

            if not res['IsTruncated']:
                break
