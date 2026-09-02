from kydb.base import BaseDB
from botocore.exceptions import ClientError, ParamValidationError
import io
import boto3
from kydb.exceptions import IndexNotSupported
from kydb.folder_meta import FolderMetaMixin
from kydb.query import ScanFolderQuery


class S3FolderQuery(ScanFolderQuery):
    """ Client-side scan-and-sort over S3, sorted by ``LastModified`` --
    ``list_objects_v2`` offers no server-side ordering. S3 has no
    separate creation timestamp either, so ``ctime`` falls back to
    ``mtime`` (``LastModified``), matching Files above. Note
    ``LastModified`` is only second-resolution, both on real S3 and
    under Moto, so two writes within the same second sort arbitrarily
    against each other -- the same clock-skew/ties caveat the plan
    documents for DynamoDB's client-side ``time.time_ns()``, just
    coarser here.
    """

    def _raw_entries(self):
        db = self._db
        prefix = db._ensure_slashes(db._get_full_path(self._folder))[1:]
        kwargs = {'Bucket': db.db_name, 'Delimiter': '/', 'Prefix': prefix}
        token = None
        while True:
            if token:
                kwargs['ContinuationToken'] = token
            res = db.s3.list_objects_v2(**kwargs)
            for item in res.get('Contents', []):
                key = item['Key']
                name = key.rsplit('/', 1)[1] if '/' in key else key
                if FolderMetaMixin._is_folder_meta(name):
                    # Directories (folder-meta marker objects) are
                    # excluded from recency results, matching DynamoDB's
                    # sparse-index behaviour.
                    continue
                ts_ns = int(item['LastModified'].timestamp() * 1_000_000_000)
                yield name, ts_ns, ts_ns

            if not res.get('IsTruncated'):
                return
            token = res.get('NextContinuationToken')


class S3DB(FolderMetaMixin, BaseDB):

    def __init__(self, url: str):
        super().__init__(url)
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

        :param folder: The folder to lsit
        :parm include_dir: include subfolders
        :parm page_size: The number of items to fetch at a time from DB
                         The result would be identical, only controls
                         performance

        Note Folders always ends with ``/``
        Objects does not
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

    def folder(self, folder: str, allow_scan: bool = False) \
            -> S3FolderQuery:
        """ Implements folder in KYDBInterface.

        S3 has no server-side ordering index -- ``list_objects_v2``
        offers no sort order -- so this is a client-side scan-and-sort
        by ``LastModified``, requiring the caller to opt in with
        ``allow_scan=True``.
        """
        if not allow_scan:
            raise IndexNotSupported(
                f'{type(self).__name__} does not support folder()/'
                "recent() natively; pass allow_scan=True to opt into "
                'an O(n) client-side scan-and-sort of the folder')
        return S3FolderQuery(self, folder, allow_scan=True)

    def reindex(self, folder: str) -> int:
        """No-op: S3 ``LastModified`` already covers existing objects."""
        return 0
