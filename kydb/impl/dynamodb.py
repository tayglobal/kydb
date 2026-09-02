import time
from kydb.base import BaseDB
from boto3.dynamodb.conditions import Key
from kydb.exceptions import IndexNotSupported
from kydb.folder_meta import FolderMetaMixin
from kydb.query import Entry, FolderQuery
import boto3


class DynamoDBFolderQuery(FolderQuery):
    """ FolderQuery backed by the ``folder-time-index`` GSI.

    ``folder-index``/``folder-time-index`` semantics: the GSI is
    ``KEYS_ONLY``, so a query page only ever carries ``path``, ``folder``
    and ``mtime``. ``ctime`` (needed by :meth:`entries`) and the object
    value (needed by :meth:`items`) require one extra per-item fetch --
    the index projection is deliberately not widened to ``ALL`` to keep
    the (potentially large, pickled) ``contents`` blob out of the index.

    Plain iteration (bare names, e.g. via ``db.recent(...)`` or
    ``for name in db.folder(...)``) needs neither, so it stays on the raw
    index page with no extra round trip per item.
    """

    _SUPPORTED_INDEXES = ('mtime',)

    def _full_folder(self) -> str:
        return self._db._ensure_slashes(self._db._get_full_path(self._folder))

    def _key_condition(self):
        if self._index_name not in self._SUPPORTED_INDEXES:
            raise IndexNotSupported(
                "DynamoDB folder query only supports by('mtime'), got "
                f"by({self._index_name!r})")

        cond = Key('folder').eq(self._full_folder())
        if self._since_ts is not None:
            # since() is inclusive: entries with mtime == ts are included.
            cond = cond & Key('mtime').gte(self._since_ts)
        return cond

    def _raw_query(self):
        """ Lazily page ``folder-time-index``.

        Yields ``(name, mtime, full_path)`` with ``mtime`` already cast
        to ``int``. Honours ``since()``/``limit()``/``asc()``/``desc()``;
        a ``limit()`` never requests (or fetches) more DynamoDB pages
        than needed to satisfy it.
        """
        key_condition = self._key_condition()
        remaining = self._limit_n
        start_key = None

        while True:
            if remaining is not None and remaining <= 0:
                return

            kwargs = dict(
                IndexName='folder-time-index',
                KeyConditionExpression=key_condition,
                ScanIndexForward=self._ascending)
            if remaining is not None:
                kwargs['Limit'] = remaining
            if start_key is not None:
                kwargs['ExclusiveStartKey'] = start_key

            res = self._db.table.query(**kwargs)

            for item in res.get('Items', []):
                full_path = item['path']
                name = full_path.rsplit('/', 1)[1]
                yield name, int(item['mtime']), full_path
                if remaining is not None:
                    remaining -= 1
                    if remaining <= 0:
                        return

            start_key = res.get('LastEvaluatedKey')
            if start_key is None:
                return

    def __iter__(self):
        for name, _mtime, _full_path in self._raw_query():
            yield name

    def entries(self):
        for name, mtime, full_path in self._raw_query():
            # KEYS_ONLY does not project ctime; fetch it narrowly rather
            # than widening the index projection or pulling `contents`.
            item = self._db.table.get_item(
                Key={'path': full_path},
                ProjectionExpression='ctime',
            ).get('Item', {})
            ctime = int(item['ctime']) if 'ctime' in item else mtime
            yield Entry(key=name, mtime=mtime, ctime=ctime)

    def items(self):
        for name, _mtime, _full_path in self._raw_query():
            key = self._db._ensure_slashes(self._folder) + name
            yield name, self._db.read(key)


class DynamoDB(FolderMetaMixin, BaseDB):

    def __init__(self, url: str):
        super().__init__(url)
        dynamodb = boto3.resource('dynamodb')
        self.table = dynamodb.Table(self.db_name)

    def get_raw(self, key):
        items = self.table.query(
            KeyConditionExpression=Key('path').eq(key))['Items']

        if not items:
            raise KeyError(key)

        return items[0]['contents'].value

    def folder_meta_set_raw(self, key: str, value):
        folder = key.rsplit('/', 1)[0] + '/'
        objname = key.rsplit('/', 1)[1]

        # `folder` is referenced via an ExpressionAttributeNames
        # placeholder. This is defensive rather than required -- `folder`
        # is not a DynamoDB reserved word -- but it keeps the expression
        # robust if the attribute is ever renamed to one that is.
        if FolderMetaMixin._is_folder_meta(objname):
            # Directories are excluded from the folder-time-index: no
            # mtime/ctime is written for `.folder-*` marker records, which
            # keeps that (sparse) GSI free of directories automatically.
            self.table.update_item(
                Key={'path': key},
                UpdateExpression='SET #f=:f, contents=:c',
                ExpressionAttributeNames={'#f': 'folder'},
                ExpressionAttributeValues={':f': folder, ':c': value})
        else:
            now_ns = time.time_ns()
            self.table.update_item(
                Key={'path': key},
                UpdateExpression=(
                    'SET #f=:f, contents=:c, mtime=:t, '
                    'ctime=if_not_exists(ctime, :t)'),
                ExpressionAttributeNames={'#f': 'folder'},
                ExpressionAttributeValues={
                    ':f': folder, ':c': value, ':t': now_ns})

    def delete_raw(self, key: str):
        self.table.delete_item(Key={
            'path': key,
        })

    def folder(self, folder: str) -> DynamoDBFolderQuery:
        """ Implements folder in KYDBInterface, backed by
        ``folder-time-index``. """
        return DynamoDBFolderQuery(self, folder)

    def list_dir_meta_folder(self, folder: str, page_size: int):
        folder = self._ensure_slashes(folder)

        done = False
        start_key = None
        while not done:
            kwargs = {
                'IndexName': "folder-index",
                'KeyConditionExpression': Key('folder').eq(folder),
                'Limit': page_size
            }

            if start_key:
                kwargs['ExclusiveStartKey'] = start_key

            res = self.table.query(**kwargs)

            for item in res.get('Items', []):
                yield item['path'].rsplit('/', 1)[1]

            start_key = res.get('LastEvaluatedKey', None)
            done = start_key is None
