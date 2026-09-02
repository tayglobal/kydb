import time
from kydb.base import BaseDB
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from kydb.exceptions import IndexNotSupported
from kydb.folder_meta import FolderMetaMixin
from kydb.query import Entry, FolderQuery, ScanFolderQuery
import boto3

TIME_INDEX = 'folder-time-index'


class DynamoDBScanFolderQuery(ScanFolderQuery):
    """ Client-side scan-and-sort over the ``folder-index`` GSI, used
    only behind ``allow_scan=True`` when ``folder-time-index`` is absent
    from the table -- i.e. against a database upgraded to a kydb that has
    recency queries, without adding the new index to the table.

    ``folder-index`` has an ``ALL`` projection, so ``mtime``/``ctime``
    come back on the index page itself and this costs no per-item reads
    beyond the folder read. Rows written before the feature existed carry
    neither, and sort as epoch exactly as they do in the native path.
    """

    def _raw_entries(self):
        db = self._db
        folder = db._ensure_slashes(db._get_full_path(self._folder))
        start_key = None
        while True:
            kwargs = {
                'IndexName': 'folder-index',
                'KeyConditionExpression': Key('folder').eq(folder),
            }
            if start_key is not None:
                kwargs['ExclusiveStartKey'] = start_key

            res = db.table.query(**kwargs)
            for item in res.get('Items', []):
                name = item['path'].rsplit('/', 1)[1]
                if FolderMetaMixin._is_folder_meta(name):
                    continue
                mtime = int(item.get('mtime', 0))
                ctime = int(item.get('ctime', mtime))
                yield name, mtime, ctime

            start_key = res.get('LastEvaluatedKey')
            if start_key is None:
                return


class DynamoDBFolderQuery(FolderQuery):
    """ FolderQuery backed by the ``folder-time-index`` GSI.

    The GSI projection is ``INCLUDE ['ctime']``, so a query page carries
    ``path``, ``folder``, ``mtime`` and ``ctime`` -- enough to serve
    iteration and :meth:`entries` off the index page alone. Only
    :meth:`items` costs a read per row, since it returns the stored
    value, which is deliberately kept out of the index so the
    (potentially large, pickled) ``contents`` blob is not duplicated
    into it.

    Objects with no ``mtime`` are absent from this sparse index
    entirely: they predate the feature, or were written while the index
    was disabled in config. They are served from a fallback read of
    ``folder-index`` and reported at ``mtime == ctime == 0`` -- see
    :meth:`_epoch_rows`.
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

    def _index_rows(self, limit=None):
        """ Lazily page ``folder-time-index``.

        Yields ``(name, mtime, ctime, full_path)`` with the timestamps
        already cast to ``int``. Honours
        ``since()``/``asc()``/``desc()`` and the ``limit`` passed in; a
        ``limit`` never requests (or fetches) more DynamoDB pages than
        needed to satisfy it.

        Raises ``IndexNotSupported`` if the table has no
        ``folder-time-index`` -- see :meth:`_translate_missing_index`.
        """
        key_condition = self._key_condition()
        remaining = limit
        start_key = None

        while True:
            if remaining is not None and remaining <= 0:
                return

            kwargs = dict(
                IndexName=TIME_INDEX,
                KeyConditionExpression=key_condition,
                ScanIndexForward=self._ascending)
            if remaining is not None:
                kwargs['Limit'] = remaining
            if start_key is not None:
                kwargs['ExclusiveStartKey'] = start_key

            try:
                res = self._db.table.query(**kwargs)
            except ClientError as e:
                self._translate_missing_index(e)
                raise

            for item in res.get('Items', []):
                full_path = item['path']
                name = full_path.rsplit('/', 1)[1]
                # ctime is projected into the index (INCLUDE), so no
                # extra read is needed. Rows written before ctime existed
                # fall back to mtime.
                ctime = item.get('ctime')
                yield (name, int(item['mtime']),
                       int(ctime) if ctime is not None else int(item['mtime']),
                       full_path)
                if remaining is not None:
                    remaining -= 1
                    if remaining <= 0:
                        return

            start_key = res.get('LastEvaluatedKey')
            if start_key is None:
                return

    def _translate_missing_index(self, err: ClientError):
        """ Turn boto3's error for a missing ``folder-time-index`` into a
        kydb ``IndexNotSupported`` naming the table and the index to add.

        An existing table upgraded to a kydb with recency queries keeps
        working for ``get``/``set``/``delete``/``list_dir`` -- none of
        which touch this GSI -- so the only symptom of not having added
        it is here, and it should not surface as a raw boto3
        ``ValidationException``.
        """
        code = err.response.get('Error', {}).get('Code')
        if code not in ('ValidationException', 'ResourceNotFoundException'):
            return
        if TIME_INDEX not in str(err):
            return

        raise IndexNotSupported(
            f'Table {self._db.db_name} has no {TIME_INDEX} GSI, so it '
            'cannot serve folder()/recent(). Add the index (folder HASH, '
            "mtime RANGE (Number), projection INCLUDE ['ctime']), or pass "
            'allow_scan=True to fall back to an O(n) scan of '
            'folder-index') from err

    def _includes_epoch(self) -> bool:
        """ Whether un-indexed objects can appear in this query at all.

        A ``since(ts)`` with ``ts > 0`` excludes every epoch row by
        definition, so the fallback read is skipped entirely in that case.
        """
        return self._since_ts is None or self._since_ts <= 0

    def _epoch_rows(self, seen):
        """ Yield ``(name, 0, 0, full_path)`` for every object in the
        folder that has no ``folder-time-index`` entry.

        These are objects written before the index existed, or while it
        was disabled in config. The names come from ``list_dir`` (the
        ``folder-index`` GSI), minus the names already served from the
        time index.

        Reporting them at epoch rather than backfilling a timestamp is
        deliberate: no real write time survives anywhere, so epoch is the
        only honest answer -- "older than anything the index has
        tracked". They sort after every indexed row under ``desc()`` and
        before every one under ``asc()``; order *among* them is
        arbitrary, as they all tie on 0.
        """
        full_folder = self._full_folder()
        for name in self._db.list_dir(self._folder, include_dir=False):
            if name in seen:
                continue
            yield (name, 0, 0, full_folder + name)

    def _raw_query(self):
        """ Yield ``(name, mtime, ctime, full_path)`` in query order,
        the indexed rows and the epoch tail together.
        """
        if self._db._use_scan_fallback(self._allow_scan):
            for entry in DynamoDBScanFolderQuery(
                    self._db, self._folder, index_name=self._index_name,
                    ascending=self._ascending, since_ts=self._since_ts,
                    limit_n=self._limit_n, allow_scan=True).entries():
                yield (entry.key, entry.mtime, entry.ctime,
                       self._full_folder() + entry.key)
            return

        if self._ascending:
            yield from self._raw_query_asc()
        else:
            yield from self._raw_query_desc()

    def _raw_query_desc(self):
        """ Newest-first: page the index, then the epoch tail.

        The tail is unreachable until the index has been read to the end,
        which is exactly what makes ``seen`` complete enough to diff
        ``list_dir`` against -- so the dedup costs no extra reads. A
        ``desc().limit(n)`` satisfied entirely from the index never
        touches ``list_dir`` at all.
        """
        remaining = self._limit_n
        seen = set()

        for row in self._index_rows(remaining):
            seen.add(row[0])
            yield row
            if remaining is not None:
                remaining -= 1
                if remaining <= 0:
                    return

        if not self._includes_epoch():
            return

        for row in self._epoch_rows(seen):
            yield row
            if remaining is not None:
                remaining -= 1
                if remaining <= 0:
                    return

    def _raw_query_asc(self):
        """ Oldest-first: the epoch tail comes *first*.

        Which names are un-indexed cannot be known without reading the
        whole time index, so an ``asc()`` query that can include epoch
        rows is O(folder) and buffers the index pass -- documented in
        plan section 9.3 rather than engineered around. ``asc()`` is the
        rarer query, and the cost disappears as a folder converges on
        being fully indexed.
        """
        if not self._includes_epoch():
            yield from self._index_rows(self._limit_n)
            return

        indexed = list(self._index_rows())
        seen = {row[0] for row in indexed}
        remaining = self._limit_n

        for row in self._epoch_rows(seen):
            yield row
            if remaining is not None:
                remaining -= 1
                if remaining <= 0:
                    return

        for row in indexed:
            yield row
            if remaining is not None:
                remaining -= 1
                if remaining <= 0:
                    return

    def __iter__(self):
        for name, _mtime, _ctime, _full_path in self._raw_query():
            yield name

    def entries(self):
        for name, mtime, ctime, _full_path in self._raw_query():
            yield Entry(key=name, mtime=mtime, ctime=ctime)

    def items(self):
        for name, _mtime, _ctime, _full_path in self._raw_query():
            key = self._db._ensure_slashes(self._folder) + name
            yield name, self._db.read(key)


class DynamoDB(FolderMetaMixin, BaseDB):

    def __init__(self, url: str):
        super().__init__(url)
        dynamodb = boto3.resource('dynamodb')
        self.table = dynamodb.Table(self.db_name)
        # Populated lazily by _has_time_index(), and only on the
        # allow_scan=True path.
        self.__has_time_index = None

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
        if FolderMetaMixin._is_folder_meta(objname) \
                or not self.mtime_index_enabled:
            # Directories are excluded from the folder-time-index: no
            # mtime/ctime is written for `.folder-*` marker records, which
            # keeps that (sparse) GSI free of directories automatically.
            #
            # The same path serves a db with `mtime-index: false` in
            # config: nothing is written, so the index costs nothing to
            # maintain, and those objects behave exactly like rows
            # predating the feature if it is ever turned back on.
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

    def folder(self, folder: str, allow_scan: bool = False) \
            -> DynamoDBFolderQuery:
        """ Implements folder in KYDBInterface, backed by
        ``folder-time-index``.

        ``allow_scan=True`` opts into an O(n) client-side scan-and-sort
        of ``folder-index``, used only when the table has no
        ``folder-time-index`` -- an existing table upgraded to a kydb
        with recency queries, without the new GSI added. With the index
        present, the native path is always used and ``allow_scan`` costs
        nothing.

        Raises ``IndexNotSupported`` when the index is disabled in
        config: nothing is writing ``mtime``, so there is no timestamp
        for even a scan to sort by.
        """
        if not self.mtime_index_enabled:
            self._raise_mtime_index_disabled()

        return DynamoDBFolderQuery(self, folder, allow_scan=allow_scan)

    def _use_scan_fallback(self, allow_scan: bool) -> bool:
        """ Whether to serve this query by scanning ``folder-index``.

        Only ever true behind ``allow_scan=True``, and only when the
        table genuinely lacks ``folder-time-index``. The check needs
        ``dynamodb:DescribeTable``, which some deployments do not grant,
        so a failed describe is treated as "assume the index is there":
        the native path then either works, or raises the clear
        ``IndexNotSupported`` from
        :meth:`DynamoDBFolderQuery._translate_missing_index`. That keeps
        a missing IAM permission from turning into a hard failure.
        """
        if not allow_scan:
            return False

        return not self._has_time_index()

    def _has_time_index(self) -> bool:
        """ Whether the table carries the ``folder-time-index`` GSI.

        Cached: the table schema does not change under a live
        connection, and this is on the path of every ``allow_scan=True``
        query.
        """
        if self.__has_time_index is None:
            try:
                desc = self.table.meta.client.describe_table(
                    TableName=self.db_name)
                indexes = desc['Table'].get('GlobalSecondaryIndexes') or []
                self.__has_time_index = any(
                    i['IndexName'] == TIME_INDEX for i in indexes)
            except ClientError:
                # No DescribeTable permission (or the call failed for
                # any other reason) -- assume present, see the docstring
                # on _use_scan_fallback.
                self.__has_time_index = True

        return self.__has_time_index

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
