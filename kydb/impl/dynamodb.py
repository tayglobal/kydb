import time
from kydb.base import BaseDB
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from kydb.exceptions import IndexNotSupported
from kydb.folder_meta import FolderMetaMixin
from kydb.query import Entry, FolderQuery, ScanFolderQuery
import boto3

TIME_INDEX = 'folder-time-index'


def gsi_name(index_name: str) -> str:
    """ The GSI that serves ``by(index_name)``.

    A user index gets one sparse GSI per index name, derived rather than
    declared -- ``folder-class_date-index`` for ``by('class_date')`` --
    which is what lets an application add an index without kydb holding
    a registry of them (``user_index_plan.md`` section 9).

    ``mtime`` is the one exception: it keeps the historical
    ``folder-time-index`` name. Deriving ``folder-mtime-index`` for it
    would be tidier and would silently orphan the index on every table
    already in production, so the special case stays.

    This is deliberately the single source of the name for both the
    query path and the "add this index" error message: the message must
    name the index the query actually looked for, or it sends the reader
    to create the wrong one.
    """
    if index_name == 'mtime':
        return TIME_INDEX

    return f'folder-{index_name}-index'


class DynamoDBScanFolderQuery(ScanFolderQuery):
    """ Client-side scan-and-sort over the ``folder-index`` GSI, used
    only behind ``allow_scan=True`` when ``folder-time-index`` is absent
    from the table -- i.e. against a database upgraded to a kydb that has
    recency queries, without adding the new index to the table.

    ``folder-index`` projects ``mtime``/``ctime`` (either via the historical
    ``ALL`` projection or the current ``INCLUDE`` projection), so this costs
    no per-item reads beyond the folder read. Rows written before the feature
    existed carry neither, and sort as epoch exactly as they do in the native
    path.
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
    """ FolderQuery backed by one GSI per index name -- ``mtime`` by
    ``folder-time-index``, a user index by ``folder-<name>-index``
    (see :func:`gsi_name`).

    Every one of those indexes has the same shape: ``folder`` HASH, the
    index attribute RANGE, projection ``INCLUDE`` carrying
    ``mtime``/``ctime``. So a query page carries ``path``, ``folder``,
    the ordering value, ``mtime`` and ``ctime`` -- enough to serve
    iteration and :meth:`entries` off the index page alone. Only
    :meth:`items` costs a read per row, since it returns the stored
    value, which is deliberately kept out of every index so the
    (potentially large, pickled) ``contents`` blob is not duplicated
    into them.

    **``mtime`` has an epoch tail; a user index does not.** Objects with
    no ``mtime`` are absent from the sparse ``folder-time-index``
    entirely -- they predate the feature, or were written while the
    index was disabled in config -- and are recovered from a fallback
    read of ``folder-index`` at ``mtime == ctime == 0``
    (:meth:`_epoch_rows`), because a missing write time still *means*
    something: older than anything the index has tracked.

    A missing user index value means nothing of the sort. An object with
    no ``class_date`` is not a signup for an infinitely distant past
    class; it is not a signup at all. There is no honest position for it
    in a business ordering, and inventing one would put junk in every
    result. So a user index query is strictly sparse in both directions:
    :meth:`_includes_epoch` is unconditionally ``False`` for it, no
    ``folder-index`` fallback is ever read, and ``asc()`` is therefore
    *cheaper* than its ``mtime`` counterpart -- it has no tail to
    discover first, so it need not buffer the whole index pass
    (``user_index_plan.md`` section 5).
    """

    def _full_folder(self) -> str:
        return self._db._ensure_slashes(self._db._get_full_path(self._folder))

    def _check_index_name(self):
        """ Reject an index name that could not name a GSI attribute.

        There is no allow-list of queryable indexes: the GSI name is
        derived from the index name, so any name a write could have used
        is queryable, and whether the index *exists* is the table's
        answer to give (:meth:`_translate_missing_index`), not a
        hardcoded tuple's.

        What is still checked is the name itself, using the same rules
        :meth:`kydb.base.BaseDB._validate_index` applies on write --
        so ``by('contents')`` is rejected here rather than being turned
        into a query against a ``folder-contents-index`` that nothing
        could ever have written.
        """
        name = self._index_name
        if name == 'mtime':
            return

        db = self._db
        if not isinstance(name, str) or not db._INDEX_NAME_RE.match(name):
            raise IndexNotSupported(
                f'Invalid index name {name!r}: an index name must match '
                '[A-Za-z][A-Za-z0-9_]*')

        if name in db._RESERVED_INDEX_NAMES:
            raise IndexNotSupported(
                f'Invalid index name {name!r}: reserved, one of '
                f'{sorted(db._RESERVED_INDEX_NAMES)}')

    def _key_condition(self):
        """ The GSI key condition for this query.

        The sort key is ``self._index_name``, not the literal
        ``'mtime'``: the bounds mean whatever the index means, so on
        ``by('class_date')`` ``since``/``until`` are business dates and
        on ``by('mtime')`` they are nanosecond timestamps.

        Both bounds are inclusive, which is what makes the day-exact
        query ``since(d).until(d)`` a single ``between(d, d)``.
        """
        self._check_index_name()

        cond = Key('folder').eq(self._full_folder())
        sort_key = Key(self._index_name)

        if self._since_ts is not None and self._until_ts is not None:
            # between() is inclusive at both ends, and yields nothing for
            # an inverted range -- the honest answer to an empty range.
            cond = cond & sort_key.between(self._since_ts, self._until_ts)
        elif self._until_ts is not None:
            cond = cond & sort_key.lte(self._until_ts)
        elif self._since_ts is not None:
            # since() is inclusive: entries whose value == ts are included.
            cond = cond & sort_key.gte(self._since_ts)
        return cond

    def _index_rows(self, limit=None):
        """ Lazily page this query's GSI (:func:`gsi_name`).

        Yields ``(name, mtime, ctime, index_value, full_path)`` with
        every number already cast to ``int`` -- boto3 hands back
        ``decimal.Decimal`` for a DynamoDB Number, and letting one
        escape would put a ``Decimal`` in ``Entry.mtime`` or in an
        arithmetic comparison the caller wrote against plain ints
        (``user_index_plan.md`` section 8 #11).

        Honours ``since()``/``until()``/``asc()``/``desc()`` and the
        ``limit`` passed in; a ``limit`` never requests (or fetches)
        more DynamoDB pages than needed to satisfy it.

        Raises ``IndexNotSupported`` if the table has no such GSI --
        see :meth:`_translate_missing_index`.
        """
        key_condition = self._key_condition()
        index_name = self._index_name
        remaining = limit
        start_key = None

        while True:
            if remaining is not None and remaining <= 0:
                return

            kwargs = dict(
                IndexName=gsi_name(index_name),
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
                # mtime/ctime are projected into every one of these
                # indexes (INCLUDE), so no extra read is needed. Rows
                # written before ctime existed fall back to mtime; a row
                # in a user index written with the mtime index disabled
                # in config has neither, and reports the epoch -- it is
                # still a genuine member of the user index, and its
                # position there does not depend on mtime.
                mtime = item.get('mtime')
                mtime = int(mtime) if mtime is not None else 0
                ctime = item.get('ctime')
                ctime = int(ctime) if ctime is not None else mtime
                # The ordering value is the sort key of whichever index
                # was queried, so on by('mtime') it *is* the mtime.
                yield (name, mtime, ctime, int(item[index_name]), full_path)
                if remaining is not None:
                    remaining -= 1
                    if remaining <= 0:
                        return

            start_key = res.get('LastEvaluatedKey')
            if start_key is None:
                return

    def _translate_missing_index(self, err: ClientError):
        """ Turn boto3's error for a missing GSI into a kydb
        ``IndexNotSupported`` naming the table, the index to add, and its
        full key schema.

        Both failures this covers are invisible until someone queries.
        An existing table upgraded to a kydb with recency queries keeps
        working for ``get``/``set``/``delete``/``list_dir`` -- none of
        which touch these GSIs -- and a *user* index is invisible for
        longer still: ``set(index={...})`` is just ``update_item``
        setting an attribute, so writes succeed and accumulate correct
        values with no index in sight. Adding the GSI later picks them
        all up. Either way the first and only symptom is here, and it
        should not surface as a raw boto3 ``ValidationException``.

        Which of the two error codes arrives is not worth predicting:
        real DynamoDB and Moto disagree (Moto returns
        ``ResourceNotFoundException`` where
        ``additional_index_plan.md`` section 9.4 expected
        ``ValidationException`` -- ``user_index_plan.md`` section 8 #10),
        so both are accepted. The ``in str(err)`` guard keeps this from
        swallowing an unrelated validation failure, and holds for a user
        index name because the message names the index that was missing.
        """
        code = err.response.get('Error', {}).get('Code')
        if code not in ('ValidationException', 'ResourceNotFoundException'):
            return

        index = gsi_name(self._index_name)
        if index not in str(err):
            return

        if self._index_name == 'mtime':
            raise IndexNotSupported(
                f'Table {self._db.db_name} has no {index} GSI, so it '
                'cannot serve folder()/recent(). Add the index (folder '
                "HASH, mtime RANGE (Number), projection INCLUDE "
                "['ctime']), or pass allow_scan=True to fall back to an "
                'O(n) scan of folder-index') from err

        raise IndexNotSupported(
            f'Table {self._db.db_name} has no {index} GSI, so it cannot '
            f"serve by({self._index_name!r}). Add the index (folder HASH, "
            f'{self._index_name} RANGE (Number), projection INCLUDE '
            "['mtime', 'ctime']); values already written by "
            f"set(index={{{self._index_name!r}: ...}}) are picked up when "
            'the index backfills') from err

    def _includes_epoch(self) -> bool:
        """ Whether un-indexed objects can appear in this query at all.

        **Always ``False`` for a user index.** The epoch tail exists
        because a missing ``mtime`` still means "written before the
        index existed", which has a defensible position in a recency
        ordering. A missing ``class_date`` has no such meaning -- the
        object is not a badly-dated signup, it is not a signup -- so a
        user index query never emits epoch rows and never reads
        ``folder-index`` as a fallback (``user_index_plan.md``
        section 5). This is what makes user indexes strictly sparse, and
        it is not an optimisation that may be relaxed: emitting a row
        here would put an object that has no value for the queried index
        into a result set ordered by it.

        For ``mtime``, a ``since(ts)`` with ``ts > 0`` excludes every
        epoch row by definition, so the fallback read is skipped in that
        case too -- there, purely as an optimisation.
        """
        if self._index_name != 'mtime':
            return False

        return self._since_ts is None or self._since_ts <= 0

    def _epoch_rows(self, seen):
        """ Yield ``(name, 0, 0, 0, full_path)`` for every object in the
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

        Only ever reached for ``mtime`` -- see :meth:`_includes_epoch`,
        which is the single place that decision is made. The ordering
        value is the ``mtime``, as it is for every ``mtime`` row.
        """
        full_folder = self._full_folder()
        for name in self._db.list_dir(self._folder, include_dir=False):
            if name in seen:
                continue
            yield (name, 0, 0, 0, full_folder + name)

    def _raw_query(self):
        """ Yield ``(name, mtime, ctime, index_value, full_path)`` in
        query order, the indexed rows and the epoch tail together.

        ``allow_scan=True`` cannot serve a user index, and says so.
        The scan fallback reads ``folder-index``, whose projection is
        ``INCLUDE ['mtime', 'ctime']`` -- deliberately fixed, because
        the point of that GSI is to list a folder cheaply. It carries no
        ``class_date``, and nothing it could be widened to would carry
        every user attribute an application might index.

        The alternative was to fetch the attribute per item, which is
        what makes this a judgement call rather than an impossibility.
        It is rejected: ``allow_scan`` today means *one query per folder
        page and zero per-item reads* -- an O(n) scan-and-sort of pages
        already being read -- and a per-item fetch would silently turn
        that into O(n) round trips, on the one path a caller reaches by
        opting into "this is the cheap-enough fallback". The escalation
        would be invisible at the call site and unbounded in the size of
        the folder.

        It is also unnecessary. The scan fallback exists for one
        specific predicament -- an existing table upgraded to a kydb
        with recency queries, where adding ``folder-time-index`` is a
        migration someone has not done yet -- and a user index has no
        such legacy: it did not exist before its GSI did. The honest
        answer is to name the one index to add, which
        :meth:`_translate_missing_index` already does.
        """
        self._check_mtime_index_enabled()

        if self._db._use_scan_fallback(self._allow_scan):
            if self._index_name != 'mtime':
                raise IndexNotSupported(
                    f'allow_scan=True cannot serve by({self._index_name!r}) '
                    'on DynamoDB: the scan fallback reads folder-index, '
                    "whose projection is INCLUDE ['mtime', 'ctime'] and "
                    f'carries no {self._index_name} to sort by. Add the '
                    f'{gsi_name(self._index_name)} GSI (folder HASH, '
                    f'{self._index_name} RANGE (Number), projection '
                    "INCLUDE ['mtime', 'ctime'])")

            for entry in DynamoDBScanFolderQuery(
                    self._db, self._folder, index_name=self._index_name,
                    ascending=self._ascending, since_ts=self._since_ts,
                    until_ts=self._until_ts,
                    limit_n=self._limit_n, allow_scan=True).entries():
                yield (entry.key, entry.mtime, entry.ctime,
                       entry.index_value, self._full_folder() + entry.key)
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
        for name, _mtime, _ctime, _index_value, _full_path in \
                self._raw_query():
            yield name

    def entries(self):
        for name, mtime, ctime, index_value, _full_path in self._raw_query():
            yield Entry(key=name, mtime=mtime, ctime=ctime,
                        index_value=index_value)

    def items(self):
        for name, _mtime, _ctime, _index_value, _full_path in \
                self._raw_query():
            key = self._db._ensure_slashes(self._folder) + name
            yield name, self._db.read(key)


class DynamoDB(FolderMetaMixin, BaseDB):

    # DynamoDB can store caller-supplied index values: they are plain
    # attributes on the object's own item, and one sparse GSI per index
    # name orders them. Without this flag BaseDB.set() would reject
    # ``index=`` before ever reaching folder_meta_set_raw().
    supports_user_index = True

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

    def folder_meta_set_raw(self, key: str, value, index=None):
        """ Write the object as a single ``update_item``.

        ``index`` is compiled into that same expression rather than
        written separately, so an object and every index value it
        carries land in one atomic item update -- the property that made
        an attribute-on-the-item schema worth choosing over separate
        index rows (``user_index_plan.md`` section 3.2).

        A rewrite that does not mention an index **preserves** it. That
        falls straight out of ``update_item``: a ``SET`` clause naming
        only ``contents`` leaves ``class_date`` untouched. The
        alternative -- an unmentioned index is cleared -- would mean any
        incidental rewrite anywhere in an application silently drops the
        object out of the index, which is the failure mode hardest to
        notice. Clearing is therefore explicit, and ``None`` compiles to
        a ``REMOVE``, which both deletes the attribute and drops the row
        from the sparse GSI (section 4.4).

        Index names go through ``ExpressionAttributeNames`` placeholders.
        The name is caller-supplied and DynamoDB has a long list of
        reserved words, so an index legitimately called ``status`` or
        ``size`` would otherwise fail at write time with an error about
        the expression rather than about the name.

        With ``index`` empty the expression is byte-for-byte what it was
        before user indexes existed: no new attributes, no behaviour
        change for existing callers.
        """
        folder = key.rsplit('/', 1)[0] + '/'
        objname = key.rsplit('/', 1)[1]

        # `folder` is referenced via an ExpressionAttributeNames
        # placeholder. This is defensive rather than required -- `folder`
        # is not a DynamoDB reserved word -- but it keeps the expression
        # robust if the attribute is ever renamed to one that is.
        names = {'#f': 'folder'}
        values = {':f': folder, ':c': value}
        set_parts = ['#f=:f', 'contents=:c']
        remove_parts = []

        if not FolderMetaMixin._is_folder_meta(objname) \
                and self.mtime_index_enabled:
            # Directories are excluded from the folder-time-index: no
            # mtime/ctime is written for `.folder-*` marker records, which
            # keeps that (sparse) GSI free of directories automatically.
            #
            # The same path serves a db with `mtime-index: false` in
            # config: nothing is written, so the index costs nothing to
            # maintain, and those objects behave exactly like rows
            # predating the feature if it is ever turned back on.
            #
            # Neither case suppresses user index values: a user index is
            # independent of both the mtime index and its config switch,
            # and `.folder-*` records never carry index values in the
            # first place (FolderMetaMixin.set_raw forwards them only for
            # the object itself).
            values[':t'] = time.time_ns()
            set_parts += ['mtime=:t', 'ctime=if_not_exists(ctime, :t)']

        for name in sorted(index or {}):
            idx_value = index[name]
            placeholder = '#idx_' + name
            names[placeholder] = name
            if idx_value is None:
                remove_parts.append(placeholder)
            else:
                set_parts.append(f'{placeholder}=:idx_{name}')
                values[f':idx_{name}'] = idx_value

        update_expression = 'SET ' + ', '.join(set_parts)
        if remove_parts:
            update_expression += ' REMOVE ' + ', '.join(remove_parts)

        self.table.update_item(
            Key={'path': key},
            UpdateExpression=update_expression,
            ExpressionAttributeNames=names,
            ExpressionAttributeValues=values)

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

        ``by('mtime')`` raises ``IndexNotSupported`` when the index is
        disabled in config: nothing is writing ``mtime``, so there is no
        timestamp for even a scan to sort by. The check runs with the
        query rather than here, so it gates ``mtime`` alone -- a user
        index attribute is written regardless of the setting and stays
        queryable through ``folder-<name>-index``.

        ``by('<user index>')`` on the returned query is served by
        ``folder-<name>-index`` and needs no opt-in; if the table has no
        such GSI, the query raises ``IndexNotSupported`` naming the index
        to add. ``allow_scan=True`` does not help there and says so --
        see :meth:`DynamoDBFolderQuery._raw_query`.
        """
        return DynamoDBFolderQuery(self, folder, allow_scan=allow_scan)

    def reindex(self, folder: str, index_name: str = 'mtime') -> int:
        """Timestamp objects in ``folder`` that have no ``mtime`` yet.

        The existing ``folder-index`` supplies the candidate paths. A
        conditional update adds ``mtime``/``ctime`` without touching the
        payload and without overwriting a concurrent normal write. One
        reindex timestamp is shared by the batch, accurately expressing that
        the original write ordering is unknown.

        **``mtime`` only.** Reindexing works at all because a missing
        ``mtime`` has a defensible replacement: "we do not know when this
        was written, and now is the earliest we can honestly claim". A
        user index has no such value. There is nothing in the object,
        the item or the clock that says which class a booking with no
        ``class_date`` was for, so any number this could stamp would be
        fabricated -- and unlike an epoch ``mtime``, it would be
        indistinguishable from a real one afterwards. The values must
        come from the writer, via ``set(index={...})``; a GSI added later
        backfills every value already written.
        """
        if index_name != 'mtime':
            raise IndexNotSupported(
                f'reindex() cannot recover values for {index_name!r}: a '
                'user index value is supplied by the writer and cannot be '
                'reconstructed from the object. Rewrite the objects with '
                f"set(key, value, index={{{index_name!r}: ...}}); adding "
                f'the {gsi_name(index_name)} GSI later picks up every '
                'value already written')

        if not self.mtime_index_enabled:
            self._raise_mtime_index_disabled()

        full_folder = self._ensure_slashes(self._get_full_path(folder))
        now_ns = time.time_ns()
        count = 0
        start_key = None

        while True:
            kwargs = {
                'IndexName': 'folder-index',
                'KeyConditionExpression': Key('folder').eq(full_folder),
            }
            if start_key is not None:
                kwargs['ExclusiveStartKey'] = start_key

            res = self.table.query(**kwargs)
            for item in res.get('Items', []):
                path = item['path']
                name = path.rsplit('/', 1)[1]
                if FolderMetaMixin._is_folder_meta(name) or \
                        'mtime' in item:
                    continue

                try:
                    self.table.update_item(
                        Key={'path': path},
                        UpdateExpression=(
                            'SET mtime=:t, ctime=if_not_exists(ctime, :t)'),
                        ConditionExpression='attribute_not_exists(mtime)',
                        ExpressionAttributeValues={':t': now_ns})
                except ClientError as err:
                    code = err.response.get('Error', {}).get('Code')
                    if code == 'ConditionalCheckFailedException':
                        # A concurrent writer indexed it after our folder
                        # page was read. It needs no work from reindex.
                        continue
                    raise
                count += 1

            start_key = res.get('LastEvaluatedKey')
            if start_key is None:
                return count

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
