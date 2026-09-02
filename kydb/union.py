
from .base import BaseDB
from .exceptions import IndexNotSupported
from .interface import KYDBInterface
from .query import FolderQuery
from typing import Tuple
from contextlib import ExitStack
import heapq


def front_db_func(self, func_name, *args, **kwargs):
    return getattr(self.dbs[0], func_name)(*args, **kwargs)


def first_success_db_func(self, func_name, *args, **kwargs):
    first_error = None
    for db in self.dbs:
        try:
            return getattr(db, func_name)(*args, **kwargs)
        except KeyError as err:
            if not first_error:
                first_error = err
            continue

    raise first_error


def any_db_func(self, func_name, *args, **kwargs):
    return any(getattr(db, func_name)(*args, **kwargs) for db in self.dbs)


def all_db_func(self, func_name, *args, **kwargs):
    for db in self.dbs:
        getattr(db, func_name)(*args, **kwargs)


def create_func(func_prototype, func_name):
    # Using partial loses the self when constructing
    # class using type. So use this function
    def f(self, *args, **kwargs):
        return func_prototype(self, func_name, *args, **kwargs)

    return f


UNION_DB_BASE_FUNCS = [
    ('__getitem__', first_success_db_func),
    ('__setitem__', front_db_func),
    ('delete', front_db_func),
    ('rmdir', front_db_func),
    ('rm_tree', front_db_func),
    ('new', front_db_func),
    ('exists', any_db_func),
    ('refresh', all_db_func),
    ('read', first_success_db_func),
    ('mkdir', front_db_func),
    ('is_dir', any_db_func),
    ('upload_objdb_config', front_db_func)
]

UnionDBBase = type(
    'UnionDBBase',
    (KYDBInterface,),
    {k: create_func(v, k) for k, v in UNION_DB_BASE_FUNCS}
)


class UnionFolderQuery(FolderQuery):
    """ FolderQuery over a :class:`UnionDB`.

    Merges each member db's own (already sorted) ``FolderQuery`` via
    ``heapq.merge``, in ``mtime`` order. On a name collision between
    member dbs, the *front* db wins -- matching
    ``first_success_db_func``, the same rule ``db['/foo']`` already
    follows.

    Front-db-wins is resolved before the merge, not by it: for each db
    in front-to-back order, any key already claimed by an earlier db is
    dropped from that db's own entries before it is handed to
    ``heapq.merge``. That guarantees the surviving entry for a
    duplicated key carries its winning (front) db's own ``mtime`` --
    not whichever db's entry for that name happened to sort first.
    ``heapq.merge`` then only has to interleave already
    mutually-exclusive-by-key sequences into one sorted stream; no
    further dedup step is needed after it.

    ``UnionDB.list_dir`` collects into a ``set``, which destroys
    ordering -- that approach is deliberately not reused here.
    """

    _SUPPORTED_INDEXES = ('mtime',)

    def _sub_query(self, db):
        """ Build the per-db FolderQuery matching this query's own
        chain state (index/order/since), or return None if `db` doesn't
        support it.
        """
        try:
            q = db.folder(self._folder, allow_scan=self._allow_scan)
        except IndexNotSupported:
            return None

        q = q.by(self._index_name)
        q = q.asc() if self._ascending else q.desc()
        if self._since_ts is not None:
            q = q.since(self._since_ts)
        return q

    def _claim_entries(self, query, seen: set):
        """ Yield `query`'s entries not already claimed by an earlier
        (front) db, marking each one claimed in `seen` as it goes.
        """
        try:
            for entry in query.entries():
                if entry.key in seen:
                    continue
                seen.add(entry.key)
                yield entry
        except KeyError:
            # Matches UnionDB.list_dir: a member db that doesn't have
            # this folder at all is simply skipped.
            return

    def entries(self):
        if self._index_name not in self._SUPPORTED_INDEXES:
            raise IndexNotSupported(
                "UnionDB folder query only supports by('mtime'), got "
                f"by({self._index_name!r})")

        seen = set()
        per_db_entries = []

        for db in self._db.dbs:
            query = self._sub_query(db)
            if query is None:
                continue
            per_db_entries.append(list(self._claim_entries(query, seen)))

        if not per_db_entries:
            raise IndexNotSupported(
                'UnionDB: no member db supports folder()/recent() '
                'recency queries' +
                ('' if self._allow_scan else
                 ' without allow_scan=True'))

        merged = heapq.merge(
            *per_db_entries, key=lambda e: e.mtime,
            reverse=not self._ascending)

        count = 0
        for entry in merged:
            yield entry
            count += 1
            if self._limit_n is not None and count >= self._limit_n:
                return

    def items(self):
        for entry in self.entries():
            key = BaseDB._ensure_slashes(self._folder) + entry.key
            yield entry.key, self._db.read(key)


class UnionDB(UnionDBBase):
    """UnionDB


The URL used on *connect* can be a semi-colon separated string.

This would create a Union Database.

Connecting::

    db = kydb.connect('memory://unittest;s3://my-unittest-fixture')

OR::

    db = kydb.connect('redis://hotfixes.epythoncloud.io;'
                      'dynamodb://my-prod-src-db')

Reading and writing::

    db1, db2 = db.dbs
    db1['/foo'] = 1
    db2['/bar'] = 2

    (db['/foo'], db['/bar']) # return (1, 2)

    # Although db2 has /foo, it is db1's /foo that the union returns
    db2['/foo'] = 3
    db['/foo'] # return 1

    # writing always happens on the front db
    db['/foo'] = 4
    db1['/foo'] # returns 4
    db2['/foo'] # returns 3
    """

    def __init__(self, dbs: Tuple[BaseDB]):
        self.dbs = dbs

    def cache_context(self) -> 'KYDBInterface':
        with ExitStack() as stack:
            for db in self.dbs:
                stack.enter_context(db.cache_context())

        return stack

    def list_dir(self, folder: str, include_dir=True, page_size=200):
        res = set()
        for db in self.dbs:
            try:
                res.update(db.list_dir(folder, include_dir, page_size))
            except KeyError:
                pass

        for key in res:
            yield key

    def ls(self, folder: str, include_dir=True):
        return list(self.list_dir(folder, include_dir))

    def folder(self, folder: str, allow_scan: bool = False) \
            -> UnionFolderQuery:
        """ Implements folder in KYDBInterface.

        Merges the per-db recency queries -- see
        :class:`UnionFolderQuery`. Support is per-member: a union where
        *no* member supports folder()/recent() still raises
        ``IndexNotSupported`` (checked eagerly here, matching every
        other backend's ``folder()``); a union where only *some* members
        support it works over those that do.
        """
        supported = False
        for db in self.dbs:
            try:
                db.folder(folder, allow_scan=allow_scan)
                supported = True
                break
            except IndexNotSupported:
                continue

        if not supported:
            raise IndexNotSupported(
                'UnionDB: no member db supports folder()/recent() '
                'recency queries' +
                ('' if allow_scan else ' without allow_scan=True'))

        return UnionFolderQuery(self, folder, allow_scan=allow_scan)

    def recent(self, folder: str, limit: int = None,
               allow_scan: bool = False):
        """ Implements recent in KYDBInterface.

        Sugar for
        ``self.folder(folder, allow_scan=allow_scan).by('mtime').desc().limit(limit)``.
        """
        query = self.folder(folder, allow_scan=allow_scan) \
            .by('mtime').desc()
        if limit is not None:
            query = query.limit(limit)
        return query

    def __repr__(self):
        """
        The representation of the db.

        i.e. <UnionDB redis://my-redis-host/source;
              kydb.S3 s3://my-s3-prod-source>
        """
        return f'<{type(self).__name__} ' + \
            ';'.join(db.url for db in self.dbs) + '>'
