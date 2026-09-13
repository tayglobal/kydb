
from .base import BaseDB
from .exceptions import IndexNotSupported
from .interface import KYDBInterface
from .query import FolderQuery
from typing import Tuple
from contextlib import ExitStack
import heapq
import logging

#: Library logger. kydb configures no handlers -- an application decides
#: where these go. The one WARNING emitted here flags a real, documented
#: correctness limitation (see :class:`UnionFolderQuery`), so silence it
#: deliberately rather than by accident::
#:
#:     logging.getLogger('kydb.union').setLevel(logging.ERROR)
logger = logging.getLogger(__name__)


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
    # `set` writes to the front db, exactly as `__setitem__` does -- and
    # because front_db_func forwards **kwargs verbatim, the keyword-only
    # `index=` reaches that db unchanged. A union has no index of its
    # own to maintain: the values live on the object, in the db that
    # holds it, and `UnionFolderQuery` reads them back from there.
    ('set', front_db_func),
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
    ``heapq.merge``, in the order of the *queried index*. On a name
    collision between member dbs, the *front* db wins -- matching
    ``first_success_db_func``, the same rule ``db['/foo']`` already
    follows.

    The merge key is ``Entry.index_value``, not ``Entry.mtime``. Each
    member has already sorted its own entries by whatever ``by()``
    asked for, and ``heapq.merge`` only produces a sorted stream if it
    is handed the same key those sub-sequences were sorted on. For
    ``by('mtime')`` the two are equal -- every backend reports
    ``index_value == mtime`` for an ``mtime`` query -- so this is a
    no-op there and the only correct answer for ``by('class_date')``,
    where merging on ``mtime`` would interleave three sorted rosters
    into one unsorted one.

    Front-db-wins is resolved before the merge, not by it: for each db
    in front-to-back order, any key already claimed by an earlier db is
    dropped from that db's own entries before it is handed to
    ``heapq.merge``. That guarantees the surviving entry for a
    duplicated key carries its winning (front) db's own ``mtime`` --
    not whichever db's entry for that name happened to sort first.
    ``heapq.merge`` then only has to interleave already
    mutually-exclusive-by-key sequences into one sorted stream; no
    further dedup step is needed after it. (The same holds for the
    ordering value: the surviving entry carries the front db's
    ``index_value``, not the shadowed one's.)

    **Known limitation, deliberately not fixed.** That claiming happens
    among the entries the members *return*, and each member applies
    ``since``/``until`` itself. So a bounded query that excludes the
    front db's copy of a name lets a shadowed back-db copy through --
    both the Saturday and the Monday roster list a student whose two
    copies disagree. It is reachable with ``by('mtime').since(...)``
    too, so it predates user indexes; see ``user_index_plan.md`` §11.

    Closing it means resolving key ownership before filtering. The cheap
    form is an existence check per surviving row against each db in
    front of its own -- bounded by the result size rather than the
    folder -- but ``exists()`` here is a full ``get_raw`` in a
    ``try``/``except`` (``BaseDB.exists_raw``), so "cheap" is still a
    read per row, and on an unbounded query it converges on the full
    listing anyway. Neither cost belongs on every union query by
    default.

    So the query stays fast and says so instead: :meth:`_log_index_use`
    emits one ``logging`` WARNING per ``(folder, index)`` when the shape
    can actually hit this -- more than one member db *and* at least one
    bound. An unbounded query returns every member's every row, which
    makes front-db-wins complete and the result correct, so it warns at
    DEBUG only.

    ``UnionDB.list_dir`` collects into a ``set``, which destroys
    ordering -- that approach is deliberately not reused here.
    """

    def _supports_index(self, name: str) -> bool:
        """ Whether *any* member db can answer ``by(name)``.

        A union is only as capable as its members, and it has always
        been partially capable on purpose: a union of DynamoDB and Files
        answers ``by('mtime')`` from the member that has an index and
        skips the one that does not. A user index is the same rule, so
        this asks the members rather than consulting a hardcoded tuple.

        ``mtime`` support is decided per member by whether
        :meth:`_sub_query` can build a query at all -- ``folder()``
        raises ``IndexNotSupported`` on a member that cannot serve it --
        so it is admitted here and settled there. A user index is
        settled by :attr:`kydb.base.BaseDB.supports_user_index`, which
        is the same flag the write side gates on: a member that could
        never have stored the value cannot be asked to order by it.
        """
        return any(self._db_supports_index(db, name)
                   for db in self._db.dbs)

    def _db_supports_index(self, db, name: str = None) -> bool:
        """ Whether one member db can answer ``by(name)``.

        ``mtime`` is left to :meth:`_sub_query`, whose ``folder()`` call
        is the member's own answer to "can I do recency queries at all".
        For a user index the folder query would be built happily and
        only fail later, inside ``entries()`` -- Files and S3 have
        nowhere to store the value but no reason to refuse a
        ``by()`` -- so the capability flag is checked up front instead.
        """
        if name is None:
            name = self._index_name

        if name == 'mtime':
            # A member with `mtime-index: false` records no timestamps,
            # so it has nothing to order by and is skipped -- the same
            # per-member partial capability as a member with no index at
            # all. Asked here rather than left to `folder()`, which no
            # longer raises for it: that gate moved to the point the
            # query runs, so it can tell `by('mtime')` from
            # `by('class_date')`.
            return getattr(db, 'mtime_index_enabled', True)

        return getattr(db, 'supports_user_index', False)

    def _sub_query(self, db):
        """ Build the per-db FolderQuery matching this query's own
        chain state (index/order/since/until), or return None if `db`
        doesn't support it.

        Every bound is propagated. ``until`` in particular must reach
        each member: the union filters nothing itself, so a bound the
        members do not apply is a bound that is not applied at all --
        ``since(d).until(d)`` would quietly widen to ``since(d)``.

        ``limit`` is deliberately *not* propagated. The merge draws from
        every member and drops keys already claimed by a front db, so a
        member truncated to ``n`` rows can starve the merged result of
        entries it still needed.
        """
        if not self._db_supports_index(db):
            return None

        try:
            q = db.folder(self._folder, allow_scan=self._allow_scan)
        except IndexNotSupported:
            return None

        q = q.by(self._index_name)
        q = q.asc() if self._ascending else q.desc()
        if self._since_ts is not None:
            q = q.since(self._since_ts)
        if self._until_ts is not None:
            q = q.until(self._until_ts)
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

    def _log_index_use(self):
        """ Record that a union index query ran, and warn when its shape
        can actually hit the shadowing limitation.

        The limitation needs two things to bite: more than one member
        db, so a key *can* be held twice, and at least one bound, so a
        front db's copy can be filtered out before it suppresses the
        back db's. An unbounded query returns every member's every row,
        which makes front-db-wins complete and the result correct -- so
        warning on one would be crying wolf on the common
        ``recent(limit=10)``.

        Warned once per ``(folder, index)`` per union instance, not once
        per query. Databases are cached by URL in ``kydb.api._db_cache``
        and outlive any one call, so this is effectively once per
        process per query shape: enough to be seen, not enough to bury a
        polling loop in duplicates.
        """
        logger.debug(
            'UnionDB folder query: folder=%s by=%s members=%d '
            'since=%s until=%s',
            self._folder, self._index_name, len(self._db.dbs),
            self._since_ts, self._until_ts)

        bounded = self._since_ts is not None or self._until_ts is not None
        if len(self._db.dbs) < 2 or not bounded:
            return

        warned = getattr(self._db, '_shadow_warned', None)
        if warned is None:
            warned = set()
            self._db._shadow_warned = warned

        key = (self._folder, self._index_name)
        if key in warned:
            return
        warned.add(key)

        logger.warning(
            "UnionDB bounded query on %r by(%r): a key held by more than "
            "one member db can appear in the result even when the front "
            "db's own copy falls outside the bounds. Each member applies "
            "since/until itself, so front-db-wins is resolved among the "
            "rows that survive filtering, and a back db's shadowed copy "
            "is not suppressed by a front db copy the bounds excluded. "
            "Re-read a row with db[key] before treating its index value "
            "as authoritative. Documented limitation, not a transient "
            "error: user_index_plan.md section 11.",
            self._folder, self._index_name)

    def entries(self):
        if not self._supports_index(self._index_name):
            if self._index_name == 'mtime':
                raise IndexNotSupported(
                    'UnionDB: no member db is maintaining the mtime '
                    'index (mtime-index: false in the kydb config for '
                    'every member), so there are no timestamps to '
                    'order by')

            raise IndexNotSupported(
                f'UnionDB: no member db supports by('
                f'{self._index_name!r}); a user index needs a member '
                'that can store index values (DynamoDB, Redis, Memory)')

        self._log_index_use()

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

        # The merge key is the value each member sorted by -- see the
        # class docstring. `index_value` equals `mtime` for by('mtime'),
        # so this is unchanged for every query that predates user
        # indexes.
        merged = heapq.merge(
            *per_db_entries, key=lambda e: e.index_value,
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

    def reindex(self, folder: str) -> int:
        """Reindex every member that maintains recency metadata.

        A union query can yield objects from any member, so only rebuilding
        the front database would leave legacy objects in later members at the
        epoch tail. Members such as Files/S3 report zero; unsupported members
        are skipped. If no member supports reindexing, the union is
        unsupported too.
        """
        count = 0
        supported = False
        for db in self.dbs:
            try:
                count += db.reindex(folder)
                supported = True
            except IndexNotSupported:
                continue

        if not supported:
            raise IndexNotSupported(
                'UnionDB: no member db supports reindex()')
        return count

    def __repr__(self):
        """
        The representation of the db.

        i.e. <UnionDB redis://my-redis-host/source;
              kydb.S3 s3://my-s3-prod-source>
        """
        return f'<{type(self).__name__} ' + \
            ';'.join(db.url for db in self.dbs) + '>'
