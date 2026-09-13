from kydb.base import BaseDB
from kydb.exceptions import IndexNotSupported
from kydb.folder_meta import FolderMetaMixin
from kydb.query import ScanFolderQuery
import re
import time


class MemoryFolderQuery(ScanFolderQuery):
    """ Client-side scan-and-sort over MemoryDB's in-memory dict --
    everything is already resident in memory, so this is a plain Python
    sort by the value recorded on write.

    All the filtering, ordering and limiting lives in
    :class:`kydb.query.ScanFolderQuery`, on the *index value* rather
    than on ``mtime``. So a user index costs this class nothing beyond
    reporting the value: the rows it yields carry the queried index's
    value in the fourth slot, and the base class does the rest --
    including dropping objects that have no value for it, which is what
    makes a user index strictly sparse with no epoch tail
    (``user_index_plan.md`` §5).
    """

    #: No fixed set of index names to declare: the values live in a
    #: dict keyed by name, written on demand and never declared up
    #: front. So this is not ``('mtime',) + something``, it is "ask the
    #: store" -- a name nobody has written is not *unsupported*, it
    #: simply has no rows, which a strictly sparse index already says
    #: honestly by returning nothing.
    _SUPPORTED_INDEXES = None

    def _raw_entries(self):
        full_folder = self._db._ensure_slashes(
            self._db._get_full_path(self._folder))
        yield from self._db._folder_time_entries(
            full_folder, self._index_name)


class MemoryDB(FolderMetaMixin, BaseDB):
    __cache = {}
    # db_name -> {full_path: (mtime_ns, ctime_ns)}, populated only for
    # real objects (never `.folder-*` marker records), so it is a sparse
    # index over __cache in exactly the way folder-time-index is sparse
    # over the DynamoDB table.
    __meta = {}
    # db_name -> {full_path: {index_name: int}}, the caller-supplied
    # index values. Separate from __meta because it answers a different
    # question -- what the object *means*, not when it was written --
    # and because an object usually has none, so an absent entry is the
    # common case and must stay cheap.
    __index = {}

    #: MemoryDB can store caller-supplied index values: a dict per
    #: object, sorted client-side behind ``allow_scan=True`` exactly as
    #: ``mtime`` already is.
    supports_user_index = True

    def __init__(self, url: str):
        super().__init__(url)
        self.__cache[self.db_name] = {}
        self.__meta[self.db_name] = {}
        self.__index[self.db_name] = {}

    def get_raw(self, key):
        if self.base_path != '/' and \
                key == self._folder_meta_path(self.base_path, ''):
            raise KeyError(key)

        return self.__cache[self.db_name][key]

    def folder_meta_set_raw(self, key: str, value, index=None):
        """ Store the object, its ``mtime``/``ctime`` pair, and any
        caller-supplied index values.

        Only the names present in ``index`` are touched: an index the
        write does not mention keeps the value it had
        (``user_index_plan.md`` §4.4), and ``None`` removes it. Removing
        rather than storing ``None`` keeps "has no value for this index"
        a single representation -- the lookup in
        :meth:`_folder_time_entries` cannot then tell a cleared value
        from one that was never written, because there is nothing to
        tell apart.

        Unlike the ``mtime`` metadata, index values are recorded even
        when ``mtime-index`` is off in config: that switch is about the
        backend's own timestamp, not about a value the caller supplied
        explicitly.
        """
        self.__cache[self.db_name][key] = value

        objname = key.rsplit('/', 1)[-1]
        if self._is_folder_meta(objname):
            # Directories are excluded from every index: no mtime and no
            # index value is ever recorded for `.folder-*` markers.
            return

        if index:
            values = self.__index[self.db_name].setdefault(key, {})
            for name, index_value in index.items():
                if index_value is None:
                    values.pop(name, None)
                else:
                    values[name] = index_value
            if not values:
                del self.__index[self.db_name][key]

        if not self.mtime_index_enabled:
            # `mtime-index: false`: no timestamp is recorded, so there is
            # nothing for folder()/recent() to sort by.
            return

        now_ns = time.time_ns()
        existing = self.__meta[self.db_name].get(key)
        ctime_ns = existing[1] if existing else now_ns
        self.__meta[self.db_name][key] = (now_ns, ctime_ns)

    def delete_raw(self, key: str):
        """ Drop the object and every trace of it in the indexes.

        An index value that outlived its object would surface in a later
        query as a name with nothing behind it, indistinguishable from a
        real result.
        """
        del self.__cache[self.db_name][key]
        self.__meta[self.db_name].pop(key, None)
        self.__index[self.db_name].pop(key, None)

    def get_cache(self):
        return self.__cache[self.db_name]

    @staticmethod
    def _list_dir_regex(folder: str):
        return re.compile(f'^{folder}([^/]+)$')

    def list_dir_meta_folder(self, folder: str, page_size: int):
        folder = self._ensure_slashes(folder)
        pattern = self._list_dir_regex(folder)
        for path in self.__cache[self.db_name].keys():
            match = pattern.search(path)
            if match:
                yield match.groups(0)[0]

    def folder(self, folder: str, allow_scan: bool = False) \
            -> MemoryFolderQuery:
        """ Implements folder in KYDBInterface.

        MemoryDB has no server-side ordering index -- this is a
        client-side scan-and-sort of the (already in-memory) folder, so
        it requires the caller to opt in with ``allow_scan=True``.

        ``by('mtime')`` raises ``IndexNotSupported`` when the index is
        disabled in config -- the mtimes it sorts by are the ones
        recorded on write, so with those switched off there is nothing
        to scan. That check happens when the query runs rather than
        here, so it can tell ``by('mtime')`` from ``by('class_date')``:
        a user index is scanned from its own store and is unaffected by
        the setting, on both the read and the write side.
        """
        if not allow_scan:
            raise IndexNotSupported(
                f'{type(self).__name__} does not support folder()/'
                "recent() natively; pass allow_scan=True to opt into "
                'an O(n) client-side scan-and-sort of the folder')
        return MemoryFolderQuery(self, folder, allow_scan=True)

    def reindex(self, folder: str) -> int:
        """Timestamp in-memory objects that have no recency metadata."""
        if not self.mtime_index_enabled:
            self._raise_mtime_index_disabled()

        full_folder = self._ensure_slashes(self._get_full_path(folder))
        prefix_len = len(full_folder)
        cache = self.__cache[self.db_name]
        meta = self.__meta[self.db_name]
        missing = [
            key for key in cache
            if key.startswith(full_folder)
            and '/' not in key[prefix_len:]
            and not self._is_folder_meta(key[prefix_len:])
            and key not in meta
        ]
        now_ns = time.time_ns()
        for key in missing:
            meta[key] = (now_ns, now_ns)
        return len(missing)

    def _folder_time_entries(self, full_folder: str,
                             index_name: str = 'mtime'):
        """ Yield ``(name, mtime, ctime, index_value)`` for every object
        directly in ``full_folder`` (sub-folders excluded) that has a
        value for ``index_name``.

        For ``mtime`` the ordering value *is* the timestamp, so every
        object in the metadata dict qualifies and the behaviour is
        unchanged. For a user index the value is looked up per object
        and one with no value is **not yielded at all** -- it is not an
        object at position zero in the business ordering, it is an
        object the ordering has nothing to say about
        (``user_index_plan.md`` §5). ``mtime`` and ``ctime`` are still
        reported alongside it, so a row ordered by ``class_date`` still
        carries a truthful write time.
        """
        prefix_len = len(full_folder)
        want_mtime = index_name == 'mtime'
        meta = self.__meta[self.db_name]
        index = self.__index[self.db_name]

        # Each index is its own row source. `mtime` reads the timestamp
        # metadata; a user index reads the index store, because the two
        # populate independently: an object written under
        # `mtime-index: false` has a class_date and no timestamp, so
        # walking the metadata would miss it entirely and the roster
        # would come back empty. Scanning the index store is also the
        # cheaper walk, since it holds only the objects that have a
        # value.
        source = meta if want_mtime else index

        # Sorted so that objects tying on the index value come out in a
        # stable, explicable order rather than in whatever order the
        # dict happens to hold them -- which shifts when a value is
        # cleared and written again, since that re-inserts the key at
        # the end. `ScanFolderQuery` sorts stably, so equal values keep
        # this order in both directions.
        for key in sorted(source):
            if not key.startswith(full_folder) or '/' in key[prefix_len:]:
                continue

            # 0 is the same "write time unknown" the epoch tail reports.
            # A row ordered by a business key still reports a truthful
            # mtime wherever one was recorded, and does not invent one
            # where it was not.
            mtime, ctime = meta.get(key, (0, 0))

            if want_mtime:
                index_value = mtime
            else:
                index_value = index.get(key, {}).get(index_name)
                if index_value is None:
                    continue

            yield key[prefix_len:], mtime, ctime, index_value
