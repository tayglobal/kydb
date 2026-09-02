from kydb.base import BaseDB
from kydb.exceptions import IndexNotSupported
from kydb.folder_meta import FolderMetaMixin
from kydb.query import ScanFolderQuery
import re
import time


class MemoryFolderQuery(ScanFolderQuery):
    """ Client-side scan-and-sort over MemoryDB's in-memory dict --
    everything is already resident in memory, so this is a plain Python
    sort by the ``mtime`` recorded on write.
    """

    def _raw_entries(self):
        full_folder = self._db._ensure_slashes(
            self._db._get_full_path(self._folder))
        yield from self._db._folder_time_entries(full_folder)


class MemoryDB(FolderMetaMixin, BaseDB):
    __cache = {}
    # db_name -> {full_path: (mtime_ns, ctime_ns)}, populated only for
    # real objects (never `.folder-*` marker records), so it is a sparse
    # index over __cache in exactly the way folder-time-index is sparse
    # over the DynamoDB table.
    __meta = {}

    def __init__(self, url: str):
        super().__init__(url)
        self.__cache[self.db_name] = {}
        self.__meta[self.db_name] = {}

    def get_raw(self, key):
        if self.base_path != '/' and \
                key == self._folder_meta_path(self.base_path, ''):
            raise KeyError(key)

        return self.__cache[self.db_name][key]

    def folder_meta_set_raw(self, key: str, value):
        self.__cache[self.db_name][key] = value

        objname = key.rsplit('/', 1)[-1]
        if self._is_folder_meta(objname) or not self.mtime_index_enabled:
            # Directories are excluded from the recency index: no mtime
            # is ever recorded for `.folder-*` marker records.
            #
            # The same applies to a db with `mtime-index: false` in
            # config -- nothing is recorded, so there is nothing for
            # folder()/recent() to sort by.
            return

        now_ns = time.time_ns()
        existing = self.__meta[self.db_name].get(key)
        ctime_ns = existing[1] if existing else now_ns
        self.__meta[self.db_name][key] = (now_ns, ctime_ns)

    def delete_raw(self, key: str):
        del self.__cache[self.db_name][key]
        self.__meta[self.db_name].pop(key, None)

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

        Raises ``IndexNotSupported`` when the index is disabled in
        config: the mtimes it sorts by are the ones recorded on write,
        so with those switched off there is nothing to scan.
        """
        if not self.mtime_index_enabled:
            self._raise_mtime_index_disabled()

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

    def _folder_time_entries(self, full_folder: str):
        """ Yield ``(name, mtime, ctime)`` for every object directly in
        ``full_folder`` (sub-folders excluded).
        """
        prefix_len = len(full_folder)
        for key, (mtime, ctime) in self.__meta[self.db_name].items():
            if key.startswith(full_folder) and \
                    '/' not in key[prefix_len:]:
                yield key[prefix_len:], mtime, ctime
