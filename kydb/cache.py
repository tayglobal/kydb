from .base import BaseDB
from .interface import KYDBInterface
from contextlib import ExitStack
from .objdb import ObjDBMixin, DBOBJ_CONFIG_PATH
from .dbobj import DbObj
import pickle


class CacheDB(KYDBInterface, ObjDBMixin):
    """CacheDB
    """

    def __init__(self, cache_db: BaseDB, persist_db: BaseDB):
        self.cache_db = cache_db
        self.persist_db = persist_db

    def cache_context(self) -> 'KYDBInterface':
        """This is related to in memory cache

        Not to be confused with the cache_db which is
        remote. For example Redis
        """
        with ExitStack() as stack:
            stack.enter_context(self.cache_db.cache_context())
            stack.enter_context(self.persist_db.cache_context())

        return stack

    def list_dir(self, folder: str, include_dir=True, page_size=200):
        """List directory always looks at the persist_db"""
        yield from self.persist_db.list_dir(folder, include_dir, page_size)

    def ls(self, folder: str, include_dir=True):
        return list(self.list_dir(folder, include_dir))

    def folder(self, folder: str, allow_scan: bool = False):
        """Delegates recency queries to persist_db, matching list_dir.

        The cache_db only holds what has been individually read, so it
        cannot answer a folder-wide question -- the returned FolderQuery
        is bound directly to persist_db (its .items()/.entries() read
        straight from persist_db, bypassing cache_db entirely, same as
        list_dir already does for plain listing).
        """
        return self.persist_db.folder(folder, allow_scan=allow_scan)

    def recent(self, folder: str, limit: int = None,
               allow_scan: bool = False):
        """Delegates recency queries to persist_db, matching list_dir."""
        return self.persist_db.recent(
            folder, limit=limit, allow_scan=allow_scan)

    def reindex(self, folder: str) -> int:
        """Reindex the authoritative persistent database.

        Recency queries intentionally ignore the partial cache, so rebuilding
        its metadata would be both incomplete and unnecessary.
        """
        return self.persist_db.reindex(folder)

    def __repr__(self):
        """
        The representation of the db.

        i.e. <CacheDB redis://my-redis-host/source;s3://my-s3-prod-source>
        """
        return f'<{type(self).__name__} {self.cache_db.url};{self.persist_db.url}>'

    def __getitem__(self, key: str):
        """Get item from DB

        Same as ``read(key)``"""
        return self.read(key)

    def __setitem__(self, key: str, value):
        """Write the item in both cache_db and persist_db

        Warning: If cache_db writes successfully and persist_db fails
        the two dbs will be out of sync
        """
        self.cache_db[key] = value
        self.persist_db[key] = value

    def set(self, key: str, value, system_obj=False, *, index=None):
        """Write the item in both cache_db and persist_db, recording any
        user index values on persist_db only.

        The two dbs do not play the same role here. ``persist_db`` is
        the authority -- it is where ``list_dir``, ``folder()`` and
        ``recent()`` already read from, because the cache only holds
        what someone happened to read -- so it is the only db whose
        index can answer a folder-wide question. Writing the values into
        ``cache_db`` as well would build a second, permanently partial
        index that nothing ever queries, and would fail outright when
        the cache is a backend with no index support at all.

        Warning: if cache_db writes successfully and persist_db fails
        the two dbs will be out of sync -- persist_db is written first
        so that a failure there leaves nothing cached to hide it.
        """
        self.persist_db.set(key, value, system_obj, index=index)
        self.cache_db.set(key, value, system_obj)

    def delete(self, key: str):
        """Delete the item in both cache_db and persist_db

        Warning: If cache_db deletes successfully and persist_db fails
        the two dbs will be out of sync
        """
        self.cache_db.delete(key)
        self.persist_db.delete(key)

    def rmdir(self, key: str):
        """Remove the directory in both cache_db and persist_db

        Warning: If cache_db deletes successfully and persist_db fails
        the two dbs will be out of sync
        """
        self.cache_db.rmdir(key)
        self.persist_db.rmdir(key)

    def rm_tree(self, key: str):
        """Remove the directory recursively in both cache_db and persist_db

        Warning: If cache_db deletes successfully and persist_db fails
        the two dbs will be out of sync
        """
        self.cache_db.rm_tree(key)
        self.persist_db.rm_tree(key)

    def new(self, class_name: str, key: str, **kwargs):
        return self.db_obj_new(class_name, key, kwargs)

    def exists(self, key) -> bool:
        """Check if a key exists in either cache_db or persist_db"""
        return self.cache_db.exists(key) or self.persist_db.exists(key)

    def refresh(self, key=None):
        """Refresh both cache_db and persist_db"""
        self.cache_db.refresh(key)
        self.persist_db.refresh(key)

    def read(self, key: str, reload=False):
        """Get Item from CacheDB

        Try to get the item from the cache_db first
        If it does not exist, get it from the persist_db
        and then write it to the cache_db
        """

        def _ensure_db(obj):
            if isinstance(obj, DbObj):
                obj.db = self

            return obj

        if self.cache_db.exists(key):
            raw_data = self.cache_db.get_raw(key)

            data = pickle.loads(raw_data)
            if self.is_data_dbobj(data):
                m = data['meta']
                return self.db_obj_new(m['class_name'], m['key'], data['data'])

            return _ensure_db(self.cache_db[key])

        item = _ensure_db(self.persist_db[key])
        self.cache_db[key] = item

        return item

    def mkdir(self, folder: str):
        """Apply mkdir to both cache_db and persist_db"""
        self.cache_db.mkdir(folder)
        self.persist_db.mkdir(folder)

    def is_dir(self, folder: str) -> bool:
        """Check is_dir in persist db"""
        return self.persist_db.is_dir(folder)

    def upload_objdb_config(self, objdb_config: dict):
        """Upload objdb_config to both cache_db and persist_db"""
        self.cache_db.upload_objdb_config(objdb_config)
        self.persist_db.upload_objdb_config(objdb_config)

    def _get_dbobj_config(self, class_name: str):
        if not self.cache_db.exists(DBOBJ_CONFIG_PATH):
            self.cache_db.upload_objdb_config(
                self.persist_db[DBOBJ_CONFIG_PATH])

        return super()._get_dbobj_config(class_name)

    def clear_cache(self):
        """Clear the cache

        This is useful when you want to clear the cache from memory

        Note: This is different to CacheDB where the cache is a database
        """
        self.cache_db.clear_cache()
        self.persist_db.clear_cache()

    def set_raw(self, key: str, value, index=None):
        """
        Set data from the DB based on key.

        It will set it both on cache_db and persist_db

        :param key: str:  The key to set, including base_path.
        :param value: The raw, pickled data.
        :param index: Optional ``{name: int or None}`` of user index
                      values, recorded on ``persist_db`` only -- for the
                      same reason :meth:`folder` reads from there. This
                      is the path a ``DbObj`` write takes
                      (``ObjDBMixin.write_dbobj`` calls ``obj.db.set_raw``
                      directly), so it has to carry the index too or an
                      indexed ``DbObj`` would be written unindexed.
        """
        if index:
            self.persist_db.set_raw(key, value, index=index)
        else:
            self.persist_db.set_raw(key, value)
        self.cache_db.set_raw(key, value)
