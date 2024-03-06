from .base import BaseDB
from .interface import KYDBInterface
from contextlib import ExitStack


class CacheDB(KYDBInterface):
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
        # TODO
        ...

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
        if self.cache_db.exists(key):
            return self.cache_db[key]

        item = self.persist_db[key]
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

