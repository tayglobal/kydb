import pickle
from typing import Tuple
import os
from .objdb import ObjDBMixin
from .cache_context import cache_context
from .exceptions import IndexNotSupported
from .interface import KYDBInterface
from typing import Optional
import yaml


class BaseDB(ObjDBMixin, KYDBInterface):
    """ Base class for KYDBInterface """

    def __init__(self, url: str):
        self.db_type = url.split(':', 1)[0]
        self.db_name, self.base_path = self._get_name_and_basepath(url)
        self._config = self._get_config()
        self.url = url
        self._cache = {}

    @property
    def mtime_index_enabled(self) -> bool:
        """ Whether this db maintains the ``mtime`` recency index on write.

        Read from the per-db config (``KYDB_CONFIG_PATH`` ->
        ``dbs.<db_name>.mtime-index``), defaulting to ``True`` when the
        key -- or the whole config file -- is absent, so the feature works
        with nothing configured.

        Setting it to ``False`` skips the index maintenance on every
        write. Objects written while it is off carry no timestamp, so
        they behave exactly like rows predating the feature: they appear
        in the epoch tail once it is turned back on, and move into place
        the first time they are rewritten.
        """
        if not self._config:
            return True

        return bool(self._config.get('mtime-index', True))

    def _raise_mtime_index_disabled(self):
        """ Raise the ``IndexNotSupported`` for a db whose recency index
        has been switched off in config.

        ``allow_scan=True`` does not rescue this: disabling the index
        removes the only record of when an object was written, so there
        is nothing left for a client-side sort to sort by. (Files and S3
        are unaffected -- their ``mtime`` comes from the substrate and is
        never maintained by kydb, so they ignore the setting entirely.)
        """
        raise IndexNotSupported(
            f'The mtime index is disabled for {self.db_name} '
            "(mtime-index: false in the kydb config), so folder()/"
            'recent() have no timestamps to order by')

    def _get_config(self) -> Optional[dict]:
        config_path = os.environ.get('KYDB_CONFIG_PATH')
        if config_path:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)

            return config['dbs'].get(self.db_name)

    @staticmethod
    def _get_name_and_basepath(url: str) -> Tuple[str, str]:
        """
        get the db_name and base_path

        :param url: str:
        :returns: a tuple (db_name, base_path)

        """
        base_path = '/'
        parts = url.split('/', 3)
        num_parts = len(parts)
        assert num_parts in (3, 4)

        db_name = parts[2]
        if len(parts) == 4:
            base_path += parts[-1]

        if base_path[-1] != '/':
            base_path += '/'

        return db_name, base_path

    def _get_full_path(self, key: str) -> str:
        """
        get the fullpath from key including base_path.

        :param key: str:
        :returns: The fullpath from key including base_path.

        """
        if key.startswith('/'):
            key = key[1:]

        res = self.base_path + key

        if not res.startswith('/'):
            res = '/' + res

        return res

    def _serialise(self, obj):
        """
        Serialises the object.

        :param obj: object to serialise
        :returns: The obj pickled

        """
        return pickle.dumps(obj)

    def _deserialise(self, data):
        """ Deserialise the data

        :param obj:
        :returns: Unpickle data
        """
        return pickle.loads(data)

    def exists(self, key) -> bool:
        """ Implements exists in KYDBInterface """
        return self.exists_raw(self._get_full_path(key))

    def exists_raw(self, key: str) -> bool:
        """ Same as exist but with base_path prepended """
        try:
            self.get_raw(key)
            return True
        except KeyError:
            return False

    def __getitem__(self, key: str):
        """ Implements __getitem__ in KYDBInterface """
        return self.read(key)

    def refresh(self, key=None):
        """ Implements refresh in KYDBInterface """
        if key:
            del self._cache[key]
        else:
            self._cache = {}

    def clear_cache(self):
        """Clear the cache

        This is useful when you want to clear the cache from memory

        Note: This is different to CacheDB where the cache is a database
        """
        self._cache = {}

    def read(self, key: str, reload=False):
        """ Implements read in KYDBInterface """
        path = self._get_full_path(key)
        res = None if reload else self._cache.get(path)
        if not res:
            res = self._deserialise(self.get_raw(path))
            if self.is_data_dbobj(res):
                res = self.read_dbobj(res)

        self._cache[key] = res
        return res

    def mkdir(self, folder: str):
        """ Implements read in KYDBInterface """
        if not folder or folder == '/':
            raise ValueError('Cannot make folder: ' + folder)

        self.mkdir_raw(self._get_full_path(folder))

    def mkdir_raw(self, folder: str):
        """ same as mkdir but with base_path prepended """
        raise NotImplementedError()

    def is_dir(self, folder: str) -> bool:
        return self.is_dir_raw(self._get_full_path(folder))

    def is_dir_raw(self, folder: str) -> bool:
        """ Same as is_dir, but prepended with base_path """
        raise NotImplementedError()

    def __setitem__(self, key: str, value):
        self.set(key, value)

    def set(self, key: str, value, system_obj=False):
        if not system_obj and \
                any(x for x in key.rsplit('/') if x.startswith('.')):
            raise KeyError('Cannot have dot (.) prefix in path, '
                           'got :' + key)

        path = self._get_full_path(key)
        self._cache[path] = value

        if self.is_dbobj(value):
            self.write_dbobj(value)
        else:
            self.set_raw(path, self._serialise(value))

    def get_raw(self, key: str):
        """
        Get data from the DB based on key.

        This is to be implemented by derived class.

        :param key: str:  The key to get, including base_path.
        :returns: str: The raw, pickled data.
        """
        raise NotImplementedError()

    def set_raw(self, key: str, value):
        """
        Set data from the DB based on key.

        This is to be implemented by derived class.

        :param key: str:  The key to set, including base_path.
        :param value: The raw, pickled data.
        """
        raise NotImplementedError()

    def delete_raw(self, key: str):
        """
        Delete data from the DB based on key

        This is to be implemented by derived class.

        :param key: str:  The key to delete

        """
        raise NotImplementedError()

    def delete(self, key: str):
        if not self.exists(key):
            raise KeyError('Cannot delete non-existence: ' + key)

        if key in self._cache:
            del self._cache[key]

        self.delete_raw(self._get_full_path(key))

    def rmdir(self, key: str):
        if key in ['.', '/', '']:
            raise KeyError('Directory does not exist: ' + key)

        path = self._get_full_path(key)

        if not self.exists_raw(path) and not self.is_dir_raw(path):
            raise KeyError('Directory does not exist: ' + path)

        try:
            next(iter(self.list_dir(key, page_size=1)))
            raise KeyError(f'Directory {key} is not empty')
        except StopIteration:
            self.rmdir_raw(path)

    def rmdir_raw(self, key: str):
        """ same as rmdir but with base_path prepended """
        raise NotImplementedError()

    def list_dir(self, folder: str, include_dir=True, page_size=200):
        return self.list_dir_raw(
            self._get_full_path(folder), include_dir, page_size)

    def list_dir_raw(self, folder: str, include_dir: bool, page_size: int):
        """ Same as list_dir but with base_path prepended to folder """
        raise NotImplementedError()

    def ls(self, folder: str, include_dir=True):
        return list(self.list_dir(folder, include_dir))

    def folder(self, folder: str, allow_scan: bool = False):
        """ Implements folder in KYDBInterface

        Default: unsupported. Backends with a server-side ordering index
        (DynamoDB, via ``folder-time-index``; Redis, via a per-folder
        sorted set) or an opt-in client-side scan (Memory, Files, S3)
        override this.
        """
        raise IndexNotSupported(
            f'{type(self).__name__} does not support folder()/recent() '
            'recency queries (no server-side ordering index)')

    def recent(self, folder: str, limit: int = None,
               allow_scan: bool = False):
        """ Implements recent in KYDBInterface

        Sugar for
        ``db.folder(folder, allow_scan=allow_scan).by('mtime').desc().limit(limit)``.
        Relies entirely on ``self.folder()`` -- backends need not override
        this separately.
        """
        query = self.folder(folder, allow_scan=allow_scan).by('mtime').desc()
        if limit is not None:
            query = query.limit(limit)
        return query

    def rm_tree(self, key: str):
        if not self.is_dir(key):
            raise KeyError('{} is not a directory'.format(key))

        folder = self._ensure_slashes(key)
        objs = list(self.list_dir(key))
        for obj in objs:
            path = folder + obj
            if obj.endswith('/'):
                self.rm_tree(path)
            else:
                self.delete(path)

        self.rmdir(key)

    def new(self, class_name: str, key: str, **kwargs):
        return self.db_obj_new(class_name, key, kwargs)

    def cache_context(self) -> KYDBInterface:
        return cache_context(self)

    def __repr__(self):
        """
        The representation of the db.

        i.e. <kydb.RedisDB redis://my-redis-host/source>
        """
        return f'<{type(self).__name__} {self.url}>'

    @staticmethod
    def _ensure_slashes(s: str):
        """Ensures s starts with / and ends with /

        :param s: The string to pass in

        Add slash in front or behind if needed
        """
        if not s.endswith('/'):
            s += '/'

        if not s.startswith('/'):
            s = '/' + s

        return s
