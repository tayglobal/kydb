import pickle
import re
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

    #: Whether this backend can store caller-supplied index values
    #: (``set(key, value, index={'class_date': 20260905})``).
    #:
    #: ``False`` by default, and deliberately opt-in rather than
    #: opt-out: a backend with nowhere to put the value -- Files and S3
    #: have no attribute storage kydb models -- must *raise* rather than
    #: accept the value and drop it. A silent drop is the one failure
    #: that reaches production undetected, because every write succeeds
    #: and only the query comes back empty, long afterwards and
    #: somewhere else. Making the flag explicit means a backend is loud
    #: by default instead of loud only if someone remembered a guard.
    supports_user_index = False

    #: Index names that would collide with the item's own structure.
    #: An index name becomes a DynamoDB attribute name, part of the
    #: derived GSI name (``folder-<name>-index``) and part of the Redis
    #: key (``<folder>:<name>-index``), so these are reserved.
    #: ``mtime``/``ctime`` are reserved additionally because they are
    #: maintained by the backend -- letting a caller write their own
    #: would hand back the clock-skew problem the recency index exists
    #: to avoid.
    _RESERVED_INDEX_NAMES = frozenset(
        {'path', 'folder', 'contents', 'mtime', 'ctime'})

    _INDEX_NAME_RE = re.compile(r'^[A-Za-z][A-Za-z0-9_]*$')

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

    def _validate_index(self, index) -> dict:
        """ Check a caller-supplied ``index`` mapping, before any write.

        :param index: The ``index=`` argument to :meth:`set`, or ``None``.
        :returns: The mapping, empty if there was nothing to write.
        :raises ValueError: for an unusable index *name*.
        :raises TypeError: for an unusable index *value*.
        :raises IndexNotSupported: if this backend cannot store index
                                   values at all.

        Names must match ``[A-Za-z][A-Za-z0-9_]*`` and must not be one
        of :attr:`_RESERVED_INDEX_NAMES`; values must be ``int`` (v1 is
        deliberately int-only, so one comparison semantic serves every
        backend -- see ``user_index_plan.md`` §4.2) or ``None``, which
        means *clear this index value*.

        ``bool`` is rejected explicitly. It is an ``int`` subclass, so
        it would otherwise sort silently as 0/1, and a boolean landing
        in a business ordering is almost certainly a mistake at the call
        site rather than an intent.

        Names and values are checked before the backend-capability
        check, so a malformed index is reported as malformed on every
        backend -- it is wrong everywhere, and reporting it as
        "unsupported here" would send the caller looking in the wrong
        place.
        """
        if not index:
            return {}

        if not isinstance(index, dict):
            raise TypeError(
                'index must be a dict of index name to int value, got '
                f'{type(index).__name__}')

        for name, value in index.items():
            if not isinstance(name, str) or \
                    not self._INDEX_NAME_RE.match(name):
                raise ValueError(
                    f'Invalid index name {name!r}: an index name must '
                    'match [A-Za-z][A-Za-z0-9_]*')

            if name in self._RESERVED_INDEX_NAMES:
                raise ValueError(
                    f'Invalid index name {name!r}: reserved, one of '
                    f'{sorted(self._RESERVED_INDEX_NAMES)}')

            if value is None:
                # Explicitly clears the value -- see set()'s docstring
                # on why an unmentioned index is preserved instead.
                continue

            if isinstance(value, bool) or not isinstance(value, int):
                raise TypeError(
                    f'Index {name!r} must be an int or None, got '
                    f'{type(value).__name__}')

        if not self.supports_user_index:
            raise IndexNotSupported(
                f'{type(self).__name__} cannot store user index values, '
                f'so set(index={{{", ".join(sorted(index))}: ...}}) would '
                'silently drop them. Use a backend with index support '
                '(DynamoDB, Redis, Memory), or encode the value in the '
                'path')

        return index

    def set(self, key: str, value, system_obj=False, *, index=None):
        """ Implements set in KYDBInterface.

        ``index`` is a keyword-only mapping of index name to ``int``
        value, validated by :meth:`_validate_index` before anything is
        written -- a rejected index must never leave a half-done write
        behind.

        A rewrite that does not mention an index **preserves** it; pass
        ``{'name': None}`` to clear one. See
        :meth:`kydb.interface.KYDBInterface.set`.
        """
        if not system_obj and \
                any(x for x in key.rsplit('/') if x.startswith('.')):
            raise KeyError('Cannot have dot (.) prefix in path, '
                           'got :' + key)

        index = self._validate_index(index)

        path = self._get_full_path(key)
        self._cache[path] = value

        if self.is_dbobj(value):
            # write_dbobj has its own serialisation path (the object's
            # stored dict plus the metadata to rebuild it), so the index
            # is threaded through it rather than through _serialise.
            self.write_dbobj(value, index=index)
        elif index:
            # Only passed when there is something to pass, so a backend
            # that has no index support (and therefore can never reach
            # here) keeps its two-argument set_raw.
            self.set_raw(path, self._serialise(value), index=index)
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

    def set_raw(self, key: str, value, index=None):
        """
        Set data from the DB based on key.

        This is to be implemented by derived class.

        :param key: str:  The key to set, including base_path.
        :param value: The raw, pickled data.
        :param index: Optional ``{name: int or None}`` of user index
                      values, already validated by
                      :meth:`_validate_index`. ``None`` for a value
                      clears that index; an index absent from the
                      mapping is left as it was.

        Only backends with ``supports_user_index = True`` are ever
        passed a non-empty ``index``.
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

    def reindex(self, folder: str) -> int:
        """Implements reindex in KYDBInterface.

        Backends that maintain recency metadata override this. Backends whose
        timestamps come directly from their substrate may return zero.
        """
        raise IndexNotSupported(
            f'{type(self).__name__} does not maintain a recency index')

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
