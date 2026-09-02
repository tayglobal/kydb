from kydb.base import BaseDB
from kydb.exceptions import IndexNotSupported
from kydb.folder_meta import FolderMetaMixin
from kydb.query import Entry, FolderQuery
from redis.exceptions import ResponseError
import redis
import boto3
import os
import base64
import time


class RedisFolderQuery(FolderQuery):
    """ FolderQuery backed by a native per-folder Redis sorted set --
    ``ZADD``/``ZRANGEBYSCORE``/``ZREVRANGEBYSCORE`` on the ``mtime``
    index, maintained on every write (:meth:`RedisDB.folder_meta_set_raw`)
    and cleaned up on every delete (:meth:`RedisDB.delete_raw`).

    This is the one backend below DynamoDB that is genuinely native --
    unlike Memory/Files/S3 it does not require ``allow_scan=True``.

    ``ctime`` is tracked the same way DynamoDB does it -- set once via
    ``HSETNX`` (Redis's own ``if_not_exists``) and never touched again by
    a rewrite -- in a companion hash so it survives independently of the
    sorted set's score.

    ``mtime``/``ctime`` are nanoseconds since the epoch, matching every
    other backend. A sorted-set score is a double and is only exact up to
    ``2**53``, so a ~19-digit nanosecond timestamp does not survive it
    intact -- storing one as a score rounds it to roughly 256ns. The
    score is therefore used **only for ordering**, which that resolution
    serves fine, while the exact nanosecond value is kept in a companion
    hash and is what ``entries()`` returns.
    """

    _SUPPORTED_INDEXES = ('mtime',)

    def _full_folder(self) -> str:
        # Matches the (no trailing slash) folder key format already used
        # by folder_meta_set_raw's `self.connection.hset(folder, ...)`.
        full = self._db._ensure_slashes(self._db._get_full_path(self._folder))
        return full[:-1]

    def entries(self):
        if self._index_name not in self._SUPPORTED_INDEXES:
            raise IndexNotSupported(
                "Redis folder query only supports by('mtime'), got "
                f"by({self._index_name!r})")

        folder = self._full_folder()
        mtime_key = RedisDB._mtime_key(folder)
        ctime_key = RedisDB._ctime_key(folder)
        conn = self._db.connection

        min_score = self._since_ts if self._since_ts is not None else '-inf'
        kwargs = {'withscores': True}
        if self._limit_n is not None:
            kwargs['start'] = 0
            kwargs['num'] = self._limit_n

        if self._ascending:
            raw = conn.zrangebyscore(mtime_key, min_score, '+inf', **kwargs)
        else:
            raw = conn.zrevrangebyscore(mtime_key, '+inf', min_score, **kwargs)

        if not raw:
            return

        members = [member for member, _score in raw]
        # The score ordered the result; the exact nanosecond values come
        # from the hashes, since a double cannot hold them precisely.
        mtimes = conn.hmget(RedisDB._mtime_val_key(folder), members)
        ctimes = conn.hmget(ctime_key, members)

        for (member, score), mtime_raw, ctime_raw in zip(raw, mtimes, ctimes):
            name = member.decode() if isinstance(member, bytes) else member
            mtime = int(mtime_raw) if mtime_raw is not None else int(score)
            ctime = int(ctime_raw) if ctime_raw is not None else mtime
            yield Entry(key=name, mtime=mtime, ctime=ctime)


class RedisDB(FolderMetaMixin, BaseDB):

    def __init__(self, url: str):
        super().__init__(url)

        self.connection = redis.Redis(
            **self._get_connection_kwargs(self.db_name))

    def _get_connection_kwargs(self, db_name: str):
        if self._config:
            password = self._get_password()
            host = self._config['host']
            port = self._config['port']
            return {
                'host': host,
                'port': port,
                'password': password,
            }

        return self._get_connection_from_dbname(db_name)

    def _get_password(self):
        pwd_cfg = self._config['password']
        encrypt_method = pwd_cfg['encryption-method']
        if encrypt_method == 'kms':
            return self._get_secret_from_kms(pwd_cfg['env_var'], pwd_cfg['encryption-key'])

        if encrypt_method == 'plain':
            return os.environ[pwd_cfg['env_var']]

        raise ValueError(f'Unknown encryption method: {encrypt_method}')

    @staticmethod
    def _get_secret_from_kms(name, kms_key_id: str):
        kms = boto3.client('kms')
        encrypted = os.environ[name]
        res = kms.decrypt(
            KeyId=kms_key_id,
            CiphertextBlob=base64.b64decode(encrypted))

        return res['Plaintext'].decode()

    @staticmethod
    def encrypt_secret(secret: str, kms_key_id: str):
        kms = boto3.client('kms')
        res = kms.encrypt(
            KeyId=kms_key_id,
            Plaintext=secret)
        return base64.b64encode(res['CiphertextBlob'])

    @staticmethod
    def _get_connection_from_dbname(db_name: str):
        if ':' in db_name:
            host, port = db_name.split(':')
            kwargs = {
                'host': host,
                'port': int(port, 10)
            }
        else:
            kwargs = {
                'host': db_name
            }

        return kwargs

    def get_raw(self, key: str):
        try:
            res = self.connection.get(key)
        except ResponseError:
            raise KeyError(f'{key} is not a valid key')

        if not res:
            raise KeyError(key)

        return res

    def folder_meta_set_raw(self, key: str, value):
        folder, obj = key.rsplit('/', 1)
        self.connection.hset(folder, obj, '.')
        self.connection.set(key, value)

        if not self._is_folder_meta(obj):
            # Directories are excluded from the recency index: no entry
            # is ever added to the sorted set for `.folder-*` marker
            # records, matching DynamoDB's sparse-index behaviour.
            #
            # The ZSET score orders the folder. It is a double, so it
            # cannot hold a nanosecond epoch exactly (~256ns resolution);
            # that is fine for ordering, and the exact value is kept in a
            # hash so callers still get nanoseconds, as on every other
            # backend.
            now_ns = time.time_ns()
            self.connection.zadd(self._mtime_key(folder), {obj: now_ns})
            self.connection.hset(self._mtime_val_key(folder), obj, now_ns)
            # HSETNX is Redis's native if_not_exists: ctime is set once,
            # on first write, and a rewrite leaves it untouched.
            self.connection.hsetnx(self._ctime_key(folder), obj, now_ns)

    def delete_raw(self, key: str):
        self.connection.delete(key)
        folder, obj = key.rsplit('/', 1)
        self.connection.hdel(folder, obj)
        self.connection.zrem(self._mtime_key(folder), obj)
        self.connection.hdel(self._mtime_val_key(folder), obj)
        self.connection.hdel(self._ctime_key(folder), obj)

    @staticmethod
    def _mtime_key(folder: str) -> str:
        return folder + ':mtime-index'

    @staticmethod
    def _mtime_val_key(folder: str) -> str:
        return folder + ':mtime-values'

    @staticmethod
    def _ctime_key(folder: str) -> str:
        return folder + ':ctime-index'

    def folder(self, folder: str, allow_scan: bool = False) \
            -> RedisFolderQuery:
        """ Implements folder in KYDBInterface, backed by a native
        per-folder sorted set (see :class:`RedisFolderQuery`).

        ``allow_scan`` is accepted for signature consistency with the
        other backends but is a no-op here: Redis's sorted set is a
        genuinely native ordering index, so there is no scan fallback to
        opt into.
        """
        return RedisFolderQuery(self, folder, allow_scan=allow_scan)

    def list_dir_meta_folder(self, folder: str, page_size: int):
        folder = self._ensure_slashes(folder)[:-1]
        try:
            for key in self.connection.hgetall(folder).keys():
                yield key.decode()
        except ResponseError:
            raise KeyError(f'{folder} is not a valid folder')
