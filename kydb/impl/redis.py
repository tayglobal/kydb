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

    def _includes_epoch(self) -> bool:
        """ Whether un-indexed (pre-feature) objects can appear at all.

        A ``since(ts)`` with ``ts > 0`` excludes every epoch entry by
        definition, so the fallback read is skipped entirely in that case.
        """
        return self._since_ts is None or self._since_ts <= 0

    def _index_entries(self):
        """ Read the sorted set, in query order.

        :returns: ``(entries, exhausted)``. ``exhausted`` is True when
                  the sorted set was read to the end, which is what makes
                  the set of names complete enough to diff the folder
                  hash against. A short page is the only evidence needed:
                  Redis returns fewer members than asked for only when it
                  has no more to give.
        """
        folder = self._full_folder()
        conn = self._db.connection

        min_score = self._since_ts if self._since_ts is not None else '-inf'
        kwargs = {'withscores': True}
        if self._limit_n is not None:
            kwargs['start'] = 0
            kwargs['num'] = self._limit_n

        if self._ascending:
            raw = conn.zrangebyscore(
                RedisDB._mtime_key(folder), min_score, '+inf', **kwargs)
        else:
            raw = conn.zrevrangebyscore(
                RedisDB._mtime_key(folder), '+inf', min_score, **kwargs)

        # A short page means the sorted set had nothing more to give.
        exhausted = self._limit_n is None or len(raw) < self._limit_n

        if not raw:
            return [], exhausted

        members = [member for member, _score in raw]
        # The score ordered the result; the exact nanosecond values come
        # from the hashes, since a double cannot hold them precisely.
        mtimes = conn.hmget(RedisDB._mtime_val_key(folder), members)
        ctimes = conn.hmget(RedisDB._ctime_key(folder), members)

        out = []
        for (member, score), mtime_raw, ctime_raw in zip(raw, mtimes, ctimes):
            name = member.decode() if isinstance(member, bytes) else member
            mtime = int(mtime_raw) if mtime_raw is not None else int(score)
            ctime = int(ctime_raw) if ctime_raw is not None else mtime
            out.append(Entry(key=name, mtime=mtime, ctime=ctime))

        return out, exhausted

    def _epoch_entries(self, seen):
        """ Yield an ``Entry`` at ``mtime == ctime == 0`` for every object
        in the folder hash with no sorted-set entry.

        These are objects written before the index existed, or while it
        was switched off in config. They are ordered after every indexed
        object under ``desc()`` -- any real timestamp is greater than 0 --
        and before every one under ``asc()``. Order *among* them is
        arbitrary, as they all tie on 0.
        """
        folder = self._full_folder()
        for raw_name in self._db.connection.hgetall(folder).keys():
            name = raw_name.decode() if isinstance(raw_name, bytes) \
                else raw_name
            if name in seen or FolderMetaMixin._is_folder_meta(name):
                continue
            yield Entry(key=name, mtime=0, ctime=0)

    def entries(self):
        if self._index_name not in self._SUPPORTED_INDEXES:
            raise IndexNotSupported(
                "Redis folder query only supports by('mtime'), got "
                f"by({self._index_name!r})")

        indexed, exhausted = self._index_entries()
        remaining = self._limit_n

        # Only a fully-read sorted set gives a complete set of names to
        # diff the folder hash against, and a short read is the only way
        # the tail is ever reached anyway.
        want_epoch = self._includes_epoch() and exhausted
        seen = {e.key for e in indexed} if want_epoch else set()

        if self._ascending and want_epoch:
            # Epoch entries sort first when ascending.
            for entry in self._epoch_entries(seen):
                yield entry
                if remaining is not None:
                    remaining -= 1
                    if remaining <= 0:
                        return

        for entry in indexed:
            yield entry
            if remaining is not None:
                remaining -= 1
                if remaining <= 0:
                    return

        if not self._ascending and want_epoch:
            for entry in self._epoch_entries(seen):
                yield entry
                if remaining is not None:
                    remaining -= 1
                    if remaining <= 0:
                        return


class RedisDB(FolderMetaMixin, BaseDB):

    def __init__(self, url: str):
        super().__init__(url)

        self.connection = redis.Redis(
            **self._get_connection_kwargs(self.db_name))

    def _get_connection_kwargs(self, db_name: str):
        # Only treat the config as the source of connection details when
        # it actually carries them. A config block may exist purely to
        # set something else -- `mtime-index`, say -- in which case the
        # connection still comes from the URL.
        if self._config and 'host' in self._config:
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
        # One pipeline, one round trip. These commands are unconditional
        # with no intermediate reads, so batching them costs nothing and
        # keeps a write cheaper than the two round trips it took before
        # the index existed.
        pipe = self.connection.pipeline(transaction=False)
        pipe.hset(folder, obj, '.')
        pipe.set(key, value)

        if self.mtime_index_enabled and not self._is_folder_meta(obj):
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
            pipe.zadd(self._mtime_key(folder), {obj: now_ns})
            pipe.hset(self._mtime_val_key(folder), obj, now_ns)
            # HSETNX is Redis's native if_not_exists: ctime is set once,
            # on first write, and a rewrite leaves it untouched.
            pipe.hsetnx(self._ctime_key(folder), obj, now_ns)

        pipe.execute()

    def delete_raw(self, key: str):
        folder, obj = key.rsplit('/', 1)
        # Single round trip, as with the write path above. The index
        # cleanup is unconditional rather than gated on
        # mtime_index_enabled: the entries are harmless no-ops when the
        # index is off, and running them anyway means a db that had the
        # index disabled after use does not leave orphaned entries behind.
        pipe = self.connection.pipeline(transaction=False)
        pipe.delete(key)
        pipe.hdel(folder, obj)
        pipe.zrem(self._mtime_key(folder), obj)
        pipe.hdel(self._mtime_val_key(folder), obj)
        pipe.hdel(self._ctime_key(folder), obj)
        pipe.execute()

    # Index keys live under a `kydb:` prefix rather than being derived as
    # `<folder>:mtime-index`. Every kydb object path starts with '/' (see
    # BaseDB._get_full_path), so a `kydb:`-prefixed key can never collide
    # with a stored object -- whereas the suffix form would have let an
    # existing object at `/a/b:mtime-index` break the next write to `/a/b`
    # with a WRONGTYPE error.
    _INDEX_PREFIX = 'kydb:'

    @classmethod
    def _mtime_key(cls, folder: str) -> str:
        return cls._INDEX_PREFIX + 'mtime-index:' + folder

    @classmethod
    def _mtime_val_key(cls, folder: str) -> str:
        return cls._INDEX_PREFIX + 'mtime-values:' + folder

    @classmethod
    def _ctime_key(cls, folder: str) -> str:
        return cls._INDEX_PREFIX + 'ctime-index:' + folder

    def folder(self, folder: str, allow_scan: bool = False) \
            -> RedisFolderQuery:
        """ Implements folder in KYDBInterface, backed by a native
        per-folder sorted set (see :class:`RedisFolderQuery`).

        ``allow_scan`` is accepted for signature consistency with the
        other backends but is a no-op here: Redis's sorted set is a
        genuinely native ordering index, so there is no scan fallback to
        opt into.

        Raises ``IndexNotSupported`` when the index is disabled in
        config -- Redis stores no per-key timestamp of its own, so with
        the sorted set unmaintained there is nothing to order by, and
        ``allow_scan=True`` cannot rescue it.
        """
        if not self.mtime_index_enabled:
            self._raise_mtime_index_disabled()

        return RedisFolderQuery(self, folder, allow_scan=allow_scan)

    def list_dir_meta_folder(self, folder: str, page_size: int):
        folder = self._ensure_slashes(folder)[:-1]
        try:
            for key in self.connection.hgetall(folder).keys():
                yield key.decode()
        except ResponseError:
            raise KeyError(f'{folder} is not a valid folder')
