from kydb.base import BaseDB
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

    A **user index** (``by('class_date')``, written with
    ``db.set(key, value, index={'class_date': 20260905})``) is the same
    structure with the name parameterised: a ZSET at
    :meth:`RedisDB._index_key` orders the folder, a companion hash at
    :meth:`RedisDB._index_val_key` holds the exact value. The precision
    argument above does not apply to it -- ``20260905`` is 14 orders of
    magnitude below ``2**53`` and survives a double exactly -- and the
    hash is kept anyway so both read paths are the same shape, and so
    §4.2 can relax to floats or bigger ints later without a storage
    change.

    Two things differ from ``mtime``, both deliberate:

    * **No epoch tail.** A user index query never falls back to the
      folder hash for un-indexed objects (see :meth:`_includes_epoch`).
    * **No declaration.** Redis has no schema, so ``by()`` accepts any
      index name; one nobody has written simply has no rows. There is
      nothing to gate on, which is why ``_SUPPORTED_INDEXES`` is absent
      here rather than merely widened -- the inherited
      :meth:`kydb.query.FolderQuery._supports_index` then admits every
      name.

    ``mtime``/``ctime`` are still reported for a user-index row -- they
    come from the same hashes as always -- so ``entries()`` tells the
    caller both where the object sits in the business ordering
    (``index_value``) and when it was actually written (``mtime``).
    """

    def _full_folder(self) -> str:
        # Matches the (no trailing slash) folder key format already used
        # by folder_meta_set_raw's `self.connection.hset(folder, ...)`.
        full = self._db._ensure_slashes(self._db._get_full_path(self._folder))
        return full[:-1]

    def _is_user_index(self) -> bool:
        """ Whether this query orders by a caller-supplied index rather
        than by the backend-stamped ``mtime``.
        """
        return self._index_name != 'mtime'

    def _includes_epoch(self) -> bool:
        """ Whether un-indexed (pre-feature) objects can appear at all.

        Never, for a user index. A missing ``mtime`` still means
        something -- *written before the index existed, therefore older
        than anything it tracks* -- which is why those objects are
        reported at 0 rather than hidden. A missing ``class_date`` means
        nothing of the sort: the object is not an ancient signup, it is
        not a signup at all, and giving it a position in a business
        ordering would put junk in every result. User index queries are
        therefore strictly sparse in both directions
        (``user_index_plan.md`` §5), and skipping the fallback read also
        makes them cheaper than ``mtime`` -- ``asc()`` has no O(folder)
        tail to discover first.

        For ``mtime``, a ``since(ts)`` with ``ts > 0`` (or an
        ``until(ts)`` with ``ts < 0``) already excludes every epoch entry
        by definition, so the fallback read is skipped in that case too.
        """
        if self._is_user_index():
            return False

        if self._since_ts is not None and self._since_ts > 0:
            return False

        return self._until_ts is None or self._until_ts >= 0

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
        user_index = self._is_user_index()
        zset_key = RedisDB._index_key(folder, self._index_name) \
            if user_index else RedisDB._mtime_key(folder)

        # `until` is the `max` argument of the range read, so a bounded
        # query is served by Redis itself rather than by discarding rows
        # after they arrive -- which also keeps `limit` honest, since
        # ZRANGEBYSCORE counts only members inside the bounds.
        min_score = self._since_ts if self._since_ts is not None else '-inf'
        max_score = self._until_ts if self._until_ts is not None else '+inf'
        kwargs = {'withscores': True}
        if self._limit_n is not None:
            kwargs['start'] = 0
            kwargs['num'] = self._limit_n

        if self._ascending:
            raw = conn.zrangebyscore(zset_key, min_score, max_score, **kwargs)
        else:
            raw = conn.zrevrangebyscore(
                zset_key, max_score, min_score, **kwargs)

        # A short page means the sorted set had nothing more to give.
        exhausted = self._limit_n is None or len(raw) < self._limit_n

        if not raw:
            return [], exhausted

        members = [member for member, _score in raw]
        # The score ordered the result; the exact nanosecond values come
        # from the hashes, since a double cannot hold them precisely.
        # `mtime`/`ctime` are read for a user index too: the ordering
        # value is the business key, but the row still has a real write
        # time and `entries()` must not lie about it.
        mtimes = conn.hmget(RedisDB._mtime_val_key(folder), members)
        ctimes = conn.hmget(RedisDB._ctime_key(folder), members)
        values = conn.hmget(
            RedisDB._index_val_key(folder, self._index_name), members) \
            if user_index else None

        out = []
        for i, ((member, score), mtime_raw, ctime_raw) in enumerate(
                zip(raw, mtimes, ctimes)):
            name = member.decode() if isinstance(member, bytes) else member
            if user_index:
                # An object can be in a user index while carrying no
                # mtime (written with `mtime-index: false`). 0 is the
                # same "write time unknown" that the epoch tail reports,
                # and the score is emphatically not a timestamp here.
                mtime = int(mtime_raw) if mtime_raw is not None else 0
                ctime = int(ctime_raw) if ctime_raw is not None else mtime
                value_raw = values[i]
                index_value = int(value_raw) if value_raw is not None \
                    else int(score)
            else:
                mtime = int(mtime_raw) if mtime_raw is not None else int(score)
                ctime = int(ctime_raw) if ctime_raw is not None else mtime
                # For an mtime query the ordering value *is* the mtime.
                index_value = mtime
            out.append(Entry(key=name, mtime=mtime, ctime=ctime,
                             index_value=index_value))

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
            yield Entry(key=name, mtime=0, ctime=0, index_value=0)

    def entries(self):
        """ Yield :class:`Entry` objects in query order.

        Any index name is accepted: Redis declares no schema, so a name
        that has never been written is not an error, it is an empty
        sorted set -- and "no object has a value for that index" is
        exactly what a strictly sparse index says by returning nothing.
        """
        self._check_mtime_index_enabled()

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

    #: Redis can store caller-supplied index values: one ZSET plus one
    #: companion hash per folder per index name, maintained in the same
    #: pipeline as the object write.
    supports_user_index = True

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

    def folder_meta_set_raw(self, key: str, value, index=None):
        """ Write the object, its folder-hash entry, its ``mtime``
        metadata and every user index value it carries -- in one round
        trip.

        ``index`` is the validated ``{name: int or None}`` mapping from
        :meth:`kydb.base.BaseDB.set`. Each named value becomes a ``ZADD``
        (the ordering) plus an ``HSET`` (the exact value), mirroring the
        ``mtime`` pair exactly; ``None`` is an explicit clear and becomes
        ``ZREM`` + ``HDEL``. An index the caller does not mention gets no
        command at all, which is precisely preserve-on-rewrite
        (``user_index_plan.md`` §4.4) -- an incidental rewrite elsewhere
        in an application must not silently drop the object out of a
        business index.

        The index *name* is also recorded in a per-folder set
        (:meth:`_index_names_key`) so :meth:`delete_raw` can find every
        index an object might be in without scanning the keyspace.

        User index maintenance is deliberately **not** gated on
        ``mtime_index_enabled``: that switch turns off the backend's own
        recency stamp, and a caller passing an explicit business value
        has said nothing about wanting it dropped.
        """
        folder, obj = key.rsplit('/', 1)
        # One pipeline, one round trip. These commands are unconditional
        # with no intermediate reads, so batching them costs nothing and
        # keeps a write cheaper than the two round trips it took before
        # the index existed.
        pipe = self.connection.pipeline(transaction=False)
        pipe.hset(folder, obj, '.')
        pipe.set(key, value)

        if index and not self._is_folder_meta(obj):
            # Directories stay out of every index, user indexes included
            # -- the same sparseness that keeps them out of `mtime`.
            self._pipe_index_writes(pipe, folder, obj, index)

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

    def _pipe_index_writes(self, pipe, folder: str, obj: str, index: dict):
        """ Queue one folder's user index maintenance onto ``pipe``.

        ``ZADD``/``HSET`` to write a value, ``ZREM``/``HDEL`` to clear
        one. The value is stored twice on purpose: the ZSET score does
        the ordering, the hash holds the value the query reports. For an
        ``int`` in any plausible business range the two are the same
        number -- a double is exact to ``2**53`` -- so unlike ``mtime``
        the hash buys no precision here. It buys a read path structurally
        identical to ``mtime``'s, and room to widen the accepted value
        type later without migrating stored data (plan §3.4).
        """
        for name, value in index.items():
            if value is None:
                pipe.zrem(self._index_key(folder, name), obj)
                pipe.hdel(self._index_val_key(folder, name), obj)
                continue

            pipe.zadd(self._index_key(folder, name), {obj: value})
            pipe.hset(self._index_val_key(folder, name), obj, value)
            # SADD is idempotent, so the name is recorded on every write
            # rather than only the first -- one extra queued command
            # against a round trip to find out whether it was needed.
            pipe.sadd(self._index_names_key(folder), name)

    def _index_names(self, folder: str):
        """ The user index names ever written in ``folder``. """
        raw = self.connection.smembers(self._index_names_key(folder))
        return sorted(n.decode() if isinstance(n, bytes) else n for n in raw)

    def delete_raw(self, key: str):
        """ Delete the object, its folder-hash entry, and every index
        entry that refers to it.

        Two round trips rather than one: the folder's user index names
        have to be read before the cleanup pipeline can be built,
        because the keys to clean are named after them. That read earns
        its cost -- an index entry outliving its object is not untidiness
        but a later query returning a name with nothing behind it, and
        the caller has no way to tell that apart from a real result. A
        folder that has never been written with a user index reads an
        empty set, and the pipeline is byte-for-byte the one it always
        was.

        The `mtime` cleanup is unconditional rather than gated on
        mtime_index_enabled: the entries are harmless no-ops when the
        index is off, and running them anyway means a db that had the
        index disabled after use does not leave orphaned entries behind.
        """
        folder, obj = key.rsplit('/', 1)
        index_names = self._index_names(folder)

        pipe = self.connection.pipeline(transaction=False)
        pipe.delete(key)
        pipe.hdel(folder, obj)
        pipe.zrem(self._mtime_key(folder), obj)
        pipe.hdel(self._mtime_val_key(folder), obj)
        pipe.hdel(self._ctime_key(folder), obj)
        for name in index_names:
            pipe.zrem(self._index_key(folder, name), obj)
            pipe.hdel(self._index_val_key(folder, name), obj)
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

    @classmethod
    def _index_key(cls, folder: str, name: str) -> str:
        """ The ZSET ordering ``folder`` by the user index ``name``.

        The `mtime` keys above are spelled out literally rather than
        routed through here, even though ``_index_key(folder, 'mtime')``
        is byte-for-byte ``_mtime_key(folder)``. Those names are on disk
        in every existing database and must not move; keeping them as
        their own constants makes that impossible to break by accident,
        and ``mtime`` is a reserved index name so the two can never be
        reached for the same query anyway.

        Index names match ``[A-Za-z][A-Za-z0-9_]*`` (validated in
        ``BaseDB._validate_index``), so no two names can produce the same
        key. The `kydb:` prefix keeps every index key out of the object
        namespace -- object paths always start with '/' -- which is what
        stops an object at the derived path breaking the next write to
        its folder with WRONGTYPE (``additional_index_plan.md`` §9.5).
        """
        return cls._INDEX_PREFIX + name + '-index:' + folder

    @classmethod
    def _index_val_key(cls, folder: str, name: str) -> str:
        """ The hash of exact values for the user index ``name``. """
        return cls._INDEX_PREFIX + name + '-values:' + folder

    @classmethod
    def _index_names_key(cls, folder: str) -> str:
        """ The set of user index names ever written in ``folder``.

        Redis has no schema to enumerate, so without this the only way
        to find the index keys holding a deleted object would be to scan
        the keyspace -- and a missed key means an orphaned ZSET member,
        which is a silently wrong query result rather than a visible
        failure. The set is written on every indexed write and read once
        per delete.
        """
        return cls._INDEX_PREFIX + 'index-names:' + folder

    def folder(self, folder: str, allow_scan: bool = False) \
            -> RedisFolderQuery:
        """ Implements folder in KYDBInterface, backed by a native
        per-folder sorted set (see :class:`RedisFolderQuery`).

        ``allow_scan`` is accepted for signature consistency with the
        other backends but is a no-op here: Redis's sorted set is a
        genuinely native ordering index, so there is no scan fallback to
        opt into.

        ``by('mtime')`` raises ``IndexNotSupported`` when the index is
        disabled in config -- Redis stores no per-key timestamp of its
        own, so with the sorted set unmaintained there is nothing to
        order by, and ``allow_scan=True`` cannot rescue it. The check
        runs with the query rather than here, so it applies to ``mtime``
        alone: a user index has its own sorted set, still maintained on
        every write, and stays queryable throughout.
        """
        return RedisFolderQuery(self, folder, allow_scan=allow_scan)

    def reindex(self, folder: str) -> int:
        """Add folder-hash objects missing from the recency sorted set.

        All legacy objects receive one shared migration timestamp. Existing
        members and their exact ``mtime``/``ctime`` hashes are untouched.
        """
        if not self.mtime_index_enabled:
            self._raise_mtime_index_disabled()

        full_folder = self._ensure_slashes(self._get_full_path(folder))[:-1]
        raw_names = self.connection.hkeys(full_folder)
        raw_indexed = self.connection.zrange(
            self._mtime_key(full_folder), 0, -1)

        def decode(value):
            return value.decode() if isinstance(value, bytes) else value

        names = [decode(value) for value in raw_names]
        indexed = {decode(value) for value in raw_indexed}
        missing = [
            name for name in names
            if name not in indexed and not self._is_folder_meta(name)
        ]
        if not missing:
            return 0

        now_ns = time.time_ns()
        pipe = self.connection.pipeline(transaction=False)
        pipe.zadd(
            self._mtime_key(full_folder),
            {name: now_ns for name in missing})
        for name in missing:
            pipe.hset(self._mtime_val_key(full_folder), name, now_ns)
            pipe.hsetnx(self._ctime_key(full_folder), name, now_ns)
        pipe.execute()
        return len(missing)

    def list_dir_meta_folder(self, folder: str, page_size: int):
        folder = self._ensure_slashes(folder)[:-1]
        try:
            for key in self.connection.hgetall(folder).keys():
                yield key.decode()
        except ResponseError:
            raise KeyError(f'{folder} is not a valid folder')
