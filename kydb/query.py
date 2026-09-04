from .exceptions import IndexNotSupported


class Entry:
    """ A single result from a :class:`FolderQuery`.

    Exposes ``.key`` (the folder-relative name, same string ``list_dir``
    would yield), ``.mtime`` and ``.ctime`` (both ``int``, nanosecond
    timestamps -- never a ``decimal.Decimal``), and ``.index_value`` --
    the value the query actually ordered by.

    ``.index_value`` is a separate slot rather than a reuse of
    ``.mtime`` because the two answer different questions. A result set
    ordered by a user index (``by('class_date')``) still carries a
    genuine ``mtime``: the row says *when the object was written* and
    *where it sits in the business ordering*, and folding one into the
    other would make ``entries()`` lie about the former. For an
    ``mtime`` query the two are equal, so nothing changes for callers
    that predate user indexes.

    ``index_value`` is last and defaults to ``None`` so the existing
    three-argument construction sites keep working while each backend is
    updated to report it.
    """

    __slots__ = ('key', 'mtime', 'ctime', 'index_value')

    def __init__(self, key: str, mtime, ctime, index_value=None):
        self.key = key
        self.mtime = mtime
        self.ctime = ctime
        self.index_value = index_value

    def __repr__(self):
        return (f'Entry(key={self.key!r}, mtime={self.mtime!r}, '
                f'ctime={self.ctime!r}, index_value={self.index_value!r})')

    def __eq__(self, other):
        if not isinstance(other, Entry):
            return NotImplemented
        return (self.key, self.mtime, self.ctime, self.index_value) == \
            (other.key, other.mtime, other.ctime, other.index_value)

    def __hash__(self):
        return hash((self.key, self.mtime, self.ctime, self.index_value))


class FolderQuery:
    """ Lazy, immutable query builder returned by
    :meth:`kydb.interface.KYDBInterface.folder`.

    Each chained call (``by``, ``asc``, ``desc``, ``since``, ``until``,
    ``limit``) returns a *new* ``FolderQuery`` -- the receiver is never
    mutated, so a half-built query can be safely reused / branched into
    more than one final query::

        base = db.folder('/my/folder').by('mtime')
        newest_10 = base.desc().limit(10)
        oldest_5 = base.asc().limit(5)

    The index queried need not be ``mtime``: ``by('class_date')`` orders
    by a caller-supplied index value written with
    ``db.set(key, value, index={'class_date': 20260905})``. The bounds
    are then business values rather than timestamps, and
    ``since(d).until(d)`` is a single-day query.

    Backend implementations subclass this and implement ``entries()``
    (and, where it is cheaper to skip the extra per-item work ``entries()``
    implies, may also override ``__iter__`` / ``items()`` -- see
    ``kydb.impl.dynamodb.DynamoDBFolderQuery`` for an example).

    Instances are normally obtained via ``db.folder(...)``, not
    constructed directly.
    """

    def __init__(self, db, folder: str, index_name: str = 'mtime',
                 ascending: bool = True, since_ts=None, limit_n=None,
                 allow_scan: bool = False, until_ts=None):
        self._db = db
        self._folder = folder
        self._index_name = index_name
        self._ascending = ascending
        self._since_ts = since_ts
        self._until_ts = until_ts
        self._limit_n = limit_n
        self._allow_scan = allow_scan

    def _clone(self, **overrides) -> 'FolderQuery':
        kwargs = dict(
            db=self._db, folder=self._folder, index_name=self._index_name,
            ascending=self._ascending, since_ts=self._since_ts,
            until_ts=self._until_ts,
            limit_n=self._limit_n, allow_scan=self._allow_scan)
        kwargs.update(overrides)
        return type(self)(**kwargs)

    def by(self, index_name: str) -> 'FolderQuery':
        """ Select which index to query by.

        :param index_name: The index name, e.g. ``'mtime'``, or the name
                           of a user index written with
                           ``set(..., index={...})``.
        :returns: A new ``FolderQuery``.
        """
        return self._clone(index_name=index_name)

    def asc(self) -> 'FolderQuery':
        """ Order results oldest-first. Returns a new ``FolderQuery``. """
        return self._clone(ascending=True)

    def desc(self) -> 'FolderQuery':
        """ Order results newest-first. Returns a new ``FolderQuery``. """
        return self._clone(ascending=False)

    def since(self, ts) -> 'FolderQuery':
        """ Only include entries whose index value is ``>= ts``
        (inclusive).

        :param ts: The threshold value, in the same units as the index
                   (e.g. nanoseconds for ``mtime``).
        :returns: A new ``FolderQuery``.
        """
        return self._clone(since_ts=ts)

    def until(self, ts) -> 'FolderQuery':
        """ Only include entries whose index value is ``<= ts``
        (inclusive).

        :param ts: The threshold value, in the same units as the index
                   (e.g. nanoseconds for ``mtime``).
        :returns: A new ``FolderQuery``.

        Both bounds are inclusive, so ``since(d).until(d)`` selects
        exactly the entries whose index value *is* ``d`` -- the
        single-day query a business index is usually asked for. On
        ``mtime`` it closes the range that ``since()`` alone could only
        leave open: "what changed yesterday" rather than "what changed
        since yesterday".

        An inverted range (``since(5).until(3)``) yields nothing rather
        than raising. That is what the underlying DynamoDB ``between``
        does, and an empty result is the honest answer to an empty
        range -- the caller asked for values that are both above 5 and
        below 3, and there are none.
        """
        return self._clone(until_ts=ts)

    def limit(self, n: int) -> 'FolderQuery':
        """ Cap the number of results to ``n``.

        Backends implementing this lazily must not fetch more pages than
        needed to satisfy ``n`` results.

        :param n: The maximum number of results.
        :returns: A new ``FolderQuery``.
        """
        return self._clone(limit_n=n)

    def _supports_index(self, name: str) -> bool:
        """ Whether this query can order by the index ``name``.

        The single place a backend says which ``by()`` names it can
        serve, so a subclass relaxes the rule by overriding one method
        rather than by finding a value that satisfies a membership test.

        The default answer comes from ``_SUPPORTED_INDEXES`` where a
        subclass declares one, and is otherwise "yes": a backend with no
        declaration has no fixed set to enumerate -- Redis and DynamoDB
        derive the index's storage from its name, so any name a write
        could have used is queryable, and whether it holds anything is
        the store's answer to give, not a tuple's. A name nobody has
        written is not unsupported; it simply has no rows, which a
        strictly sparse index already reports honestly by returning
        nothing.
        """
        supported = getattr(self, '_SUPPORTED_INDEXES', None)
        return True if supported is None else name in supported

    def _check_mtime_index_enabled(self):
        """ Raise if this query needs ``mtime`` and the db is not
        maintaining it (``mtime-index: false`` in the kydb config).

        Called when the query *runs*, not when it is built, and only
        when the index actually being queried is ``mtime``. That is the
        whole point: ``folder()`` cannot know which index the caller
        wants, because ``by()`` is chained onto the object it returns.
        Checking eagerly there meant ``mtime-index: false`` also closed
        ``by('class_date')`` -- a db would record caller-supplied index
        values correctly and then refuse to query them back.

        The setting says "do not maintain *my* timestamp index". A
        business key is not a timestamp the backend stamped; it is a
        value the caller supplied, stored on the object, and still
        perfectly orderable. So only ``mtime`` is gated.

        Backends call this from whichever method actually reads the
        index, which is also what keeps the exception's timing
        unchanged for existing callers: ``list(db.recent(f))`` raises
        exactly as it always did, because ``recent()`` was already
        lazy.
        """
        if self._index_name != 'mtime':
            return

        if not getattr(self._db, 'mtime_index_enabled', True):
            self._db._raise_mtime_index_disabled()

    def entries(self):
        """ Yield :class:`Entry` objects (``.key``, ``.mtime``,
        ``.ctime``, ``.index_value``), lazily, in query order. To be
        implemented by derived class.
        """
        raise NotImplementedError()

    def __iter__(self):
        """ Yield names (``str``), lazily, in query order. """
        for entry in self.entries():
            yield entry.key

    def items(self):
        """ Yield ``(name, value)`` pairs, lazily, in query order. """
        for entry in self.entries():
            key = self._db._ensure_slashes(self._folder) + entry.key
            yield entry.key, self._db.read(key)


class ScanFolderQuery(FolderQuery):
    """ Generic client-side scan-and-sort ``FolderQuery``, for backends
    with no server-side ordering index (Memory, Files, S3).

    Only reached behind the ``allow_scan=True`` opt-in -- by the time one
    of these is constructed, the backend's ``folder()`` has already
    decided the O(n) scan cost is the caller's explicit choice.

    Subclasses implement :meth:`_raw_entries`, yielding a
    ``(name, mtime, ctime, index_value)`` row for every object directly
    in the folder (never directories), in any order, where
    ``index_value`` is that object's value for ``self._index_name``.
    This base class filters on ``since()``/``until()``, sorts by
    ``asc()``/``desc()`` and applies ``limit()`` -- all of them on the
    *index value*, not on ``mtime``, which is what lets a scan backend
    serve a user index with no extra query code of its own.

    A three-element ``(name, mtime, ctime)`` row is still accepted, from
    backends that only ever report ``mtime`` (Files and S3 read theirs
    from the substrate and have nowhere to store a user index value).
    It is read as "``mtime`` is the only index this row knows about".

    ``_SUPPORTED_INDEXES`` stays ``('mtime',)`` here -- Files and S3
    read their timestamps from the substrate and have nowhere to put a
    caller-supplied value. A backend that *can* store user index values
    relaxes it, either by widening the tuple or by overriding
    :meth:`FolderQuery._supports_index`.
    """

    _SUPPORTED_INDEXES = ('mtime',)

    def _raw_entries(self):
        """ Yield ``(name, mtime, ctime, index_value)`` rows for the
        objects directly in the folder. To be implemented by derived
        class.
        """
        raise NotImplementedError()

    def _normalised_rows(self):
        """ Yield every raw row as a 4-tuple, widening the legacy
        three-element form.

        For a three-element row the queried index can only be ``mtime``
        (that is all :meth:`FolderQuery._supports_index` admits when a
        subclass has not been generalised), so the index value *is* the
        ``mtime``.
        Any other index name gets ``None`` -- meaning "this object has
        no value for that index" -- which :meth:`entries` then drops.
        """
        for row in self._raw_entries():
            if len(row) == 3:
                name, mtime, ctime = row
                index_value = mtime if self._index_name == 'mtime' else None
            else:
                name, mtime, ctime, index_value = row
            yield name, mtime, ctime, index_value

    def entries(self):
        """ Yield :class:`Entry` objects in query order.

        Objects with no value for the queried index (``index_value is
        None``) are **dropped**, never defaulted to 0.

        That is the deliberate difference from the ``mtime`` epoch tail
        (``additional_index_plan.md`` §9.3), where a missing timestamp
        is reported at ``mtime == ctime == 0`` because it still means
        something: *older than anything the index has tracked*. A
        missing user index value means nothing of the sort. An object
        with no ``class_date`` is not a signup for an infinitely distant
        past class; it is not a signup at all. Fabricating a position
        for it in a business ordering would put junk in every result, so
        user index queries are strictly sparse in both directions.

        (A row that reaches here with ``mtime`` as the queried index
        always carries a real timestamp -- the scan backends read it
        from the substrate or from the write path -- so this does not
        disturb ``mtime`` behaviour.)
        """
        if not self._supports_index(self._index_name):
            raise IndexNotSupported(
                f"{type(self._db).__name__} scan query only supports "
                f"by('mtime'), got by({self._index_name!r})")

        self._check_mtime_index_enabled()

        rows = []
        for name, mtime, ctime, index_value in self._normalised_rows():
            if index_value is None:
                continue
            if self._since_ts is not None and index_value < self._since_ts:
                continue
            if self._until_ts is not None and index_value > self._until_ts:
                continue
            rows.append((name, mtime, ctime, index_value))

        rows.sort(key=lambda r: r[3], reverse=not self._ascending)

        if self._limit_n is not None:
            rows = rows[:self._limit_n]

        for name, mtime, ctime, index_value in rows:
            yield Entry(key=name, mtime=mtime, ctime=ctime,
                        index_value=index_value)
