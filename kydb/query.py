class Entry:
    """ A single result from a :class:`FolderQuery`.

    Exposes ``.key`` (the folder-relative name, same string ``list_dir``
    would yield), ``.mtime`` and ``.ctime`` (both ``int``, nanosecond
    timestamps -- never a ``decimal.Decimal``).
    """

    __slots__ = ('key', 'mtime', 'ctime')

    def __init__(self, key: str, mtime, ctime):
        self.key = key
        self.mtime = mtime
        self.ctime = ctime

    def __repr__(self):
        return (f'Entry(key={self.key!r}, mtime={self.mtime!r}, '
                f'ctime={self.ctime!r})')

    def __eq__(self, other):
        if not isinstance(other, Entry):
            return NotImplemented
        return (self.key, self.mtime, self.ctime) == \
            (other.key, other.mtime, other.ctime)

    def __hash__(self):
        return hash((self.key, self.mtime, self.ctime))


class FolderQuery:
    """ Lazy, immutable query builder returned by
    :meth:`kydb.interface.KYDBInterface.folder`.

    Each chained call (``by``, ``asc``, ``desc``, ``since``, ``limit``)
    returns a *new* ``FolderQuery`` -- the receiver is never mutated, so a
    half-built query can be safely reused / branched into more than one
    final query::

        base = db.folder('/my/folder').by('mtime')
        newest_10 = base.desc().limit(10)
        oldest_5 = base.asc().limit(5)

    Backend implementations subclass this and implement ``entries()``
    (and, where it is cheaper to skip the extra per-item work ``entries()``
    implies, may also override ``__iter__`` / ``items()`` -- see
    ``kydb.impl.dynamodb.DynamoDBFolderQuery`` for an example).

    Instances are normally obtained via ``db.folder(...)``, not
    constructed directly.
    """

    def __init__(self, db, folder: str, index_name: str = 'mtime',
                 ascending: bool = True, since_ts=None, limit_n=None,
                 allow_scan: bool = False):
        self._db = db
        self._folder = folder
        self._index_name = index_name
        self._ascending = ascending
        self._since_ts = since_ts
        self._limit_n = limit_n
        self._allow_scan = allow_scan

    def _clone(self, **overrides) -> 'FolderQuery':
        kwargs = dict(
            db=self._db, folder=self._folder, index_name=self._index_name,
            ascending=self._ascending, since_ts=self._since_ts,
            limit_n=self._limit_n, allow_scan=self._allow_scan)
        kwargs.update(overrides)
        return type(self)(**kwargs)

    def by(self, index_name: str) -> 'FolderQuery':
        """ Select which index to query by.

        :param index_name: The index name, e.g. ``'mtime'``.
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

    def limit(self, n: int) -> 'FolderQuery':
        """ Cap the number of results to ``n``.

        Backends implementing this lazily must not fetch more pages than
        needed to satisfy ``n`` results.

        :param n: The maximum number of results.
        :returns: A new ``FolderQuery``.
        """
        return self._clone(limit_n=n)

    def entries(self):
        """ Yield :class:`Entry` objects (``.key``, ``.mtime``, ``.ctime``),
        lazily, in query order. To be implemented by derived class.
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
