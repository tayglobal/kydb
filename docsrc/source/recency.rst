Folder recency queries
======================

kydb can list objects in a folder in modification-time order. The common
case is :meth:`~kydb.interface.KYDBInterface.recent`::

    newest_names = db.recent('/articles', limit=10)

The return value is lazy. Iterating it yields folder-relative names, like
``list_dir``. For more control, build a query with
:meth:`~kydb.interface.KYDBInterface.folder`::

    base = db.folder('/articles').by('mtime')

    newest = base.desc().limit(10)              # names
    since = base.since(timestamp_ns).items()    # (name, value)
    metadata = base.desc().entries()            # Entry objects

Each builder method returns a new query, so ``base`` can safely be reused.
``since()`` is inclusive. An ``Entry`` exposes ``key``, ``mtime`` and
``ctime``; timestamps are integer nanoseconds since the Unix epoch.
Directories are not included and the API intentionally has no
``include_dir`` option.

Backend support and scan fallback
---------------------------------

* DynamoDB uses ``folder-time-index`` and Redis uses a native sorted set.
* Memory, Files and S3 require ``allow_scan=True`` because they sort the
  complete folder client-side. Files use ``st_mtime_ns`` and S3 uses
  ``LastModified``.
* HTTP and HTTPS databases do not support recency queries.
* CacheDB delegates to its persistent database. UnionDB merges the ordered
  streams from its supporting members and keeps the front database's value
  when a name occurs more than once.

Unsupported native queries raise ``IndexNotSupported`` instead of silently
performing an expensive scan::

    db.recent('/articles', limit=10, allow_scan=True)

On DynamoDB, ``allow_scan=True`` also supports an older table that has no
``folder-time-index``, provided ``folder-index`` projects ``mtime`` and
``ctime`` (through either ``INCLUDE`` or the historical ``ALL`` projection).
It does not override ``mtime-index: false``: when kydb was told not to record
timestamps, there is no ordering information to scan.

Legacy objects and reindexing
-----------------------------

An object written before recency indexing existed has no recoverable write
time. DynamoDB and Redis report it honestly at ``mtime == ctime == 0``. Such
objects appear after indexed objects in descending order, before them in
ascending order, and do not match ``since(ts)`` when ``ts > 0``. A normal
rewrite gives the object a timestamp and moves it into the index.

No migration is required. If migration-day ordering is preferable, the
optional maintenance call timestamps only the missing objects and returns
the number changed::

    changed = db.reindex('/articles')

``reindex`` preserves existing timestamps and values. Because the original
write times no longer exist, all rows handled by one call receive the same
current timestamp. They therefore move ahead of older indexed objects, and
their relative order is undefined. Files and S3 return zero because their
storage substrates already provide timestamps.

Timestamp ordering caveats
--------------------------

DynamoDB, Redis and Memory timestamps come from ``time.time_ns()`` on the
client. DynamoDB has no server-side timestamp function. Writers whose clocks
drift can therefore be returned in the wrong relative order; recency is not
a distributed total-order guarantee. Writes with identical timestamps may
also appear in either order, although pagination still returns each object
exactly once.

Redis uses its sorted-set score only for ordering and keeps the exact
nanosecond timestamp in a companion hash, avoiding floating-point precision
loss in returned ``Entry`` values. S3 ``LastModified`` is coarser than the
other backends, so ties there are more likely.

Configuration
-------------

The maintained recency index is enabled by default. It can be disabled per
database in the file selected by ``KYDB_CONFIG_PATH``::

    dbs:
      my-table:
        mtime-index: false

This setting affects DynamoDB, Redis and Memory. Files and S3 ignore it
because they read timestamps directly from storage. Objects written while
the setting is false join the epoch tail if it is later enabled.

DynamoDB schema
---------------

See :doc:`implementations` for the complete table definition. In brief,
``folder-time-index`` uses ``folder`` as its partition key and numeric
``mtime`` as its sort key, with ``ctime`` projected by ``INCLUDE``. The
regular ``folder-index`` should use an ``INCLUDE`` projection containing
``mtime`` and ``ctime`` so the explicit scan fallback does not need one read
per object.
