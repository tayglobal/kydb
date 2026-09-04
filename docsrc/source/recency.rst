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
``since()`` and ``until()`` are both inclusive, so ``since(a).until(b)``
is a closed range and an inverted one yields nothing rather than raising.
An ``Entry`` exposes ``key``, ``mtime``, ``ctime`` and ``index_value``
(the value the query ordered by); timestamps are integer nanoseconds
since the Unix epoch. Directories are not included and the API
intentionally has no ``include_dir`` option.

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

Business keys
-------------

``mtime`` answers *when was this written*. Often the question is *what
does this mean* -- and the two are not the same. A gym takes a booking on
Tuesday for Saturday's 18:30 HIIT class; the roster the front desk needs
every morning is ordered by the class date, and ``mtime`` records
Tuesday.

Write a business key with ``set(..., index={...})`` and query it with the
same builder::

    db.set('/signups/anna', booking, index={'class_date': 20260905})

    # who is booked into Saturday's classes -- one indexed query
    for name, rec in db.folder('/signups').by('class_date') \
                       .since(20260905).until(20260905).items():
        ...

    # and mtime still answers its own question, unchanged
    db.recent('/signups', limit=10)     # who booked most recently

Rebooking is the ordinary write the application was already making, with
one more argument -- the object's key never moves::

    db.set('/signups/anna', booking, index={'class_date': 20260907})

Values are ``int``, and only ``int``
....................................

A date is ``int(d.strftime('%Y%m%d'))``, a datetime an epoch value, a
rank or priority already numeric. Restricting this to integers buys one
comparison semantic on every backend: a Redis sorted-set score is a
double, so ordering strings would need a parallel lexicographic read path
with different bound semantics on the same builder, and ``since()`` would
mean numeric comparison on one index and bytewise comparison on another.

``bool`` is rejected explicitly -- it is an ``int`` subclass, and a
boolean in a business ordering is a mistake rather than an intent. A bad
value raises ``TypeError`` naming the index. Index names must match
``[A-Za-z][A-Za-z0-9_]*`` and must not be ``path``, ``folder``,
``contents``, ``mtime`` or ``ctime``; a bad name raises ``ValueError``.
Both are raised before anything is written.

A rewrite preserves index values it does not mention
....................................................

::

    db.set('/signups/anna', rec, index={'class_date': 20260905})
    db.set('/signups/anna', updated_rec)                       # still 20260905
    db.set('/signups/anna', rec, index={'class_date': None})   # now cleared

The alternative -- an unmentioned index is cleared -- would mean every
incidental rewrite anywhere in an application silently drops the object
out of the index, which is the failure mode hardest to notice and hardest
to explain. Clearing is therefore explicit, with ``None``. ``db[key] =
value`` never touches index values.

Strictly sparse: no epoch tail
..............................

An object with no value for the queried index does **not** appear in that
index's results, in either direction, and there is no fallback read.

This is the deliberate difference from ``mtime``, where an object with no
timestamp is reported at ``mtime == ctime == 0`` (see *Legacy objects and
reindexing* above) because a missing write time still means something:
*older than anything the index has tracked*. A missing ``class_date``
means nothing of the sort. An object with no class date is not a signup
for an infinitely distant past class; it is not a signup at all, and
inventing a position for it in a business ordering would put junk in
every result.

Two consequences follow. A user index query is *cheaper* than its
``mtime`` counterpart -- ``asc()`` has no O(folder) tail to discover
first. And ``reindex()`` is ``mtime``-only: nothing in the object, the
item or the clock says which class a booking with no ``class_date`` was
for, so any value it stamped would be fabricated. Rewrite those objects
with ``set(index=...)`` instead; a GSI added later picks up every value
already written.

Backend support
...............

* DynamoDB, Redis and Memory store index values. Memory still requires
  ``allow_scan=True``, as it does for ``mtime``.
* Files and S3 **raise** ``IndexNotSupported`` on ``set(index=...)``.
  Neither has anywhere to put a caller-supplied attribute that kydb
  models, and accepting the value and discarding it is the one behaviour
  that lets a bug reach production undetected: every write succeeds, and
  only the query -- later, elsewhere -- comes back empty.
* ``CacheDB`` records index values on its persistent database only, which
  is where its folder queries already read from.
* ``UnionDB`` writes to its front database and merges the members that
  can hold index values, skipping those that cannot. A bounded query over
  a union has a documented correctness limitation -- see below.
* ``mtime-index: false`` does **not** affect user indexes, on either the
  read or the write side. That setting turns off the timestamp kydb stamps
  for itself; a business key is a value you supplied and stored on the
  object, so ``by('class_date')`` keeps working on a database where
  ``by('mtime')`` and ``recent()`` raise.

Bounded queries over a UnionDB
..............................

A union merges the rows its members return, and **each member applies**
``since``/``until`` **itself**. Front-db-wins is then resolved among the
rows that survived filtering -- so a key held by two members can appear
even when the front database's copy, the authoritative one, falls outside
the bounds.

With ``/signups/anna`` at ``class_date=20260905`` in the front database
and ``20260907`` in the back, both days list her::

    union['/signups/anna']['day']                      # 20260905
    roster(20260905)                                   # ['anna']
    roster(20260907)                                   # ['anna', 'yuki']   <-- wrong

An **unbounded** query is unaffected: every member returns every row, so
front-db-wins is complete and ``anna`` appears once, on her real day. The
common ``recent(folder, limit=10)`` is in this safe category.

The hole is not specific to business keys -- ``by('mtime').since(...)``
reaches it whenever a back database holds a newer copy -- and it predates
user indexes.

It is **not fixed**, deliberately. Closing it means resolving key
ownership before filtering: at best an existence check per surviving row
against every database in front of its own, and ``exists()`` in kydb is a
full read rather than a key probe, so even the cheap form is a read per
row. On an unbounded query it converges on a full folder listing per
member. That is a permanent cost on every union query, to correct a
result that is only wrong when two members disagree about the same key.

Instead kydb **logs** it. A bounded query over a union of two or more
members emits one ``logging`` WARNING per ``(folder, index)`` on the
``kydb.union`` logger, naming the folder and index and pointing here.
Every union index query is also logged at DEBUG. kydb configures no
handlers, so nothing is printed until the application sets logging up.
If the warning does not apply to you -- a union whose members never hold
the same key cannot hit this -- silence it explicitly::

    logging.getLogger('kydb.union').setLevel(logging.ERROR)

When a row's index value matters, re-read it with ``db[key]`` to get the
authoritative copy.

DynamoDB needs one GSI per index name
.....................................

``by('class_date')`` is served by a GSI named ``folder-class_date-index``
-- ``folder`` as partition key, ``class_date`` (Number) as sort key,
projection ``INCLUDE`` with ``mtime`` and ``ctime``. The name is derived
from the index name, so nothing has to be declared to kydb.

The GSI is entirely opt-in and is never needed to *write*: an
``update_item`` just sets an attribute. So an application can start
recording index values before the index exists, and every value is
already there when it is added. Until then, only ``by('<name>')`` fails,
with an ``IndexNotSupported`` naming the index to create.
``allow_scan=True`` cannot substitute for it -- the scan fallback reads
``folder-index``, whose projection carries no user attribute -- and says
so.

DynamoDB allows 20 GSIs per table and kydb's own two are already spoken
for, so 18 user indexes is the real ceiling.

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

It also closes the door on *reading* business keys, because ``folder()``
raises before ``by()`` has been called and so cannot know which index was
wanted. Business-key **writes** are unaffected -- an explicit
``index={...}`` says nothing about wanting the backend's own timestamp --
so the values are recorded correctly throughout and every one of them is
queryable again the moment the setting is turned back on.

DynamoDB schema
---------------

See :doc:`implementations` for the complete table definition. In brief,
``folder-time-index`` uses ``folder`` as its partition key and numeric
``mtime`` as its sort key, with ``ctime`` projected by ``INCLUDE``. The
regular ``folder-index`` should use an ``INCLUDE`` projection containing
``mtime`` and ``ctime`` so the explicit scan fallback does not need one read
per object. Each business key adds one more index of the same shape,
``folder-<name>-index``, with the index name as its numeric sort key.
