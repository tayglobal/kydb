# User-Supplied Index Values: Querying by a Business Key

Status: approved — design verified against Moto 5.2.3 / boto3 1.43.88
Scope: design for all backends; implementation targets DynamoDB, Redis, Memory
Date: 2026-09-04
Follows: `additional_index_plan.md` (the `mtime` recency index, merged in #51)

## 1. Problem

`additional_index_plan.md` §6 left `by()` deliberately open — "a future
non-time index is `by('status')` — the same API, no new method". That future
is not reachable today: every backend declares
`_SUPPORTED_INDEXES = ('mtime',)` (`kydb/query.py:141`,
`kydb/impl/dynamodb.py:70`, `kydb/impl/redis.py:36`), so `by('anything_else')`
raises `IndexNotSupported`.

The gap this exposes is not "another index" in the abstract. It is that
**kydb can only order by when a write happened, never by what the object
means**. The timestamp is stamped by the backend
(`kydb/impl/dynamodb.py:331`, `kydb/impl/redis.py:257`,
`kydb/impl/memory.py:54` — all `time.time_ns()`), and `set()`
(`kydb/base.py:189`) has no parameter that could carry a caller value into
the index.

### 1.1 The motivating use case

A gym books students into classes. A signup written on Tuesday is for
Saturday's 18:30 HIIT class. The question the front desk asks every morning
is *"who is booked into today's classes?"* — and `mtime` cannot answer it,
because it records Tuesday.

Today the only efficient workaround is to encode the business date in the
path (`/signups/2026-09-05/hiit-1830/anna`) and lean on `list_dir`, which is
a real GSI query on `folder` (`kydb/impl/dynamodb.py:459`). That works, and
remains a perfectly good schema, but it has two costs: the date becomes part
of the object's identity, so rebooking is a non-atomic delete-then-set
(`additional_index_plan.md` §3D rejected path-encoded time for exactly this
reason), and only one attribute can ever be hierarchised this way.

## 2. Recommendation (summary)

| Decision | Choice |
| --- | --- |
| Write API | `db.set(key, value, index={'class_date': 20260905})` |
| Value type | **`int` only** in v1 (§4.2). Strings deferred, with reasons |
| Read API | Existing builder: `db.folder(f).by('class_date')`, plus a new `until()` bound |
| Day-exact query | `.since(d).until(d)` — the gym query (§6.2) |
| DynamoDB storage | One sparse GSI per index name: `folder-<name>-index`, `folder` HASH, `<name>` RANGE, projection `INCLUDE ['mtime','ctime']` |
| Redis storage | One ZSET per folder per index name, mirroring the `mtime` structure |
| Memory storage | Per-object dict of index values; client-side sort behind `allow_scan=True` |
| Files / S3 | Unsupported. `set(index=...)` **raises** rather than silently dropping |
| Missing value | Row is absent from the index. **No epoch tail** (§5) |
| Rewrite with no `index=` | **Preserves** the existing value (§4.4) |
| Clearing a value | Explicit `index={'class_date': None}` |
| Declaration | None required — the GSI name is derived from the index name |

## 3. Storage

### 3.1 DynamoDB — one GSI per index name — **chosen**

The index value is stored as a plain attribute on the object's own item, and
a GSI named `folder-<index_name>-index` sorts it:

```
folder-class_date-index:  folder HASH, class_date RANGE (Number)
                          projection INCLUDE ['mtime', 'ctime']
```

This is the `folder-time-index` shape with the sort key parameterised, so it
inherits every property already established and tested in
`additional_index_plan.md`: one query, server-side ordering, native
`LastEvaluatedKey` pagination, no extra write per object, and automatic
sparseness (an object with no `class_date` is simply not in the index).

`mtime` and `ctime` are projected so `entries()` can still report them from
the index page alone, with no per-row read — the N+1 mistake corrected in
§12 of the previous plan. `contents` stays out, for the same reason as
before.

**Cost: the 20-GSI-per-table limit.** Two are already spoken for
(`folder-index`, `folder-time-index`), leaving 18 user indexes. This is a
real ceiling and should be documented rather than hidden. It is also the
honest one: DynamoDB charges for index maintenance per index, and pretending
otherwise would only move the cost somewhere less visible.

### 3.2 Rejected: one generic GSI with a composite sort key

Store `uidx = f'{index_name}#{padded_value}'` and use a single GSI for every
named index. It dodges the 20-index ceiling, but a DynamoDB item can hold
only one value per attribute name, so an object could belong to exactly one
user index at a time. Supporting two would require separate index *rows*,
which is `additional_index_plan.md` §3C — non-atomic read-modify-write on a
hot key — rejected there and no better here.

### 3.3 Rejected: deriving index values from the stored object

`db.index_on('/signups', 'class_date', lambda obj: obj['class_date'])`, or a
`DbObj` field declaration. It removes the chance of a write forgetting the
index, but most kydb values are plain pickled dicts with no schema, the
extractor must then be registered identically in every process that writes,
and a changed extractor silently invalidates existing index entries with no
way to detect it. Explicit-at-the-call-site loses less.

### 3.4 Redis

Exactly the `mtime` structure, keyed by index name — `kydb:<name>-index:<folder>`
(ZSET, ordering), `kydb:<name>-values:<folder>` (hash, exact values). `ctime`
has no analogue here: a user index has one value, not a created/modified pair.

**Correction, recorded against the as-built code.** This section
originally specified the suffix form `<folder>:<name>-index`. The merged
implementation uses the `kydb:`-prefixed form above, and the `mtime` keys
it mirrors (`kydb:mtime-index:<folder>`, `kydb:mtime-values:<folder>`,
`kydb:ctime-index:<folder>`) are spelled the same way. The reason is the
§9.5 collision below: every kydb object path begins with `/`
(`BaseDB._get_full_path`), so a `kydb:`-prefixed key can never be an
object path, whereas the suffix form would let an existing object at
`/a/b:class_date-index` break the next write to `/a/b` with a
`WRONGTYPE` error. A third key, `kydb:index-names:<folder>`, holds the
set of index names ever written in a folder so `delete_raw` knows which
keys to clean without scanning the keyspace.

The score-precision problem that forced the companion hash for `mtime`
(`additional_index_plan.md` §7) does not arise for `int` values in any
plausible business range — `20260905` is 14 orders of magnitude below
`2**53`. The companion hash is kept anyway, for one reason: it makes the
Redis read path structurally identical to the `mtime` one, and it is what
lets §4.2 relax to floats or larger ints later without a storage change.

The key-namespace collision noted in `additional_index_plan.md` §9.5 is
therefore closed rather than merely not-made-worse: with the `kydb:`
prefix, no index key can be spelled the same way as an object path.

### 3.5 Memory

A per-db `{full_path: {index_name: value}}` dict alongside the existing
`__meta`. Sorted in Python behind `allow_scan=True`, as `mtime` already is.

### 3.6 Files and S3 — unsupported, loudly

Neither has anywhere to put a caller-supplied attribute without inventing
sidecar storage (xattrs, S3 object metadata) that the rest of kydb does not
model. Both already raise `IndexNotSupported` for a non-`mtime` `by()`, which
covers the read side.

The write side must be closed too: `set(key, value, index={...})` on Files or
S3 **raises `IndexNotSupported`**. Accepting an index value and silently
discarding it is the one behaviour that would let a bug reach production
undetected — the writes succeed, and the query returns nothing.

## 4. Write API

### 4.1 Shape

```python
db.set('/signups/anna', record, index={'class_date': 20260905})
db['/signups/anna'] = record          # unchanged; no index value
```

`index` is a keyword-only mapping of index name to value, added to
`BaseDB.set` (`kydb/base.py:189`) and threaded through `set_raw` /
`folder_meta_set_raw`. `__setitem__` is untouched.

`db.set_index(key, {...})` — updating index values without rewriting the
payload — is deliberately **not** in v1. It is a natural follow-up, but it
needs its own thinking about what happens when the object does not exist.

**`DbObj` values carry index values too.** `ObjDBMixin.write_dbobj` has
its own serialisation path — the object's `get_stored_dict()` plus the
metadata needed to rebuild it, pickled there rather than by
`BaseDB._serialise` — so `index` is threaded through it explicitly and
passed on only when non-empty, leaving the two-argument `set_raw` that
unsupported backends implement untouched. `CacheDB.set_raw` had to learn
the argument for the same reason: `write_dbobj` calls `obj.db.set_raw`,
which for a cached db is the wrapper's own, and it routes the values to
`persist_db` only.

### 4.2 Values are `int` — and why the restriction is worth it

DynamoDB sort keys may be Number, String or Binary, so strings are storable.
They are excluded from v1 because they do not survive the cross-backend
contract:

- A Redis ZSET score is a double. Ordering strings needs a *lexicographic*
  sorted set (all scores equal, `ZRANGEBYLEX`), which is a second, parallel
  read path with different bound semantics.
- `since()`/`until()` would mean numeric comparison on one index and
  bytewise comparison on another, on the same builder, distinguished only by
  the type the caller last wrote.

`int` costs the caller very little: a date is `20260905`
(`int(d.strftime('%Y%m%d'))`), a datetime is an epoch value, a rank or
priority is already numeric. What it buys is one comparison semantic, one
Redis structure, and one `Entry` type on every backend.

Enforced on write with a clear `TypeError` naming the index and the type
received. `bool` is rejected explicitly — it is an `int` subclass and
almost certainly a mistake.

### 4.3 Reserved names

An index name becomes a DynamoDB attribute name, so it must not collide with
the item's own structure. Reject `path`, `folder`, `contents`, `mtime`,
`ctime`, and any name that is not a valid identifier of
`[A-Za-z][A-Za-z0-9_]*`. This also keeps the derived GSI name
(`folder-<name>-index`) and the Redis key (`<folder>:<name>-index`)
unambiguous. `mtime` is reserved because it is maintained by the backend;
a caller wanting to override it is asking for `additional_index_plan.md`'s
clock-skew problem back.

### 4.4 A rewrite preserves index values it does not mention

```python
db.set('/signups/anna', rec, index={'class_date': 20260905})
db.set('/signups/anna', updated_rec)      # class_date still 20260905
db.set('/signups/anna', rec, index={'class_date': None})   # now cleared
```

This falls straight out of `update_item`: a `SET` expression that does not
name `class_date` leaves it alone (verified, §8 #7). The alternative — an
unmentioned index is cleared — would mean every incidental rewrite anywhere
in an application silently drops the object out of the index, which is the
failure mode hardest to notice and hardest to explain.

Clearing is therefore explicit: `None` compiles to a `REMOVE` on DynamoDB
(verified, §8 #8), `ZREM`+`HDEL` on Redis.

The cost of preserve-by-default is that a *moved* booking must be updated,
not merely rewritten — but that is a one-line `index=` on a write the
application was already making, and unlike the path-encoded schema it stays a
single atomic item update.

## 5. No epoch tail — the deliberate difference from `mtime`

`additional_index_plan.md` §9.3 gives objects with no `mtime` a synthetic
`mtime == ctime == 0` and yields them after every indexed row, because a
missing write-time still means something: *older than anything tracked*.

A missing **user** index value means nothing of the sort. An object with no
`class_date` is not a signup for an infinitely distant past class; it is not a
signup at all. Fabricating a position for it in a business ordering would put
junk in every query result.

So: **user index queries are strictly sparse.** An object with no value for
the queried index does not appear, in either direction, and there is no
fallback read of `list_dir`. This makes user indexes *cheaper* than `mtime` —
`asc()` has no O(folder) penalty (`additional_index_plan.md` §9.3), because
there is no epoch tail to discover first.

`mtime` behaviour is unchanged. The two must not be unified.

## 6. Read API

### 6.1 `until()`, the one addition

```python
def until(self, ts) -> 'FolderQuery':
    """ Only include entries whose index value is <= ts (inclusive). """
```

`since()` alone cannot express a closed range, and every business-key query
is a closed range — one day, one week, one sprint. `until()` composes with
`since()` and compiles to:

- both bounds → `Key(name).between(lo, hi)` (verified, §8 #3)
- upper only → `Key(name).lte(hi)` (verified, §8 #4)
- Redis → the `max` argument of `ZRANGEBYSCORE` / `ZREVRANGEBYSCORE`
- scan backends → one more list comprehension filter

It is added to the shared `FolderQuery`/`ScanFolderQuery` and applies to
`mtime` too, where it usefully means "what changed *yesterday*" — a query
that is impossible today.

Both bounds are inclusive, matching `since()`'s existing contract. An
inverted range (`since(5).until(3)`) yields nothing rather than raising;
that is what the underlying `between` does, and an empty result is the
honest answer to an empty range.

### 6.2 The gym use case, end to end

```python
db.set(f'/signups/{student}', booking, index={'class_date': 20260905})

# who is booked into today's classes -- one indexed query
today = 20260905
for name, booking in db.folder('/signups').by('class_date') \
                       .since(today).until(today).items():
    ...

# tomorrow's roster, biggest classes first handled client-side
db.folder('/signups').by('class_date').since(20260906).until(20260906)

# and mtime still answers its own question, unchanged
db.recent('/signups', limit=10)     # who booked most recently
```

The two questions stop competing: `class_date` orders by when the class *is*,
`mtime` by when the booking *happened*.

### 6.3 `Entry.index_value`

`Entry` gains a fourth slot, `index_value` — the value the query ordered by.
For an `mtime` query it equals `mtime`, so nothing changes for existing
callers; `.key`, `.mtime` and `.ctime` keep their meanings and `__eq__` /
`__hash__` extend to include it.

A separate slot rather than reusing `.mtime` because a result set ordered by
`class_date` still carries a genuine `mtime`, and conflating them would make
`entries()` lie about when the object was written.

## 7. Wrappers

- **`UnionDB`** — `UnionFolderQuery` already merges per-db sorted generators
  with `heapq.merge`, front-db-wins (`kydb/union.py:99`). It merges on the
  ordering value, so it must merge on `index_value`, not `mtime`. Its
  hardcoded `by('mtime')` check (`kydb/union.py:127`) becomes a check that at
  least one member supports the requested index.
- **`CacheDB`** — delegates every folder query to `persist_db`
  (`kydb/cache.py:36`). Unchanged in shape; `set(index=...)` must forward the
  index values to `persist_db` (and only there — the cache db holds no index).

## 8. Verification

Checked against Moto 5.2.3 / boto3 1.43.88 before this plan was finalised
(`probe.py`, scratchpad). 13 assertions, all resolved:

| # | Assertion | Result |
| --- | --- | --- |
| 1 | Table creates with a third GSI on a user-named Number range key | pass |
| 2 | Sparse GSI excludes an object written with no index value | pass |
| 3 | `between(lo,hi)` serves the day-exact gym query | pass |
| 4 | `lte(hi)` works as `until()` | pass |
| 5 | `INCLUDE ['mtime','ctime']` carries both — no N+1 read | pass |
| 6 | The `contents` blob stays out of the index | pass |
| 7 | A rewrite with no `index=` preserves the existing value | pass |
| 8 | `REMOVE` clears the value and drops the row from the index | pass |
| 9 | `ScanIndexForward` orders by the user index both ways | pass |
| 10 | A missing GSI raises an error kydb can translate | pass (corrected) |
| 11 | The value reads back as `Decimal` — must cast to `int` | pass |
| 12 | A bounded Redis ZSET range serves the same day-exact query | pass |
| 13 | A date-as-`int` score is exact in a ZSET double | pass |

**Assertion 10 was mis-specified and is worth recording.** It predicted
`ValidationException`, from `additional_index_plan.md` §9.4. Moto actually
returns `ResourceNotFoundException` for a query against a GSI the table does
not have. This is not a design problem and needs no code change:
`_translate_missing_index` (`kydb/impl/dynamodb.py:152`) already accepts both
codes. Confirmed separately that the missing index's *name* appears in the
error message for a user index name too, which is what the existing
`TIME_INDEX not in str(err)` guard relies on — so the translation
generalises by parameterising the index name and message, nothing more.

As in the previous plan: Moto is an emulator, and every behaviour above is
documented DynamoDB semantics rather than an emulator quirk, so the whole
suite can run in CI under Moto with no real table required. The real-service
gaps identified in `additional_index_plan.md` §12 (GSI backfill timing, 1 MiB
page boundaries) are properties of GSIs in general and were already closed
there; a user index is the same construct with a different sort key and does
not reopen them.

## 9. Table specification (additive)

Unchanged from `additional_index_plan.md` §11, plus, **per user index**:

5. GSI `folder-<name>-index`: partition key `folder`, sort key `<name>`
   (Number), projection `INCLUDE` with `mtime` and `ctime`

Entirely opt-in. A table with no user index GSI keeps working exactly as it
does today; only `by('<name>')` fails, with the translated
`IndexNotSupported` naming the index to add. Writes never require the GSI —
`update_item` just sets an attribute — so an application can start recording
index values before the index exists, and the values are all there when it
is added.

`docsrc/source/implementations.rst`, `docsrc/source/recency.rst` and
`AGENTS.md` document the table requirements and must be updated together.

## 10. `mtime-index: false` gates `mtime` alone (resolved)

An earlier draft recorded this as a limitation. It is now fixed.

`folder()` used to check `mtime_index_enabled` eagerly and raise before
`by()` had been called — and `by()` is chained onto the object `folder()`
returns, so at that point the index name is not yet known. Switching the
backend's own recency stamp off therefore closed every business-key query
on that db as well. Writes were never affected: `set(index={...})`
recorded its values throughout, so such a db stored `class_date` values
correctly and then refused to query them back. The data was right and
unreachable.

The check now runs where the query runs — `FolderQuery._check_mtime_index_enabled`,
called from `ScanFolderQuery.entries`, `RedisFolderQuery.entries` and
`DynamoDBFolderQuery._raw_query` — and only when the index being queried
is `mtime`. The setting means "do not maintain *my* timestamp index"; a
business key is a value the caller supplied and stored on the object, not
a timestamp the backend stamped, so it is not the setting's to gate.

Three consequences worth stating:

- **The timing change is invisible to existing callers.** `recent()` was
  already lazy, so `list(db.recent(f))` raises exactly where it always
  did. The whole existing suite passes unmodified.
- **`reindex()` keeps its eager guard.** It *maintains* the mtime index
  rather than reading one, so there is no index name to wait for.
- **`UnionDB` decides per member.** `_db_supports_index` now asks each
  member whether its mtime index is enabled, so a member with it off is
  skipped — the same partial capability a union already applied to a
  member with no index at all. When *every* member has it off the union
  raises rather than returning an empty result, which would read as
  "nothing was written" rather than "nothing is recording timestamps".

One backend bug surfaced with it: `MemoryDB._folder_time_entries` walked
the `mtime` metadata dict to find candidate objects, so with the stamp
off it found nothing and a business-key query came back empty. Each
index is now its own row source — `mtime` reads the timestamp metadata, a
user index reads the index store — which is both correct and the cheaper
walk. Locked in by `kydb/tests/test_mtime_index_disabled.py`.

## 11. Out of scope for v1

Recorded so they are not mistaken for oversights:

- **String / float index values** (§4.2).
- **`db.set_index(key, {...})`** without rewriting the payload (§4.1).
- **Multi-attribute (composite) indexes** — `by('class_date', 'time')`.
  Expressible as one packed `int` today (`20260905_1830`).
- **Index-only projection queries** returning attributes without the payload.
- **Automatic GSI creation.** kydb never mutates table schema; the error
  message names what to add.

- **`UnionDB` resolves front-db-wins *after* each member applies the
  bounds.** A union merges the entries its members return, and each
  member filters on `since`/`until` itself. So a bounded query that
  excludes the front db's copy of a name lets a shadowed back-db copy
  through: with `/signups/anna` at `class_date=20260905` in the front db
  and `20260907` in the back, both the Saturday and the Monday roster
  list `anna`, even though `db['/signups/anna']` is unambiguously the
  front db's booking.

  The same hole is reachable without user indexes — `by('mtime').since()`
  on a union whose back db holds a *newer* copy — so it predates this
  feature.

  **Decision: not fixed; logged instead.** Closing it means resolving key
  ownership before filtering. Two costs were considered and both
  rejected as defaults:

  | Approach | Cost |
  | --- | --- |
  | Full folder listing per member, before filtering | O(folder) on every union query, including a `recent(limit=10)` that is currently cheap |
  | Existence check per surviving row against each db in front of it | Bounded by result size — but `BaseDB.exists_raw` is a full `get_raw` in a `try`/`except`, so it is a read per row, and an unbounded query converges on the listing anyway |

  Neither belongs on every union query to correct a result that is only
  wrong when two members disagree about the same key. An **unbounded**
  query is unaffected — every member returns every row, so front-db-wins
  is complete — which is exactly the shape `recent()` uses.

  So the query stays fast and reports the hazard.
  `UnionFolderQuery._log_index_use` emits one `logging` WARNING per
  `(folder, index)` on the `kydb.union` logger when both conditions for
  the bug hold: more than one member db, and at least one bound. Every
  union index query is logged at DEBUG. Warned once per shape rather
  than once per call, because databases are cached by URL in
  `kydb.api._db_cache` and outlive any one query — enough to be seen,
  not enough to bury a polling loop.

  kydb configures no handlers (`logging.getLogger(__name__)` only), so an
  application decides where this goes and can silence it deliberately
  with `logging.getLogger('kydb.union').setLevel(logging.ERROR)`.

  The limitation is asserted in `kydb/tests/test_user_index_wrappers.py`
  and `kydb/tests/test_union_shadowing_warning.py` — including the wrong
  answer itself, so that closing the hole later fails a test that must
  then be deleted on purpose. Documented for users in
  `docsrc/source/recency.rst`.
