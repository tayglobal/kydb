# Additional Index: Recency Queries on a Folder

Status: approved — all §2 decisions confirmed 2026-09-02
Scope: design for all backends, implementation targeted at DynamoDB first
Date: 2026-09-02

## 1. Problem

`kydb` can list a folder (`list_dir` / `ls`) but cannot answer "give me the
most recently added items in this folder". Two things block it:

1. **No ordering.** The DynamoDB `folder-index` GSI is `folder` HASH with no
   sort key (`kydb/impl/tests/conftest.py:64`,
   `docsrc/source/implementations.rst:25`). DynamoDB returns query results
   ordered by sort key; with no sort key the order is unspecified, so
   `list_dir_meta_folder` (`kydb/impl/dynamodb.py:39`) can only return an
   arbitrary page.
2. **No timestamp.** `folder_meta_set_raw` (`kydb/impl/dynamodb.py:24`)
   writes exactly `path`, `folder`, `contents`. Nothing records when an
   object was written.

## 2. Recommendation (summary)

| Decision | Choice |
| --- | --- |
| Storage | New sparse GSI `folder-time-index`: `folder` HASH, `mtime` RANGE, projection `INCLUDE ['ctime']` |
| Write path | `update_item` instead of `put_item`, setting `mtime` always and `ctime` via `if_not_exists` |
| Indexed attribute | `mtime` (modify time). `ctime` stored but not indexed |
| Directories | Not indexed — folder-meta records get no `mtime`, so the sparse index excludes them for free |
| API | Lazy query builder `db.folder(...)`, with `db.recent(...)` as sugar |
| Other backends | Explicit `IndexNotSupported`, with opt-in `allow_scan=True` client-side fallback |
| Migration | None required — rows with no `mtime` sort as an epoch tail; `db.reindex(folder)` optional |

## 3. Storage options considered

### A. Second GSI with a time sort key — **chosen**

`folder-time-index`: HASH `folder`, RANGE `mtime` (Number, `time.time_ns()`),
projection `INCLUDE` with non-key attribute `ctime`.

```python
res = self.table.query(
    IndexName='folder-time-index',
    KeyConditionExpression=Key('folder').eq(folder),
    ScanIndexForward=False,   # newest first
    Limit=n)
```

One query, ordering done server-side, native pagination via
`LastEvaluatedKey`, no extra writes per object.

`KEYS_ONLY` matters. The existing `folder-index` uses `ALL`, so every pickled
payload is duplicated into the index even though the only field ever read is
`item['path']`. Do not repeat that here. (Changing `folder-index` to
`KEYS_ONLY` is a separate, worthwhile cleanup — it requires dropping and
recreating that index, so it is listed as optional.)

Why a *new* index rather than changing the existing one: GSI key schemas are
immutable, so a sort key cannot be retrofitted onto `folder-index`. An LSI is
also impossible — the table has no range key and LSIs can only be defined at
table creation.

### B. Reuse `folder-index` and sort client-side — rejected as the primary path

Zero schema change, but it reads the whole folder on every call and makes
`limit` meaningless. Retained only as the opt-in fallback for backends with no
server-side ordering (see §7).

### C. Maintain an explicit per-folder index object — rejected for DynamoDB

A `.index-mtime-<folder>` key holding a sorted list. Portable to every
backend, but it turns every write into a non-atomic read-modify-write on a
hot key, and the object grows without bound. Worth noting that this *is* the
right shape for Redis, where it is a native sorted set (`ZADD` /
`ZREVRANGE`) with none of those problems.

### D. Timestamp encoded in `path` — rejected

Breaks key-as-identity: the caller could no longer address an object by the
key they wrote.

## 4. Timestamp semantics

"Most recently added" is ambiguous between create time and modify time, and
the current `put_item` overwrites the whole item, so a naive `ctime` would be
clobbered on every rewrite. `update_item` gives both in a single write with no
read-before-write:

```python
self.table.update_item(
    Key={'path': key},
    UpdateExpression=('SET folder=:f, contents=:c, mtime=:t, '
                      'ctime=if_not_exists(ctime, :t)'),
    ExpressionAttributeValues={':f': folder, ':c': value, ':t': now_ns})
```

Sorting by both would need two GSIs. Index `mtime` and store `ctime` as plain
data: "recently touched" is what a source/config DB is actually asked for, and
`ctime` remains available on the item for display or filtering.

Two properties to document rather than engineer around:

- **Clock skew.** `time.time_ns()` is client-side. DynamoDB has no
  server-side timestamp function, so two writers with drifting clocks can
  produce a slightly wrong relative order. Acceptable for this use case; state
  it in the docs instead of implying a total order.
- **Ties.** Two writes in the same nanosecond sort arbitrarily against each
  other. GSI sort keys need not be unique, and `LastEvaluatedKey` on a GSI
  query carries the table key (`path`) alongside the index keys, so paging
  through a run of identical timestamps neither drops nor duplicates items.
  Verified — see §12.

## 5. Directories are excluded, deliberately

`FolderMetaMixin.set_raw` (`kydb/folder_meta.py:47`) routes both real objects
and `.folder-*` marker records through `folder_meta_set_raw`. If `mtime` is
simply not written for marker records, the GSI is sparse and directories drop
out of the recency index automatically — no filtering code, smaller index.

The consequence is that `include_dir` cannot be honoured for recency queries.
That is the correct semantic: "most recently added items" means objects.
Recency queries should therefore not accept an `include_dir` argument at all,
rather than accepting one and ignoring it.

## 6. API

### Chosen: a lazy query builder, plus one sugar method

```python
db.recent('/my/folder', limit=10)                        # names, newest first

db.folder('/my/folder').by('mtime').desc().limit(10)     # names
db.folder('/my/folder').by('mtime').since(ts).items()    # (name, value) pairs
db.folder('/my/folder').by('mtime').desc().entries()     # .key, .mtime, .ctime
```

`db.recent(folder, limit=n)` is defined as
`db.folder(folder).by('mtime').desc().limit(n)`.

Rationale:

- Each clause maps 1:1 onto a DynamoDB query parameter — `by` to `IndexName`,
  `desc` to `ScanIndexForward`, `since` to `KeyConditionExpression`, `limit`
  to `Limit`. Nothing in the API promises something the backend cannot do
  cheaply.
- It stays a generator, matching `list_dir`, so paging is lazy and a
  `limit()` never fetches more than it needs.
- It leaves room to grow. A future non-time index is `by('status')` — the
  same API, no new method. This is why the feature is "additional index"
  rather than "recency": time is the first index, not a special case.
- `db.recent()` keeps the common case to one obvious call for users who never
  need the rest.

### Rejected: overloading `list_dir`

`db.list_dir(folder, order_by='mtime', reverse=True, limit=10)` keeps one
entry point, but bloats the signature and, since most backends cannot honour
the arguments, produces silent behavioural differences between backends.

### Rejected: a bare `db.recent()` and nothing else

Simple, but too narrow — no "since yesterday", no way to get the timestamps
back, and no path to non-time indexes without adding another method.

### Return types

`limit()` / iteration yields **names** (`str`), consistent with `list_dir`.
`items()` yields `(name, value)`. `entries()` yields a small object exposing
`.key`, `.mtime`, `.ctime`.

Note that boto3's resource API returns Number attributes as `decimal.Decimal`,
so `entries()` must cast `mtime` / `ctime` back to `int` rather than leaking a
`Decimal` into user code. The cast is lossless at nanosecond magnitude
(verified, §12). Names are the default because that is what
`list_dir` already returns. The index projects `ctime` alongside the keys, so
`entries()` is also served from the index page with no extra read; only
`items()`, which returns the stored value, costs a read per row.

## 7. Other backends

| Backend | Support |
| --- | --- |
| DynamoDB | Native, via `folder-time-index` |
| Redis | Natural fit: per-folder sorted set, `ZADD` on write, `ZREVRANGE` on read. See the score-precision note below |
| Memory | Sort in Python — everything is already in a dict |
| Files | Sort by `st_mtime` from the filesystem |
| S3 | `list_objects_v2` returns `LastModified` but offers no server-side sort — scan and sort |
| HTTP/HTTPS | Not supported |

Backends without server-side ordering must raise a clear `IndexNotSupported`
by default rather than quietly degrading into a full scan. An opt-in
`allow_scan=True` enables the option-B client-side sort for memory, files and
S3, so the cost is always the caller's explicit choice.

### Redis: the sorted-set score cannot hold a nanosecond timestamp

A Redis ZSET score is an IEEE-754 double, exact only to `2**53`. A
`time.time_ns()` value is ~19 digits, roughly 200x beyond that, so storing one
as a score silently rounds it to about 256ns.

Rounding is harmless for *ordering* — writes are milliseconds apart in
practice — but not for the value handed back to callers. The score is
therefore used only to order the folder, while the exact nanosecond value
lives in a companion hash and is what `entries()` returns. This keeps `mtime`
in the same units (nanoseconds) on every backend; a caller comparing
`entry.mtime` across two databases must not get milliseconds from one and
nanoseconds from another.

Locked in by `test_recent_mtime_is_exact_nanoseconds`, which brackets the
value between two `time.time_ns()` calls around the write.

## 8. Wrapper gotchas

Both wrappers need their own implementation; neither inherits correct
behaviour.

- **`UnionDB.list_dir` collects into a `set`** (`kydb/union.py:114`), which
  destroys any ordering. A recency query across a union must merge the
  per-db sorted generators — `heapq.merge` over them, dedup'ing by name with
  front-db-wins, matching the existing union read semantics.
- **`CacheDB.list_dir` already delegates to `persist_db`**
  (`kydb/cache.py:32`). Recency queries must do the same: the cache db holds
  only what has been read, so it cannot answer a folder-wide question.

## 9. Backward compatibility with existing databases

Superseded decision: an earlier draft of this plan required a
`db.reindex(folder)` backfill before recency queries were meaningful.
Reindex is now **optional**. Upgrading kydb must not break an existing
database, and must not require a data migration to do so.

### 9.1 Which backends are affected

| Backend | Legacy objects | Migration |
| --- | --- | --- |
| Files | `st_mtime_ns` comes from the filesystem | None — real timestamps already |
| S3 | `LastModified` comes from S3 | None — real timestamps already |
| Memory | Process-local; no legacy data exists | None |
| DynamoDB | No `mtime` attribute on pre-existing rows | Epoch tail (§9.3) |
| Redis | Not a member of `<folder>:mtime-index` | Epoch tail (§9.3) |
| HTTP/HTTPS | Unsupported either way | n/a |

Only the two backends where kydb maintains the index itself are affected.
Files and S3 derive `mtime` from the substrate, so pre-existing objects
already sort correctly with their true timestamps.

### 9.2 Why epoch, and not a reindex

Neither DynamoDB nor Redis retains any record of when a pre-existing
object was written, so `reindex` has no true timestamp to recover — it
can only stamp every legacy row with `now`. That places every ancient
object at the *top* of `recent()`, ahead of genuinely recent writes,
and does so most severely for the users with the most existing data.

Treating a missing timestamp as epoch (`mtime == ctime == 0`) asserts
only what is actually known: this object is older than anything the
index has tracked. It sorts correctly under `desc()` and is excluded by
any `since(ts)` with `ts > 0`.

The write path is self-healing. `ctime=if_not_exists(ctime, :t)`
(`kydb/impl/dynamodb.py`) and `HSETNX` (`kydb/impl/redis.py`) mean a
legacy object leaves the epoch tail the first time it is rewritten, with
`ctime` correctly recorded at that first touch. A database converges on
a fully indexed state through normal use, with no migration step.

`db.reindex(folder)` therefore remains available but is demoted to an
optional tool for callers who would rather have all legacy rows claim
migration-day than sort as epoch. It is not a prerequisite for the
feature.

### 9.3 The epoch tail

A missing timestamp cannot be defaulted at read time alone: the
DynamoDB GSI is deliberately sparse (§5), so a row with no `mtime` is
absent from `folder-time-index` entirely, and a legacy Redis object was
never `ZADD`ed to the sorted set. Neither is visible to the index query,
so a second source of names is required.

That source already exists on both backends and is exactly what
`list_dir` reads — the `folder-index` GSI, and the Redis folder hash.

Every real `mtime` is `> 0`, so legacy rows sort after every indexed row
under `desc()`. That gives an ordering where the fallback is only ever
reached at the end:

- **`desc()` — the `recent()` path.** Page `folder-time-index` (or
  `ZREVRANGEBYSCORE`) as normal. Only when that index is **exhausted**,
  read `list_dir`, subtract the names already yielded, and emit the
  remainder as `mtime = ctime = 0`. `db.recent(folder, limit=10)` on a
  folder holding at least 10 indexed objects never touches the fallback
  at all, so the common query costs nothing extra.
- **Dedup is free and sound.** The fallback is unreachable until the
  whole index has been consumed, so the set of already-yielded names is
  necessarily complete at that point. No extra reads are needed to build
  it. The memory cost is one name-set per folder, paid only by a caller
  who was already iterating the entire folder.
- **`since(ts)` with `ts > 0`** excludes every epoch row by definition —
  skip the fallback entirely.
- **`asc()` pays the cost.** Legacy rows come first, so `list_dir` must
  be read before the first result can be yielded, and `asc().limit(n)`
  degrades to O(folder). Documented rather than engineered around;
  `asc()` is the rarer query, and the cost disappears as the folder
  converges on being fully indexed.

`ctime` already falls back to `mtime` when absent, on both backends
(`DynamoDBFolderQuery._raw_query`, `RedisFolderQuery.entries`), so an
epoch row reports `ctime == 0` with no further change.

### 9.4 The missing GSI is a separate break

Independent of any data question: on an existing DynamoDB table that has
no `folder-time-index`, a recency query raises a raw boto3
`ValidationException` rather than a kydb exception. The write path is
unaffected (`update_item` requires no index), as are `list_dir`, `get`,
`set` and `delete` — so existing code keeps working and only the new API
fails, but it fails opaquely.

Two changes close this:

1. Translate that `ValidationException` into `IndexNotSupported`, naming
   the table and the GSI that needs adding.
2. Make `allow_scan=True` real on DynamoDB, where it is currently a
   documented no-op (`DynamoDB.folder`): scan `folder-index`, read
   `mtime` per item, epoch where absent. The library then keeps working
   against an unmigrated table with no schema change at all, at a cost
   the caller explicitly opted into.

### 9.5 Redis key-namespace collision

`RedisDB._mtime_key` derives `<folder>:mtime-index` (likewise
`:mtime-values`, `:ctime-index`). An existing database that already
holds an object at one of those paths will fail its next write with
`WRONGTYPE` when `zadd` hits a string key. Unlikely, but unlike
everything else in this section it is a genuine break of an existing
write path rather than a degraded query. Detect and raise a clear error,
or move the index keys to a prefix that cannot collide with an object
path.

## 10. Operational note

The GSI partition key is `folder`, so all index writes for one folder land on
a single index partition (~1000 WCU/s). Only relevant for a very hot folder;
mitigation if it ever matters is sharding the index key as `folder#<n>` and
fanning the query across shards. Not worth building now — noted so the
constraint is known.

## 11. Table specification (updated)

The DynamoDB table must have:

1. `path` (String) as partition key
2. `folder` (String) attribute
3. GSI `folder-index`: partition key `folder` — existing, used by `list_dir`
4. GSI `folder-time-index`: partition key `folder`, sort key `mtime`
   (Number), projection `INCLUDE` with non-key attribute `ctime` — new,
   used by recency queries

Items 1–3 are unchanged, so an existing table keeps working on upgrade
without any schema change: the write path and `list_dir` do not touch
`folder-time-index`. Item 4 is required only to serve recency queries;
without it they raise `IndexNotSupported` (§9.4), or fall back to a scan
under `allow_scan=True`.

`docsrc/source/implementations.rst` and `AGENTS.md` both document the table
requirements and must be updated together.

## 12. Verification

Every assumption above that could be checked mechanically was checked against
Moto 5.2.3 / boto3 1.43.86 before this plan was finalised. All 14 assertions
passed:

| # | Assertion | Result |
| --- | --- | --- |
| 1 | Table creates with a composite GSI using a Number sort key | pass |
| 2 | Binary `contents` round-trips through `update_item` (`['contents'].value`) | pass |
| 3 | Sparse GSI excludes the folder-meta record written without `mtime` | pass |
| 4 | `ScanIndexForward=False` returns newest-first | pass |
| 5 | `KEYS_ONLY` returns `path` + `folder` + `mtime`, and no `contents` | pass, but incomplete — see below |
| 6 | `Limit` is honoured and `LastEvaluatedKey` pages correctly | pass |
| 7 | `since(ts)` works as `Key('mtime').gte(ts)` | pass |
| 8 | Rewrite preserves `ctime`, bumps `mtime`, and reorders the index | pass |
| 9 | Deleting an item removes its index entry | pass |
| 10 | The existing `folder-index` is unaffected | pass |
| 11 | A legacy row without `mtime` is invisible, and a reindex makes it visible | pass |

Two secondary probes settled details that affect the implementation:

- **`mtime` reads back as `decimal.Decimal`**, not `int`. `Decimal == int`
  compares true and `int(...)` is lossless at 19 digits, but the query API
  must cast explicitly so callers never see a `Decimal`.
- **Tie pagination is stable.** Five items sharing one `mtime`, paged two at
  a time, returned all five across three pages with no duplicates and no
  drops. `LastEvaluatedKey` on the GSI query was confirmed to contain
  `{folder, mtime, path}` — the table key is what makes this safe.

### Correction found during implementation

Assertion 5 was checked but under-specified, and the plan was wrong as a
result. `KEYS_ONLY` projects `path` + `folder` + `mtime` and **not `ctime`** —
the original probe confirmed which attributes were present without noticing
that `ctime`, which `entries()` is specified to expose, was not among them.
Serving `.ctime` from a `KEYS_ONLY` index therefore costs one extra read per
row, making `entries()` N+1 in the size of the result.

Fixed by changing the index projection to `INCLUDE` with non-key attribute
`ctime`:

```
KEYS_ONLY       -> ['folder', 'mtime', 'path']            ctime absent
INCLUDE[ctime]  -> ['ctime', 'folder', 'mtime', 'path']   ctime present
```

This costs a few bytes per index entry, still keeps the `contents` blob out
of the index (the reason `KEYS_ONLY` was chosen), and removes the extra read
entirely. Locked in by `test_dynamodb_entries_does_not_refetch_per_item`,
which asserts one index query and zero per-item reads.

`items()` keeps one extra read per row by necessity — it returns the stored
value, which must not be projected into the index.

### What this does and does not establish

Moto is an emulator, so this confirms the design is expressible and that the
API contract is the one being relied on — it is not proof of real-service
behaviour. Every verified behaviour above is documented DynamoDB semantics
rather than an emulator quirk, so the risk is low. The two points that a Moto
run genuinely cannot exercise are real GSI backfill timing when the index is
added to a table that already holds data, and pagination across real
1MB page boundaries. Both are worth one confirming run against a real table
using the procedure in `AGENTS.md` before release, but neither blocks
implementation.

The practical consequence: **the full test suite in §8 of the TODO can run in
CI under Moto.** No part of it requires a real DynamoDB table.

## 13. Making the index optional

The read side is already optional: `folder()` / `recent()` are opt-in
API and `list_dir` never touches `folder-time-index`, so a caller who
ignores the feature pays nothing on reads.

The **write** side is unconditional, and that is the real cost. Both
`DynamoDB.folder_meta_set_raw` and `RedisDB.folder_meta_set_raw`
maintain the index on every write with no way to opt out.

### 13.1 What it actually costs

| Backend | Cost of leaving it on |
| --- | --- |
| DynamoDB, no GSI | Two extra attributes per row. Negligible |
| DynamoDB, GSI present | One extra index write per object write |
| Redis | Write went from 2 round trips to 5; delete likewise |
| Memory | One dict entry per object |
| Files, S3 | Nothing — `mtime` comes from the substrate, never written |

Redis is the outlier and the only one worth engineering around.
`folder_meta_set_raw` issues `hset`, `set`, `zadd`, `hset`, `hsetnx`
(`kydb/impl/redis.py`) and `delete_raw` issues `delete`, `hdel`, `zrem`,
`hdel`, `hdel` — five sequential round trips each where there were two.

### 13.2 Pipeline Redis first

Both sequences are unconditional, with no intermediate reads, so a
pipeline collapses each to a single round trip — better than the two
they started from. Do this before adding any switch: it removes most of
the reason to want one.

### 13.3 The switch: per-db config

Where it goes: the config mechanism that already exists and is already
per-db — `KYDB_CONFIG_PATH` → `config['dbs'][db_name]`, read by
`BaseDB._get_config`, already used by Redis for host/port/password.

```yaml
dbs:
  my-table:
    mtime-index: false
```

Default is `true`. When the key is absent — including when no
`KYDB_CONFIG_PATH` is set at all — the index is maintained, so the
feature stays discoverable and nothing needs configuring to work.

Rejected: a URL parameter (`dynamodb://table/path?index=off`).
`_get_name_and_basepath` splits the URL on `/` and takes `parts[2]` as
the db name, so a query string lands in `base_path`; and `_db_cache`
keys on the whole URL, so an operational tuning knob would be baked into
an identity string that gets copied into every caller.

Also rejected for now: a `connect()` kwarg, which would have to change
the `_db_cache` key and puts the setting at the call site rather than
with the deployment.

### 13.4 Behaviour when disabled

- Writes skip `mtime` / `ctime` entirely — no attributes on DynamoDB, no
  `zadd` / `hset` / `hsetnx` on Redis, no `__meta` entry on Memory.
- `folder()` / `recent()` raise `IndexNotSupported`, saying the index is
  disabled for this db. They must **not** return every object at epoch
  in arbitrary order, which is technically consistent with §9 but
  useless.
- `allow_scan=True` does **not** rescue it, on DynamoDB, Redis or
  Memory. `allow_scan` opts into a missing *ordering*, not missing
  *data*: with the index off nothing is recording a timestamp, so a
  client-side sort has nothing to sort by. Raising is the honest answer;
  returning an arbitrary order would not be.
- Files and S3 ignore the setting entirely: their `mtime` is read from
  the substrate and is never maintained by kydb, so there is nothing to
  disable and nothing to lose.

One coupling this exposed, fixed alongside:
`RedisDB._get_connection_kwargs` treated the presence of *any* per-db
config block as meaning it carries host/port/password. A block that
exists only to set `mtime-index` must not force a connection block to be
written too, so the connection details are now taken from the config
only when `host` is actually present.

### 13.5 Why this composes with the epoch tail

Toggling the flag is only safe *because* of §9. Objects written while
the index was off carry no timestamp, so on re-enabling they land in the
epoch tail, sort last, and heal into place the first time they are
rewritten — the same mechanism, in both directions, with no migration.

Without the epoch tail, turning the index off and back on again would
silently drop every object written in between from all recency queries,
with nothing to indicate it had happened.
