# Additional Index — TODO

Implementation checklist for `additional_index_plan.md`.
Order matters: schema and write path first, then read path, then the wrappers,
then the other backends.

## 0. Decisions — CONFIRMED 2026-09-02

- [x] **Index `mtime`** (modify time); `ctime` stored via `if_not_exists` but
      not indexed. A re-write resurfaces an object.
- [x] **Directories excluded** from recency results via the sparse index —
      no `mtime` written on `.folder-*` records. Recency queries take no
      `include_dir` argument.
- [x] **Attribute names `mtime` / `ctime`**, matching the terse existing
      `path` / `folder` / `contents` vocabulary. GSI named
      `folder-time-index`.
- [x] **`IndexNotSupported(KydbException)`** in `kydb/exceptions.py`, raised
      by default on backends without server-side ordering; `allow_scan=True`
      opts into the client-side scan-and-sort.

## 1. Schema

- [x] Add GSI `folder-time-index` to the test table in
      `kydb/impl/tests/conftest.py:54` — `folder` HASH, `mtime` RANGE
      (Number), projection `KEYS_ONLY`; add `mtime` to `AttributeDefinitions`
- [x] ~~Confirm Moto supports a sparse GSI with a Number sort key and
      honours `ScanIndexForward`~~ — **verified** against Moto 5.2.3 /
      boto3 1.43.86, 14/14 assertions passed. The whole suite in §8 can run
      in CI; no real DynamoDB table is required. See §12 of
      `additional_index_plan.md`
- [x] Update table spec in `docsrc/source/implementations.rst:25`
- [ ] Update table spec in `AGENTS.md` (the "The table must have:" list)
      — out of scope for this stage: `AGENTS.md` has pre-existing
      uncommitted changes from another session that must not be touched;
      left for a later stage/session to update alongside those changes

## 2. DynamoDB write path

- [x] Replace `put_item` with `update_item` in
      `DynamoDB.folder_meta_set_raw` (`kydb/impl/dynamodb.py:24`):
      `SET folder=:f, contents=:c, mtime=:t, ctime=if_not_exists(ctime, :t)`
- [x] Skip `mtime` / `ctime` for folder-meta records so the index stays
      sparse — detect via `FolderMetaMixin._is_folder_meta` on the last path
      component
- [x] Verify `contents` still round-trips as Binary through `update_item`
      (`get_raw` reads `items[0]['contents'].value` — `kydb/impl/dynamodb.py:19`)
- [x] Confirm delete needs no change (removing the item removes its index
      entry automatically)

## 3. Query API

- [x] Add `IndexNotSupported` to `kydb/exceptions.py`
- [x] Add the query builder — `db.folder(path)` returning a lazy object with
      `.by(name)`, `.desc()`, `.asc()`, `.since(ts)`, `.limit(n)`
      (`kydb/query.py:FolderQuery`)
- [x] Iteration and `.limit()` yield names (`str`); add `.items()` →
      `(name, value)` and `.entries()` → object with `.key`, `.mtime`,
      `.ctime`
- [x] Index projection is `INCLUDE ['ctime']`, NOT `KEYS_ONLY` — `ctime` is
      absent from a KEYS_ONLY page, which would otherwise make `.entries()`
      N+1 in the size of the result
- [x] Cast `mtime` / `ctime` from `decimal.Decimal` to `int` in `.entries()`
      — boto3's resource API returns Number attributes as `Decimal`; the cast
      is lossless at nanosecond magnitude (verified)
- [x] Add `db.recent(folder, limit=n)` as sugar for
      `db.folder(folder).by('mtime').desc().limit(n)`
- [x] Add both to `KYDBInterface` (`kydb/interface.py`) with docstrings, and
      a `NotImplementedError`/`IndexNotSupported` default in `BaseDB`
      — note: `KYDBInterface`'s own default also raises `IndexNotSupported`
      (not `NotImplementedError`) directly, so wrapper classes that don't
      subclass `BaseDB` (`UnionDB`, `CacheDB`) inherit the correct
      `IndexNotSupported` behaviour for free ahead of stage 5
- [x] No `include_dir` argument on recency queries — directories are not
      indexed

## 4. DynamoDB read path

- [x] Implement the backend query: `IndexName='folder-time-index'`,
      `KeyConditionExpression=Key('folder').eq(folder)`,
      `ScanIndexForward=False`, `Limit=n`
      (`kydb/impl/dynamodb.py:DynamoDBFolderQuery._raw_query`)
- [x] Honour `since(ts)` by adding `Key('mtime').gte(ts)` to the condition
      — inclusive boundary, tested
- [x] Page with `LastEvaluatedKey` and stay lazy — a `limit(n)` must not
      fetch more pages than needed
- [x] Apply `_get_full_path` / `_ensure_slashes` consistently, and check
      behaviour with a non-empty `base_path` (both `BASE_PATHS` cases in
      `test_impl.py`)

## 5. Wrappers

- [x] `UnionDB`: implement recency separately — `heapq.merge` over the
      per-db sorted generators, dedup by name, front-db wins.
      `UnionDB.list_dir` (`kydb/union.py:114`) uses a `set` and must not be
      reused
- [x] `CacheDB`: delegate recency to `persist_db`, matching
      `CacheDB.list_dir` (`kydb/cache.py:32`)

## 6. Other backends

- [x] Raise `IndexNotSupported` by default in `BaseDB`
- [x] Add opt-in `allow_scan=True` client-side sort fallback
- [x] Memory: sort the in-memory dict
- [x] Files: sort by `st_mtime`
- [x] S3: sort by `LastModified` from `list_objects_v2` (scan-and-sort only)
- [x] Redis: native sorted set per folder — `ZADD` on write, `ZREVRANGE` on
      read; also remove the member on delete (`RedisDB.delete_raw`,
      `kydb/impl/redis.py:88`)
- [x] HTTP/HTTPS: leave unsupported

- [x] Redis: exact ns kept in a companion hash; the ZSET score (a double,
      exact only to 2**53) orders the folder but is never the value
      returned — keeps `mtime` in nanoseconds on every backend

## 7. Backward compatibility with existing databases

Supersedes the original "Migration" stage. Reindex is no longer a
prerequisite — see §9 of `additional_index_plan.md`. Files, S3 and Memory
need nothing here; only DynamoDB and Redis maintain their own index.

### 7a. Epoch tail (required)

- [x] DynamoDB: when `folder-time-index` is exhausted under `desc()`, fall
      back to `list_dir` (the `folder-index` GSI), subtract the names
      already yielded, and emit the remainder as `mtime = ctime = 0`
      (`DynamoDBFolderQuery._raw_query`)
- [x] Redis: same, falling back to the folder hash after the sorted set is
      exhausted (`RedisFolderQuery.entries`)
- [x] Skip the fallback entirely when `since(ts)` is set with `ts > 0` —
      no epoch row can satisfy it
- [x] `asc()`: read the fallback names *before* the index, since epoch rows
      sort first. Accept that `asc().limit(n)` is O(folder) and document it
- [x] Build the already-yielded name set from the index pages as they are
      consumed — the fallback is unreachable until the index is exhausted,
      so no extra reads are needed to make the set complete

### 7b. Missing-GSI handling (required)

- [x] Translate boto3's `ValidationException` for a missing
      `folder-time-index` into `IndexNotSupported`, naming the table and
      the GSI to add
- [x] Make `allow_scan=True` real on DynamoDB — currently a documented
      no-op in `DynamoDB.folder`. Scan `folder-index`, read `mtime` per
      item, epoch where absent, so an unmigrated table works with no schema
      change at all

### 7c. Redis key collision (required)

- [x] `_mtime_key` / `_mtime_val_key` / `_ctime_key` derive
      `<folder>:mtime-index` etc. An existing object stored at one of those
      paths makes the next write fail with `WRONGTYPE` on `zadd`. Either
      detect and raise a clear error, or move the index keys to a prefix
      that cannot collide with an object path

### 7d. Reindex (optional, demoted)

- [ ] Implement `db.reindex(folder)` — scan the folder and `update_item`
      the missing `mtime` / `ctime`. No longer required for correctness:
      it stamps legacy rows with `now`, which sorts them *ahead* of
      genuinely recent writes. Offer it as a deliberate choice, not a
      migration step

## 8. Tests

- [x] Ordering: write objects in a known order, assert newest-first
- [x] `limit` returns exactly n and stops paging early (also asserted via a
      monkeypatched `table.query` call-count, confirming no over-fetch)
- [x] `since(ts)` boundary — inclusive vs exclusive, pick and test it
      (**inclusive**, `Key('mtime').gte(ts)`)
- [x] Paging across a run of identical `mtime` values drops nothing and
      duplicates nothing (verified as DynamoDB behaviour; lock it in as a
      regression test) — done via a synthetic multi-page `table.query`
      fake, since real Moto keeps 5 tiny items on one page
- [x] `.entries()` returns `int` timestamps, not `Decimal`
- [x] Rewriting an object bumps `mtime` but preserves `ctime` — write-path
      case already covered in stage 1; stage 2 adds the read-path version
      (rewrite reorders `recent()` results)
- [x] Directories never appear in recency results
- [x] Delete removes the item from recency results
- [x] Works with and without `base_path`
- [x] Union merge order is correct across two dbs — stage 3 (wrappers):
      `test_union_recent_merge_order_across_two_dbs` and
      `test_union_recent_dedup_front_db_wins` (the latter specifically
      catches the "keep whichever entry the merge saw first" trap:
      front-db-wins must not depend on which db's write is newer)
- [x] `IndexNotSupported` raised on unsupported backends unless
      `allow_scan=True` — now also covering the `allow_scan=True` opt-in
      fallback itself (memory/files/s3), Redis's native no-raise case,
      and HTTP/HTTPS staying unsupported even with `allow_scan=True`
      (stage 3)
- [x] Legacy row (no `mtime`) appears at the tail of `desc()` results,
      with `mtime == ctime == 0` — DynamoDB and Redis
- [x] Legacy row is absent from `since(ts)` for any `ts > 0`, and the
      fallback is not read at all in that case
- [x] `desc().limit(n)` issues no `list_dir` / fallback read when the index
      already covers `n` rows (assert via call-count, as the existing
      over-fetch test does)
- [x] Rewriting a legacy object promotes it out of the epoch tail and sets
      `ctime` at that first touch
- [x] `asc()` returns legacy rows first, before any indexed row
- [x] Missing `folder-time-index` raises `IndexNotSupported`, not a raw
      boto3 `ValidationException`; `allow_scan=True` serves the query
      anyway
- [x] Redis write against a folder colliding with an index key name fails
      with a clear kydb error, not `WRONGTYPE`
- [ ] `reindex` makes pre-existing rows carry a real `mtime` — stage 7d
      (optional), not in scope here

## 9. Docs

- [ ] Document the query API and `recent()` — likely a new
      `docsrc/source` page, linked from `index.rst`
- [ ] Document the clock-skew caveat (client-side timestamps, no total order
      across writers with drifting clocks)
- [x] Document the epoch tail: objects written before the index existed
      report `mtime == ctime == 0`, appear at the end of `desc()` order,
      are excluded by any `since(ts > 0)`, and move into place the first
      time they are rewritten
- [x] Document that `reindex` is optional, and why it is usually the worse
      choice (it claims migration-day as the mtime of every legacy object)
- [x] Document that an existing DynamoDB table keeps working on upgrade;
      `folder-time-index` is needed only for recency queries

## 10. Pre-release confirmation on a real table

Moto cannot exercise these two; everything else is covered in CI.

- [ ] GSI backfill timing when `folder-time-index` is added to a table that
      already contains data
- [ ] Pagination across real 1MB page boundaries
- [ ] Use the procedure in `AGENTS.md` (disposable `kydb-real-tests-YYYYMMDD`
      table, deleted afterwards)

## 11. Optional follow-ups

- [ ] Change `folder-index` projection from `ALL` to `KEYS_ONLY` — only
      `item['path']` is ever read (`kydb/impl/dynamodb.py:52`), so the `ALL`
      projection duplicates every pickled payload into the index. Requires
      dropping and recreating the index.
      **No longer free**: `DynamoDBScanFolderQuery` (the `allow_scan=True`
      fallback for a table with no `folder-time-index`) now reads
      `mtime`/`ctime` straight off the `folder-index` page, which only
      works because the projection is `ALL`. Under `KEYS_ONLY` that
      fallback would see no timestamps and report every object at epoch.
      Either add `mtime`/`ctime` as `INCLUDE` non-key attributes when
      narrowing the projection, or accept a per-item read in the scan
      fallback
- [ ] Shard the index partition key as `folder#<n>` if a single folder ever
      exceeds ~1000 writes/s
- [ ] Generalise `by()` to user-defined non-time indexes

## 12. Making the index optional

See §13 of `additional_index_plan.md`. Order matters: pipeline Redis
first, since it removes most of the motivation for the switch.

Implemented 2026-09-02. New tests live in
`kydb/impl/tests/test_index_compat.py`; 263 pass, and flake8 reports the
same 24 pre-existing warnings as before the change.

### 12a. Pipeline Redis (do first, independent of the flag)

- [x] `RedisDB.folder_meta_set_raw`: collapse `hset` / `set` / `zadd` /
      `hset` / `hsetnx` into one pipeline — 5 round trips to 1
- [x] `RedisDB.delete_raw`: same for `delete` / `hdel` / `zrem` / `hdel` /
      `hdel`
- [x] Test asserts a single round trip (mock/patch the connection, count
      `execute` calls), and that behaviour is otherwise unchanged

### 12b. The `mtime-index` config flag

- [x] Read `mtime-index` from the per-db config in `BaseDB` (alongside
      `_get_config`), defaulting to `True` when the key or the whole
      config file is absent — `BaseDB.mtime_index_enabled`
- [x] Incidental fix this exposed: `RedisDB._get_connection_kwargs`
      treated *any* per-db config block as carrying host/port/password,
      so a block setting only `mtime-index` forced a connection block to
      be written too. Connection details are now read from the config
      only when `host` is present
- [x] DynamoDB: skip `mtime` / `ctime` in `folder_meta_set_raw` when off
- [x] Redis: skip `zadd` / `hset` / `hsetnx` when off
- [x] Memory: skip the `__meta` entry when off
- [x] Files / S3: ignore the setting — `mtime` comes from the substrate
      and costs nothing to maintain
- [x] `folder()` / `recent()` raise `IndexNotSupported` naming the db and
      saying the index is disabled by config — never return every object
      at epoch
- [x] ~~`allow_scan=True` continues to work when the index is disabled~~
      — **corrected during implementation**: it must NOT. `allow_scan`
      opts into a missing *ordering*, not missing *data*; with the index
      off nothing records a timestamp, so DynamoDB/Redis/Memory raise
      regardless. Plan §13.4 updated to match

### 12c. Tests

- [x] Default (no config file, no key) maintains the index — the existing
      suite covers this, assert it explicitly so a default flip is caught
- [x] `mtime-index: false` writes no `mtime` / `ctime` (DynamoDB), adds no
      sorted-set member (Redis), records no `__meta` (Memory)
- [x] `folder()` / `recent()` raise `IndexNotSupported` when disabled, and
      the message says so
- [x] `allow_scan=True` is refused too when the index is disabled
      (see the correction in 12b)
- [x] Toggle round trip: write with the index off, turn it on, and assert
      those objects appear in the epoch tail (§7a) and leave it on rewrite
