# User-Supplied Index — TODO

Implementation checklist for `user_index_plan.md`.

Order matters. Stage 1 defines the shared contract every backend implements
against and must land first. Stages 2a and 2b are file-disjoint and run in
parallel. Stage 3 integrates.

## Standard test command

From the repo root, per `AGENTS.md`:

```bash
PYTHONPATH=. python -m pytest kydb -q
```

Baseline before this work: **268 passed, 1 skipped**. No existing test may
change behaviour — this feature is purely additive. If an existing test needs
editing, that is a signal the design was misread; stop and say so.

## 0. Decisions — CONFIRMED, do not relitigate

- [x] Write API is `db.set(key, value, index={'name': int})`, keyword-only.
      `__setitem__` unchanged.
- [x] Values are **`int` only** in v1. `bool` rejected explicitly.
      `TypeError` naming the index and the received type.
- [x] A rewrite that does not mention an index **preserves** it.
      `index={'name': None}` clears it.
- [x] **No epoch tail** for user indexes — an object with no value for the
      queried index does not appear. `mtime` behaviour unchanged.
- [x] DynamoDB: one sparse GSI per index name, `folder-<name>-index`.
- [x] Files and S3 **raise** on `set(index=...)` rather than dropping it.

## Stage 1 — Shared contract (`kydb/query.py`, `kydb/base.py`, `kydb/interface.py`, `kydb/folder_meta.py`)

Owns every shared file. Stages 2a/2b assume this is done.

- [x] `Entry` gains a fourth slot `index_value` (`kydb/query.py:13`).
      Extend `__init__`, `__repr__`, `__eq__`, `__hash__`. For an `mtime`
      query it equals `mtime`. Keep the parameter order
      `(key, mtime, ctime, index_value=None)` so existing construction sites
      keep working until each backend is updated.
- [x] `FolderQuery.until(ts)` — inclusive upper bound, mirroring `since()`.
      Add `until_ts` to `__init__`, `_clone` and the `_clone` kwargs dict
      (`kydb/query.py:56-70`). Docstring: both bounds inclusive; an inverted
      range yields nothing.
- [x] `ScanFolderQuery.entries()` applies `until` alongside `since`
      (`kydb/query.py:150`), and sorts/filters on the **index value** rather
      than always `r[1]`, so the scan backends inherit user indexes for free.
      Generalise `_raw_entries` to yield
      `(name, mtime, ctime, index_value)`.
- [x] `BaseDB.set` (`kydb/base.py:189`) grows a keyword-only
      `index: dict | None = None` and passes it to `set_raw`.
- [x] Validation helper on `BaseDB` — call it from `set` before any write:
      - name matches `^[A-Za-z][A-Za-z0-9_]*$`
      - name not in `{'path', 'folder', 'contents', 'mtime', 'ctime'}`
      - value is `int` and not `bool`, or is `None` (clear)
      - raise `ValueError` for a bad name, `TypeError` for a bad value,
        both naming the offending index
- [x] `set_raw` / `folder_meta_set_raw` signatures take `index=None`
      (`kydb/base.py:214`, `kydb/folder_meta.py:53-60`). `FolderMetaMixin.set_raw`
      forwards it to `folder_meta_set_raw`; folder-meta marker records never
      carry index values.
- [x] Default `BaseDB` behaviour when a backend cannot store index values:
      raise `IndexNotSupported` if `index` is non-empty. Backends that can
      store them override. This is what makes Files/S3 loud by default
      (`user_index_plan.md` §3.6) rather than by remembering to add a guard.
- [x] `KYDBInterface.set` / `folder` docstrings document the new argument,
      the int-only rule, preserve-on-rewrite, and that user indexes are
      strictly sparse with no epoch tail (`kydb/interface.py:62`, `:105`).
- [x] Tests in `kydb/tests/test_query.py` (new file): `until()` clone
      semantics, immutability (a chained call returns a new query, receiver
      unmutated), inverted range yields nothing, `Entry` equality/hash with
      `index_value`, and every validation rejection.

## Stage 2a — DynamoDB (`kydb/impl/dynamodb.py`, `kydb/impl/tests/conftest.py`)

Depends on Stage 1. File-disjoint from Stage 2b — do not touch
`redis.py`, `memory.py`, `files.py`, `s3.py`, or `test_impl.py`.

- [x] Add `folder-class_date-index` to the Moto test table
      (`kydb/impl/tests/conftest.py:54`): `folder` HASH, `class_date` RANGE
      (Number), projection `INCLUDE ['mtime','ctime']`; add `class_date` to
      `AttributeDefinitions`. This is the fixture the gym test needs.
- [x] `folder_meta_set_raw` (`kydb/impl/dynamodb.py:305`) accepts `index` and
      compiles it into the existing `update_item`:
      - `int` value → append `, <name>=:idx_<name>` to the `SET` clause
      - `None` → add a `REMOVE <name>` clause
      - use `ExpressionAttributeNames` placeholders for index names, so a
        name that happens to be a DynamoDB reserved word still works
      - unchanged when `index` is empty — no new attributes, no behaviour
        change for existing callers
- [x] Derive the GSI name: `folder-<index_name>-index`, with `mtime` still
      mapping to the existing `folder-time-index` (module constant
      `TIME_INDEX`, `kydb/impl/dynamodb.py:10`). One helper, used by both the
      query and the error message.
- [x] `DynamoDBFolderQuery`: drop the `_SUPPORTED_INDEXES = ('mtime',)` gate
      (`:70`) in favour of "any valid index name"; build the key condition
      from `self._index_name` rather than the literal `'mtime'`
      (`:76-85`), including `until` → `between` / `lte`.
- [x] `_index_rows` reads `item[self._index_name]` for the ordering value and
      keeps `mtime`/`ctime` from the projection. Cast every Number to `int` —
      boto3 returns `Decimal` (plan §8 #11).
- [x] `_includes_epoch()` returns **False** for any non-`mtime` index
      (`:165`), so `_raw_query`/`asc()`/`desc()` skip the `folder-index`
      fallback read entirely. This is plan §5 and is the main correctness
      risk in this stage — a user index must never emit epoch rows.
- [x] `_translate_missing_index` (`:141`) parameterised on the index name:
      both the `in str(err)` guard and the message. Keep accepting both
      `ValidationException` and `ResourceNotFoundException` (plan §8 #10).
      The message must name the GSI to add and its key schema.
- [x] `allow_scan=True` on a user index: the scan fallback reads
      `folder-index`, which projects only `mtime`/`ctime` — **not** arbitrary
      user attributes. Either extend the scan to fetch the attribute per item
      (documented O(n) reads, consistent with what `allow_scan` already
      means) or raise a clear `IndexNotSupported` saying a scan cannot serve
      a user index on this projection. **Pick one, document it in the
      docstring, and test it.**
- [x] `reindex` is `mtime`-only. A user index has no value to recover for a
      row that never had one — raise `IndexNotSupported` if asked to reindex
      a user index rather than stamping anything.
- [x] Tests in `kydb/impl/tests/test_user_index_dynamodb.py` (new file):
      write+query round trip; sparseness (no epoch rows, in both `asc()` and
      `desc()`); `since`/`until`/`between`; preserve-on-rewrite; `None`
      clears; ordering both directions; `entries()` exposes `index_value`
      **and** a real `mtime`; `entries()` costs one query and zero per-item
      reads (mirror `test_dynamodb_entries_does_not_refetch_per_item`);
      missing GSI raises a translated `IndexNotSupported` naming the index;
      `limit()` pages lazily.

## Stage 2b — Redis, Memory, and the Files/S3 guard

Depends on Stage 1. File-disjoint from Stage 2a — do not touch
`dynamodb.py`, `conftest.py`, or `test_impl.py`.

### Redis (`kydb/impl/redis.py`)

- [x] `_index_key(folder, name)` / `_index_val_key(folder, name)` alongside
      the existing `_mtime_key` / `_mtime_val_key` (`:257`), so `mtime` keeps
      its current key names byte-for-byte — **existing databases must not be
      invalidated.**
- [x] `folder_meta_set_raw` writes user index values into the pipeline it
      already builds (`:243`): `ZADD` + `HSET` per index, `ZREM` + `HDEL`
      for a `None`. Still one round trip.
- [x] `delete_raw` cleans up every user index for the object. The set of
      index names present must be discoverable — keep a per-folder set of
      index names (`<folder>:index-names`), or scan the known keys. Prefer
      the explicit set; an orphaned ZSET member is a silently wrong query
      result later.
- [x] `RedisFolderQuery`: index name parameterised; `until` maps to the
      `max` argument of `ZRANGEBYSCORE` / `ZREVRANGEBYSCORE`; `_epoch_entries`
      **skipped entirely** for a user index (plan §5).
- [x] `_SUPPORTED_INDEXES` gate removed (`:36`).

### Memory (`kydb/impl/memory.py`)

- [x] Per-db `{full_path: {name: value}}` store beside `__meta` (`:27`).
      Written in `folder_meta_set_raw` (`:41`), cleaned in `delete_raw`.
- [x] `_folder_time_entries` generalised to yield the queried index's value
      as the fourth element, skipping objects with no value for that index.
- [x] `MemoryFolderQuery` inherits `until`/filtering from the Stage 1
      `ScanFolderQuery` — it should need almost no new logic. If it does,
      Stage 1 generalised the wrong thing; say so rather than duplicating.

### Files and S3 (`kydb/impl/files.py`, `kydb/impl/s3.py`)

- [x] Confirm the Stage 1 default already raises `IndexNotSupported` on
      `set(index=...)` for both, and that `by('<user index>')` still raises.
      Add a test each; no implementation expected. If a guard *is* needed,
      Stage 1's default did not land correctly — fix it there, not here.

- [x] Tests in `kydb/impl/tests/test_user_index_redis_memory.py` (new file):
      the Redis and Memory equivalents of the Stage 2a list, plus Redis
      delete cleanup (an index entry must not outlive its object) and the
      Files/S3 rejection tests.

## Stage 3 — Wrappers, docs, and the end-to-end use case

Depends on 2a and 2b.

- [x] `UnionDB.set` forwards `index` to the front db (`kydb/union.py`).
- [x] `UnionFolderQuery` merges on `index_value`, not `mtime`
      (`kydb/union.py:99-159`); replace the hardcoded `by('mtime')` check
      (`:127`) with "at least one member supports this index". `until()`
      must propagate to each member query.
- [x] `CacheDB.set` forwards `index` to `persist_db` only
      (`kydb/cache.py`); folder queries already delegate (`:36`).
- [x] `docsrc/source/recency.rst` — a "Business keys" section: the write API,
      the int-only rule, preserve-on-rewrite, sparseness vs. the `mtime`
      epoch tail, and the per-index GSI requirement.
- [x] `docsrc/source/implementations.rst` and `AGENTS.md` — the additive
      table spec from plan §9, including the `folder-class_date-index` the
      test fixture now needs.
- [x] `README.md` — the gym example, if the feature showcase warrants it.
- [x] **`kydb/tests/test_gym_signups.py` (new file) — the acceptance test.**
      This is what "make sure this use case works" means, and it must run on
      every backend that supports user indexes (parameterise over
      memory/redis/dynamodb the way `test_impl.py` does):
      - book students into classes across three different `class_date`s,
        writing the bookings **out of order** and on a different day from the
        class, so a passing `mtime` implementation would fail
      - query one day exactly with `.since(d).until(d)` and assert the exact
        roster
      - assert a student with no `class_date` never appears
      - rebook a student to another day with a single `set(index=...)` and
        assert they leave the old day's roster and join the new one
      - assert `db.recent()` still answers "who booked most recently",
        ordered by booking time, independently of `class_date`
- [x] Full suite green: `PYTHONPATH=. python -m pytest kydb -q`, ≥268 passed
      with no existing test modified.

## Done — as-built notes

All three stages are merged. `PYTHONPATH=. python -m pytest kydb -q`
reports **496 passed, 1 skipped**; no existing test was modified.

Stage 3 added, beyond the list above:

- `kydb/tests/test_user_index_wrappers.py` — UnionDB / CacheDB / `DbObj`
  routing, which the gym test exercises only incidentally.
- `kydb/tests/conftest.py` — an opt-in `local_backends` fixture, because
  `kydb/impl/tests/conftest.py` only reaches its own directory and the
  acceptance test lives outside it. It is idempotent against that
  fixture already running (nested `mock_aws`, an already-patched
  `redis.Redis`).
- `DbObj` values can carry index values (plan §4.1). `write_dbobj` and
  `CacheDB.set_raw` thread `index` through; the previous
  `IndexNotSupported` raise in `BaseDB.set` is gone.
- `FolderQuery._supports_index(name)` replaces the tuple-membership gate,
  and MemoryDB's `_AnyIndexName` sentinel is gone with it.
  `UnionFolderQuery` overrides it as "at least one member supports this".

### Follow-up landed after the four stages

- **`mtime-index: false` now gates `mtime` alone** (plan §10). The eager
  guard moved out of `folder()` and into the point the query runs, so
  `by('class_date')` works on a db whose recency stamp is switched off.
  `reindex()` keeps its eager guard; `UnionDB` skips a member with the
  stamp off and raises only when every member has it off. Fixed a
  MemoryDB bug found by it: `_folder_time_entries` walked the `mtime`
  metadata to find candidates, so a business-key query came back empty
  with the stamp off — each index is now its own row source. Covered by
  `kydb/tests/test_mtime_index_disabled.py` (41 tests).
- **`test_recent_mtime_is_exact_nanoseconds` was flaky** (~1 run in 256
  per backend) and is fixed. Its guard proves precision by showing the
  timestamp is *not* one a double holds exactly, but a genuine clock
  lands on such a value once every 256 writes. An escape clause would
  have destroyed the test — a regressed value is exactly representable
  *every* time — so it now retries up to 8 writes and fails only if all
  of them are representable, which keeps full detection power.

- **The `UnionDB` shadowing limitation is logged, not fixed** (plan §11,
  by decision). A bounded query over a union of 2+ members emits one
  `logging` WARNING per `(folder, index)` on the `kydb.union` logger;
  every union index query is logged at DEBUG. kydb introduces a module
  logger and configures no handlers. Unbounded queries — including
  `recent(limit=10)` — are correct and stay silent. Covered by
  `kydb/tests/test_union_shadowing_warning.py` (11 tests), which asserts
  the wrong answer itself, so closing the hole later fails a test that
  must then be deleted on purpose. User-facing docs in
  `docsrc/source/recency.rst`.

## Notes for whoever picks this up

- The repo's existing docstrings carry the *reasoning*, not just the
  contract — match that density (see `DynamoDBFolderQuery`'s class
  docstring). Explain why a user index has no epoch tail wherever the code
  makes that choice.
- `additional_index_plan.md` is the companion design and its §5, §9.3 and
  §13 are load-bearing context for this one.
- Real DynamoDB is **not** required. Everything here runs under Moto; see
  plan §8.
