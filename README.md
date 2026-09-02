<p align="center">
  <img src="https://raw.githubusercontent.com/tayglobal/kydb/master/docsrc/source/_static/images/kydb-logo.png" alt="kydb" width="320">
</p>

<h1 align="center">kydb — the Kinyu Database</h1>

<p align="center">
  <em>One intuitive, Pythonic interface. Many NoSQL backends. Change your database by changing a URL.</em>
</p>

<p align="center">
  <a href="https://pypi.org/project/kydb/"><img src="https://img.shields.io/pypi/v/kydb.svg" alt="PyPI"></a>
  <a href="https://kydb.readthedocs.io/en/latest/"><img src="https://img.shields.io/badge/docs-readthedocs-blue.svg" alt="Documentation"></a>
  <a href="https://github.com/tayglobal/kydb/actions"><img src="https://github.com/tayglobal/kydb/workflows/Python%20application/badge.svg" alt="Build"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-green.svg" alt="License"></a>
</p>

---

kydb is an abstraction layer over NoSQL databases, built for the kind of
workloads found in financial services: object hierarchies, layered caches,
market-data invalidation, and objects that must survive years of refactoring.

```python
import kydb

db = kydb.connect('dynamodb://my-table')
db['/trading/books/MYBOOK'] = my_book_object
```

That is the whole API surface you need to learn. Everything below is the
same `db` object, pointed at a different URL.

## Installation

```bash
pip install kydb
```

## Why kydb

| | |
|---|---|
| **A dict, backed by anything** | `db[key] = value` works identically on S3, DynamoDB, Redis, the filesystem, memory or HTTP. |
| **The URL *is* the config** | `s3://bucket`, `redis://host:6379`, `dynamodb://table`. Swap backends without touching a line of application code. |
| **Filesystem hierarchy** | `mkdir`, `ls`, `rmdir`, `rm_tree` — on databases that have no concept of a folder. |
| **Unions** | `redis://cache;s3://prod` — read from the first database that has the key, always write to the front. |
| **Cache DB** | `redis://cache\|dynamodb://table` — a read-through cache in front of a persistent store. |
| **Cache context** | Scope in-memory caching to a block, so live market data reprices while static data stays cached. |
| **Store any Python object** | Anything pickleable, or use the `DbObj` decorators for refactor-proof, field-level control. |
| **Recency queries** | `db.recent('/articles', limit=10)` — server-side ordered indexes on DynamoDB and Redis. |

## Databases in one line each

```python
db = kydb.connect('s3://my-kydb-bucket')             # AWS S3
db = kydb.connect('dynamodb://kydb')                 # AWS DynamoDB
db = kydb.connect('redis://cache.epythoncloud.io')   # Redis (port optional)
db = kydb.connect('memory://cache001')               # In-memory
db = kydb.connect('files://tmp/foo/bar')             # Local filesystem
db = kydb.connect('http://my-source-host')           # HTTP  (read-only source)
db = kydb.connect('https://my-source-host')          # HTTPS (read-only source)
```

Every one of them returns the same
[`KYDBInterface`](https://kydb.readthedocs.io/en/latest/api.html). Your code
does not know, and does not care, which one it got.

```python
def save_report(db, name, report):
    db[f'/reports/{name}'] = report   # works against all of the above
```

Prefer paths relative to a root? Set a base path in the URL:

```python
db = kydb.connect('dynamodb://my-source-db/home/tony.yum')
db['foo'] = 'Hello World!'   # writes to /home/tony.yum/foo
```

## Reading and writing

```python
db['/mytest/foo'] = 123
db['/mytest/foo']            # 123 — served from cache, no round trip
db.read('/mytest/foo', reload=True)   # force a read from the database
db.exists('/mytest/foo')     # True
db.delete('/mytest/foo')
```

Any pickleable object goes in as-is:

```python
from datetime import datetime

db['/mytest/bar'] = {
    'my_int': 123,
    'my_float': 123.456,
    'my_str': 'hello',
    'my_list': [1, 2, 3],
    'my_datetime': datetime.now(),
}
```

## A filesystem on a document store

Document databases have no folders. kydb gives you them anyway — including
empty ones — on every backend.

```python
db.mkdir('/empty-folder')
db.ls('/')                    # ['empty-folder/']

db['/foo/bar/baz'] = 123      # intermediate folders are created for you
db.ls('/')                    # ['empty-folder/', 'foo/']
db.ls('/foo')                 # ['bar/']

db.rmdir('/empty-folder')     # removes an empty folder
db.rm_tree('/foo')            # recursive, like rm -rf
db.ls('/')                    # []
```

Folders always end in `/`; objects never do. `list_dir` is the lazy,
paginated generator behind `ls`.

## Unions: layer databases with a URL

Separate the databases with `;`. Reads try each database in order and return
the first hit; writes always go to the front.

```python
db = kydb.connect('memory://scratch;s3://team-shared;s3://firm-golden')

db['/curves/USD.OIS']      # found in whichever layer has it first
db['/curves/USD.OIS'] = c  # always written to memory://scratch
```

That is a personal overlay on top of shared data, with no branching logic in
your code — perfect for "what if I tweak this one curve?" research against a
production dataset you must not touch.

## CacheDB: a fast cache in front of a slow store

Separate two databases with `|`. Reads hit the cache first and fall back to
the persistent store; writes go to both.

```python
db = kydb.connect('redis://my-redis-host|s3://my-s3-prod-source')

db['/refdata/instruments'] = instruments   # written to Redis *and* S3
db['/refdata/instruments']                 # served from Redis
```

## Cache context: invalidate exactly what you mean to

kydb caches reads in memory, so repeatedly pricing a book does not hammer the
database. But live market data must not be cached across ticks. `refresh()`
is too blunt — it drops the positions too — and tracking invalidation by hand
is error-prone. Scope the cache to a `with` block instead:

```python
db = kydb.connect('dynamodb://tradingdb')
book = db['/Books/MyBook']

with db.cache_context():

    positions = book.Positions()      # loaded once, cached for the outer block

    while keep_pricing():

        with db.cache_context():
            for qty, inst in positions.items():
                print(qty, inst, inst.Price())

        wait_for_next_marketdata_tick()
        # inner block exited: all market-data cache is gone

# outer block exited: the position cache is gone too
```

## Recency queries

List a folder in modification-time order, newest first. DynamoDB uses a
`folder-time-index` GSI and Redis a native sorted set — the ordering is done
by the database, not in your process.

```python
newest = db.recent('/articles', limit=10)     # lazy generator of names
```

For more control, build a query. Each call returns a new query, so a partly
built one can be reused and branched:

```python
base = db.folder('/articles').by('mtime')

names    = base.desc().limit(10)             # ['2026-09-02-launch', ...]
pairs    = base.since(timestamp_ns).items()  # (name, value) pairs
entries  = base.desc().entries()             # Entry: .key, .mtime, .ctime
```

Timestamps are integer nanoseconds since the Unix epoch. Backends with no
server-side ordering index (Memory, Files, S3) refuse to silently run an
expensive scan — you have to ask for it:

```python
db.recent('/articles', limit=10)                    # raises IndexNotSupported
db.recent('/articles', limit=10, allow_scan=True)   # client-side sort, opt-in
```

Objects written before the index existed are reported honestly at
`mtime == 0` rather than being guessed at, and move into place the first time
they are rewritten. No migration is required. See the
[recency documentation](https://kydb.readthedocs.io/en/latest/recency.html)
for the full ordering contract and the DynamoDB schema.

## Refactor-proof objects with `DbObj`

Pickling a class ties your stored data to the module path it was written
from. `DbObj` breaks that link: the database holds a name, and a config maps
that name to wherever the class lives today. Move the class, and old objects
still deserialise.

Register the classes once:

```python
db = kydb.connect('memory://decorated_py_obj')

db.upload_objdb_config({
    'Greeter': {
        'module_path': 'path.to.module',
        'class_name': 'Greeter',
    }
})
```

Declare which attributes are persisted with `@kydb.stored` — everything else
is transient state, rebuilt by `init()` on each load:

```python
import kydb

class Greeter(kydb.DbObj):

    def init(self):
        self.greet_count = 0      # transient

    @kydb.stored
    def name(self):
        return 'John'             # the default value

    def greet(self):
        self.greet_count += 1
        return 'Hello ' + self.name()
```

```python
key = '/hello-world/greeter001'

greeter = db.new('Greeter', key)
greeter.name()                     # 'John'  — the default
greeter.greet()                    # 'Hello John'

greeter = db.new('Greeter', key, name='Tony')
greeter.greet()                    # 'Hello Tony'

greeter.name.setvalue('Jane')
greeter.greet()                    # 'Hello Jane'

greeter.get_stored_dict()          # {'name': 'Jane'} — only `name` persists
```

Persist it and read it back; `greet_count` resets because it was never
stored:

```python
db[key] = greeter

db.read(key, reload=True).greet_count   # 0
db.read(key, reload=True).name()        # 'Jane'
```

## Secured Redis

Redis connection details, including a password encrypted with AWS KMS, can be
supplied through a config file pointed at by `KYDB_CONFIG_PATH`:

```yaml
dbs:
  my-redis-db:
    host: cache.epythoncloud.io
    port: 6379
    password:
      encryption-method: kms          # or `plain`
      encryption-key: <kms-key-id>
      env_var: MY_REDIS_PASSWORD      # holds the base64 ciphertext
```

The same file switches off the recency index per database with
`mtime-index: false`.

## Documentation

Full documentation, including the complete API reference and the DynamoDB
table schema, lives at
[kydb.readthedocs.io](https://kydb.readthedocs.io/en/latest/).

## License

MIT — see [LICENSE](LICENSE).
