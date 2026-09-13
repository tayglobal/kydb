""" DynamoDB point reads: ``GetItem``, not ``Query``.

``path`` is the table's sole key attribute, so ``DynamoDB.get_raw`` is a
point read.  It was historically a ``Query`` with a key condition on
``path``, which matched exactly one item and cost exactly the same read
capacity -- so the change to ``get_item`` is a latency and correctness
change, not a cost one (``ak-server/optimisation_todo.md`` section 2).

The correctness half is what these tests are really for.  A read-through
cache in front of the table (DAX) caches *query result sets* under a
different TTL from item reads, so a ``Query`` point read keeps serving a
pre-write result after a write has invalidated the item.  Nothing uses
DAX today, and the way to keep it that way is to make the query path
unable to come back silently: the call-count tests below fail if it does.

The rest is the contract ``get_raw`` has to keep unchanged across the
switch -- ``KeyError`` on a missing item, ``contents`` round-tripping as
``Binary``, and the callers that depend on both (``exists_raw``,
``is_dir_raw``, ``CacheDB.read``).
"""

import pickle

import kydb
import pytest
from boto3.dynamodb.types import Binary

from test_impl import ALL_DB_TYPES, BASE_PATHS, DB_URLS, get_db


def _require_dynamodb():
    if 'dynamodb' not in ALL_DB_TYPES:
        pytest.skip('dynamodb not in KYDB_TEST_DB_TYPES')


def _counting(db):
    """ Instrument ``query`` and ``get_item`` on the table.

    Returns ``(query_calls, get_item_calls, restore)``.  Both are
    recorded: asserting only that ``get_item`` was called would still
    pass if ``get_raw`` did both.
    """
    query_calls = []
    get_item_calls = []
    original_query = db.table.query
    original_get_item = db.table.get_item

    def counting_query(**kwargs):
        query_calls.append(kwargs)
        return original_query(**kwargs)

    def counting_get_item(**kwargs):
        get_item_calls.append(kwargs)
        return original_get_item(**kwargs)

    db.table.query = counting_query
    db.table.get_item = counting_get_item

    def restore():
        del db.table.query
        del db.table.get_item

    return query_calls, get_item_calls, restore


# --- the operation itself ---------------------------------------------

def test_get_raw_uses_get_item_not_query():
    """ The regression guard: a point read must not be a Query. """
    _require_dynamodb()
    db = get_db('dynamodb', '')
    key = '/unittests/point_read/obj'
    try:
        db[key] = {'a': 1}
        full_path = db._get_full_path(key)

        query_calls, get_item_calls, restore = _counting(db)
        try:
            assert db.get_raw(full_path) == pickle.dumps({'a': 1})
        finally:
            restore()

        assert query_calls == []
        assert len(get_item_calls) == 1
        # Exactly the key, nothing else: in particular no ConsistentRead,
        # which the Query it replaced did not set either.  Adding one
        # here would double the read cost of every read in the codebase.
        assert get_item_calls[0] == {'Key': {'path': full_path}}
    finally:
        db.rm_tree('/unittests/point_read')


def test_read_uses_get_item_not_query():
    """ The public read path, not just get_raw directly. """
    _require_dynamodb()
    db = get_db('dynamodb', '')
    key = '/unittests/point_read_public/obj'
    try:
        db[key] = 'hello'

        query_calls, get_item_calls, restore = _counting(db)
        try:
            assert db.read(key, reload=True) == 'hello'
        finally:
            restore()

        assert query_calls == []
        assert len(get_item_calls) == 1
    finally:
        db.rm_tree('/unittests/point_read_public')


@pytest.mark.parametrize('base_path', BASE_PATHS)
def test_get_item_key_is_the_full_path(base_path):
    """ base_path must be prepended, exactly as the key condition did. """
    _require_dynamodb()
    db = get_db('dynamodb', base_path)
    key = '/unittests/point_read_base_path/obj'
    try:
        db[key] = 7
        full_path = db._get_full_path(key)

        query_calls, get_item_calls, restore = _counting(db)
        try:
            assert db.read(key, reload=True) == 7
        finally:
            restore()

        assert query_calls == []
        assert get_item_calls == [{'Key': {'path': full_path}}]
        if base_path:
            assert full_path.startswith('/' + base_path + '/')
    finally:
        db.rm_tree('/unittests/point_read_base_path')


# --- the missing-item contract ----------------------------------------

def test_missing_key_raises_key_error_naming_the_path():
    """ ``get_item`` omits ``Item`` where ``query`` returned ``[]``. """
    _require_dynamodb()
    db = get_db('dynamodb', '')
    full_path = db._get_full_path('/unittests/point_read_missing/nope')

    with pytest.raises(KeyError) as excinfo:
        db.get_raw(full_path)

    assert excinfo.value.args[0] == full_path


def test_read_missing_key_raises_key_error():
    _require_dynamodb()
    db = get_db('dynamodb', '')
    with pytest.raises(KeyError):
        db.read('/unittests/point_read_missing/nope')


def test_deleted_key_raises_key_error():
    """ A read after delete must miss, not resurrect the old value. """
    _require_dynamodb()
    db = get_db('dynamodb', '')
    key = '/unittests/point_read_delete/obj'
    db[key] = 1
    assert db.read(key, reload=True) == 1

    db.delete(key)

    with pytest.raises(KeyError):
        db.read(key, reload=True)
    assert db.exists(key) is False


# --- exists_raw / is_dir_raw route through get_raw ---------------------

def test_exists_is_true_and_false_without_raising():
    _require_dynamodb()
    db = get_db('dynamodb', '')
    key = '/unittests/point_read_exists/obj'
    try:
        db[key] = 1
        assert db.exists(key) is True
        assert db.exists('/unittests/point_read_exists/absent') is False
    finally:
        db.rm_tree('/unittests/point_read_exists')


def test_exists_uses_get_item_not_query():
    _require_dynamodb()
    db = get_db('dynamodb', '')
    key = '/unittests/point_read_exists_op/obj'
    try:
        db[key] = 1

        query_calls, get_item_calls, restore = _counting(db)
        try:
            assert db.exists(key) is True
            assert db.exists('/unittests/point_read_exists_op/absent') \
                is False
        finally:
            restore()

        assert query_calls == []
        assert len(get_item_calls) == 2
    finally:
        db.rm_tree('/unittests/point_read_exists_op')


def test_is_dir_still_works_through_folder_meta():
    """ ``is_dir_raw`` -> ``exists_raw`` -> ``get_raw``. """
    _require_dynamodb()
    db = get_db('dynamodb', '')
    try:
        db.mkdir('/unittests/point_read_isdir/foo')
        assert db.is_dir('/unittests/point_read_isdir/foo') is True
        assert db.is_dir('/unittests/point_read_isdir/bar') is False
    finally:
        db.rm_tree('/unittests/point_read_isdir')


# --- contents round-trips as Binary -----------------------------------

def test_contents_round_trips_as_binary():
    """ ``get_item`` and ``query`` deserialise a B attribute the same
    way -- as ``boto3.dynamodb.types.Binary`` -- so ``.value`` is still
    the right unwrap.  Asserted on the raw item, not just on the value,
    so a future change of shape is caught here rather than as a
    ``AttributeError`` deep in a caller.
    """
    _require_dynamodb()
    db = get_db('dynamodb', '')
    key = '/unittests/point_read_binary/obj'
    try:
        value = {'nested': [1, 2, 3], 'text': 'x'}
        db[key] = value
        full_path = db._get_full_path(key)

        item = db.table.get_item(Key={'path': full_path})['Item']
        assert isinstance(item['contents'], Binary)
        assert item['contents'].value == pickle.dumps(value)

        raw = db.get_raw(full_path)
        assert isinstance(raw, bytes)
        assert pickle.loads(raw) == value
    finally:
        db.rm_tree('/unittests/point_read_binary')


# Explicit ids: pytest renders a bytes parameter by repr, and the 100 KB
# blob below would otherwise put a 400 KB test id in every -v run.
@pytest.mark.parametrize('value', [
    pytest.param(0, id='zero'),
    pytest.param(-1, id='negative'),
    pytest.param(123456789012345678901234567890, id='bignum'),
    pytest.param(3.5, id='float'),
    pytest.param('', id='empty_str'),
    pytest.param('unicode: 山 \U0001f600', id='unicode'),
    pytest.param(b'\x00\x01\x02\xff', id='bytes'),
    pytest.param(None, id='none'),
    pytest.param(True, id='true'),
    pytest.param([], id='empty_list'),
    pytest.param({}, id='empty_dict'),
    pytest.param({'a': [1, {'b': (2, 3)}]}, id='nested'),
    pytest.param(b'\x00' * 100_000, id='large_blob_100kb'),
])
def test_value_round_trip(value):
    """ Values that exercise pickling, empty payloads and a large binary
    blob -- an empty ``bytes`` value is the one DynamoDB historically
    rejected, and a 100KB payload spans more than one wire chunk.
    """
    _require_dynamodb()
    db = get_db('dynamodb', '')
    key = '/unittests/point_read_values/obj'
    try:
        db[key] = value
        assert db.read(key, reload=True) == value
    finally:
        db.rm_tree('/unittests/point_read_values')


def test_rewrite_is_visible_to_the_next_read():
    """ The read-your-write case the DAX query-TTL trap would break. """
    _require_dynamodb()
    db = get_db('dynamodb', '')
    key = '/unittests/point_read_rewrite/obj'
    try:
        for expected in range(5):
            db[key] = expected
            assert db.read(key, reload=True) == expected
    finally:
        db.rm_tree('/unittests/point_read_rewrite')


# --- callers that hold get_raw directly -------------------------------

def test_cache_db_read_through_get_raw():
    """ ``CacheDB.read`` calls ``get_raw`` on the cache db and indexes
    the persist db, so it depends on both halves of the contract.
    """
    _require_dynamodb()
    db = kydb.connect(DB_URLS['memory'] + '|' + DB_URLS['dynamodb'])
    key = '/unittests/point_read_cache/obj'
    try:
        db[key] = {'v': 1}
        # First read: cache miss, served by the DynamoDB point read.
        assert db.read(key) == {'v': 1}
        # Second read: cache hit, served by CacheDB's own get_raw path.
        assert db.read(key) == {'v': 1}
        assert db.persist_db.read(key, reload=True) == {'v': 1}
    finally:
        db.rm_tree('/unittests/point_read_cache')


def test_folder_items_still_reads_each_object():
    """ ``folder(...).items()`` reads objects by key; those reads are
    point reads too, and must be ``get_item``.
    """
    _require_dynamodb()
    db = get_db('dynamodb', '')
    folder = '/unittests/point_read_items/'
    try:
        for i in range(3):
            db[folder + f'obj{i}'] = i

        # items() goes through db.read(), which consults BaseDB._cache;
        # clear it so the count below is the number of table reads, not
        # whatever the writes happened to leave cached.
        db.clear_cache()

        query_calls, get_item_calls, restore = _counting(db)
        try:
            items = dict(db.folder(folder).by('mtime').desc().items())
        finally:
            restore()

        assert items == {'obj0': 0, 'obj1': 1, 'obj2': 2}
        # One get_item per object read, and no point read left on query.
        assert len(get_item_calls) == 3
        assert all(
            set(kwargs) == {'Key'} for kwargs in get_item_calls)
        # The only queries are the index reads, never a point read.
        assert all('IndexName' in kwargs for kwargs in query_calls)
    finally:
        db.rm_tree('/unittests/point_read_items')
