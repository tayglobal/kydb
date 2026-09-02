""" Backward compatibility for the additional index.

Covers plan sections 9 (existing databases keep working without a
migration) and 13 (the index can be switched off per db).

Two distinct situations produce an object with no timestamp, and they
must behave identically:

1. it was written before the feature existed;
2. it was written while ``mtime-index: false`` was set in config.

Either way the object is invisible to the sparse index, and is served
from a fallback folder read as an "epoch" entry -- ``mtime == ctime ==
0``, sorting after everything indexed under ``desc()``.
"""
import os
import boto3
import kydb
import pytest
import time
import yaml
from contextlib import contextmanager

from kydb.exceptions import IndexNotSupported

from test_impl import ALL_DB_TYPES, DB_URLS, assert_eventually_equal, get_db


def _require(db_type):
    if db_type not in ALL_DB_TYPES:
        pytest.skip(f'{db_type} not in KYDB_TEST_DB_TYPES')


@contextmanager
def kydb_config(config: dict):
    """ Run with ``KYDB_CONFIG_PATH`` pointing at ``config``.

    ``kydb.connect`` memoises by URL, so the cache is cleared on the way
    in and out -- otherwise a db built under a different config would be
    handed back and the setting silently ignored.
    """
    import tempfile
    orig = os.environ.get('KYDB_CONFIG_PATH')
    fd, path = tempfile.mkstemp(suffix='.yml')
    with os.fdopen(fd, 'w') as f:
        yaml.safe_dump(config, f)

    kydb.api._db_cache.clear()
    os.environ['KYDB_CONFIG_PATH'] = path
    try:
        yield
    finally:
        kydb.api._db_cache.clear()
        if orig is None:
            os.environ.pop('KYDB_CONFIG_PATH', None)
        else:
            os.environ['KYDB_CONFIG_PATH'] = orig
        os.unlink(path)


def _dynamodb_table_name():
    return os.environ.get('KINYU_UNITTEST_DYNAMODB', 'kydb-test-table')


def _write_legacy_row(db, full_path):
    """ Put a row carrying no ``mtime``/``ctime``, exactly as a kydb
    predating the feature would have written it (``put_item``, three
    attributes).
    """
    folder = full_path.rsplit('/', 1)[0] + '/'
    db.table.put_item(Item={
        'path': full_path,
        'folder': folder,
        'contents': boto3.dynamodb.types.Binary(db._serialise('legacy')),
    })


# --- Section 9: the epoch tail -------------------------------------------

def test_dynamodb_legacy_row_appears_at_tail_of_desc():
    """ A row with no mtime is invisible to the sparse GSI, but must
    still show up -- last, at epoch -- rather than vanishing.
    """
    _require('dynamodb')
    db = get_db('dynamodb', '')
    folder = '/unittests/test_epoch_tail/'
    try:
        _write_legacy_row(db, folder + 'old')
        for i in range(3):
            db[folder + f'new{i}'] = i
            time.sleep(0.001)

        assert_eventually_equal(
            lambda: list(db.folder(folder).by('mtime').desc()),
            ['new2', 'new1', 'new0', 'old'])
        entries = list(db.folder(folder).by('mtime').desc().entries())
        assert [e.key for e in entries] == ['new2', 'new1', 'new0', 'old']

        old = entries[-1]
        assert old.mtime == 0
        assert old.ctime == 0
        # And the real rows are unaffected.
        assert all(e.mtime > 0 for e in entries[:-1])
    finally:
        db.rm_tree(folder)


def test_dynamodb_legacy_row_first_under_asc():
    """ Epoch sorts before every real timestamp. """
    _require('dynamodb')
    db = get_db('dynamodb', '')
    folder = '/unittests/test_epoch_asc/'
    try:
        for i in range(3):
            db[folder + f'new{i}'] = i
            time.sleep(0.001)
        _write_legacy_row(db, folder + 'old')

        names = assert_eventually_equal(
            lambda: list(db.folder(folder).by('mtime').asc()),
            ['old', 'new0', 'new1', 'new2'])
        assert names == ['old', 'new0', 'new1', 'new2']
    finally:
        db.rm_tree(folder)


def test_dynamodb_since_excludes_legacy_and_skips_fallback():
    """ ``since(ts)`` with ts > 0 cannot match an epoch row, so the
    fallback folder read must not happen at all.
    """
    _require('dynamodb')
    db = get_db('dynamodb', '')
    folder = '/unittests/test_epoch_since/'
    try:
        _write_legacy_row(db, folder + 'old')
        ts = time.time_ns()
        db[folder + 'new'] = 1

        assert_eventually_equal(
            lambda: list(db.folder(folder).by('mtime').since(ts).desc()),
            ['new'])

        query_calls = []
        original_query = db.table.query

        def counting_query(**kwargs):
            query_calls.append(kwargs)
            return original_query(**kwargs)

        db.table.query = counting_query
        try:
            names = list(db.folder(folder).by('mtime').since(ts).desc())
        finally:
            del db.table.query

        assert names == ['new']
        assert all(c['IndexName'] == 'folder-time-index'
                   for c in query_calls)
    finally:
        db.rm_tree(folder)


def test_dynamodb_limit_covered_by_index_skips_fallback():
    """ The common query -- recent(n) over a folder with at least n
    indexed objects -- must not pay for the epoch tail at all.
    """
    _require('dynamodb')
    db = get_db('dynamodb', '')
    folder = '/unittests/test_epoch_limit/'
    try:
        _write_legacy_row(db, folder + 'old')
        for i in range(4):
            db[folder + f'new{i}'] = i
            time.sleep(0.001)

        assert_eventually_equal(
            lambda: list(db.recent(folder, limit=2)), ['new3', 'new2'])

        query_calls = []
        original_query = db.table.query

        def counting_query(**kwargs):
            query_calls.append(kwargs)
            return original_query(**kwargs)

        db.table.query = counting_query
        try:
            names = list(db.recent(folder, limit=2))
        finally:
            del db.table.query

        assert names == ['new3', 'new2']
        assert len(query_calls) == 1
        assert query_calls[0]['IndexName'] == 'folder-time-index'
    finally:
        db.rm_tree(folder)


def test_dynamodb_rewrite_promotes_legacy_row_out_of_tail():
    """ The write path is self-healing: a legacy object rewritten once
    joins the index at its real time, with ctime set at that first touch.
    """
    _require('dynamodb')
    db = get_db('dynamodb', '')
    folder = '/unittests/test_epoch_promote/'
    try:
        _write_legacy_row(db, folder + 'old')
        db[folder + 'new'] = 1

        assert_eventually_equal(
            lambda: list(db.folder(folder).by('mtime').desc()), ['new', 'old'])

        before = time.time_ns()
        db[folder + 'old'] = 'rewritten'

        assert_eventually_equal(
            lambda: list(db.folder(folder).by('mtime').desc()), ['old', 'new'])
        entries = list(db.folder(folder).by('mtime').desc().entries())
        assert [e.key for e in entries] == ['old', 'new']
        assert entries[0].mtime >= before
        assert entries[0].ctime >= before
    finally:
        db.rm_tree(folder)


def test_dynamodb_epoch_tail_excludes_directories():
    """ Sub-folders are not objects and must stay out of the tail, just
    as they stay out of the index.
    """
    _require('dynamodb')
    db = get_db('dynamodb', '')
    folder = '/unittests/test_epoch_dirs/'
    try:
        _write_legacy_row(db, folder + 'old')
        db[folder + 'sub/child'] = 1
        db[folder + 'new'] = 1

        names = assert_eventually_equal(
            lambda: list(db.folder(folder).by('mtime').desc()), ['new', 'old'])
        assert names == ['new', 'old']
    finally:
        db.rm_tree(folder)


def test_redis_legacy_object_appears_at_tail():
    """ Same contract on Redis: an object in the folder hash with no
    sorted-set entry is served at epoch, last.
    """
    _require('redis')
    db = get_db('redis', '')
    folder = '/unittests/test_redis_epoch/'
    try:
        db[folder + 'old'] = 1
        db[folder + 'new'] = 2
        # Drop 'old' from the index to leave exactly what a pre-feature
        # write left behind: the object and its folder-hash entry, and
        # nothing else.
        full = db._ensure_slashes(db._get_full_path(folder))[:-1]
        db.connection.zrem(db._mtime_key(full), 'old')
        db.connection.hdel(db._mtime_val_key(full), 'old')
        db.connection.hdel(db._ctime_key(full), 'old')

        entries = list(db.folder(folder).by('mtime').desc().entries())
        assert [e.key for e in entries] == ['new', 'old']
        assert entries[-1].mtime == 0
        assert entries[-1].ctime == 0

        assert list(db.folder(folder).by('mtime').asc()) == ['old', 'new']
    finally:
        db.rm_tree(folder)


# --- Section 9.4: a table with no folder-time-index ----------------------

@contextmanager
def _table_without_time_index():
    """ A table shaped exactly like a pre-feature kydb table: path key,
    folder-index, and no folder-time-index.
    """
    name = '{}-no-time-{}-{}'.format(
        _dynamodb_table_name(), os.getpid(), time.time_ns())
    client = boto3.client('dynamodb')
    client.create_table(
        TableName=name,
        KeySchema=[{'AttributeName': 'path', 'KeyType': 'HASH'}],
        AttributeDefinitions=[
            {'AttributeName': 'path', 'AttributeType': 'S'},
            {'AttributeName': 'folder', 'AttributeType': 'S'},
        ],
        BillingMode='PAY_PER_REQUEST',
        GlobalSecondaryIndexes=[{
            'IndexName': 'folder-index',
            'KeySchema': [{'AttributeName': 'folder', 'KeyType': 'HASH'}],
            'Projection': {'ProjectionType': 'ALL'},
        }],
        Tags=[
            {'Key': 'Purpose', 'Value': 'kydb-real-tests'},
            {'Key': 'ManagedBy', 'Value': 'openclaw'},
        ],
    )
    waiter_config = {'Delay': 1, 'MaxAttempts': 60}
    client.get_waiter('table_exists').wait(
        TableName=name, WaiterConfig=waiter_config)
    kydb.api._db_cache.clear()
    try:
        yield kydb.connect('dynamodb://' + name)
    finally:
        kydb.api._db_cache.clear()
        client.delete_table(TableName=name)
        client.get_waiter('table_not_exists').wait(
            TableName=name, WaiterConfig=waiter_config)


def test_missing_time_index_still_reads_and_writes():
    """ The core promise: upgrading kydb against an untouched table must
    not break anything that worked before.
    """
    _require('dynamodb')
    with _table_without_time_index() as db:
        db['/unittests/compat/obj'] = 123
        assert db['/unittests/compat/obj'] == 123
        assert_eventually_equal(
            lambda: 'obj' in list(db.list_dir('/unittests/compat')), True)
        db.delete('/unittests/compat/obj')
        assert not db.exists('/unittests/compat/obj')


def test_missing_time_index_raises_index_not_supported():
    """ ...and the one thing that cannot work says so in kydb's own
    vocabulary, rather than leaking a boto3 ValidationException.
    """
    _require('dynamodb')
    with _table_without_time_index() as db:
        db['/unittests/compat/obj'] = 123
        with pytest.raises(IndexNotSupported) as excinfo:
            list(db.recent('/unittests/compat'))

        msg = str(excinfo.value)
        assert 'folder-time-index' in msg
        assert 'allow_scan' in msg


def test_missing_time_index_allow_scan_serves_the_query():
    """ allow_scan=True falls back to folder-index, so an unmigrated
    table can answer recency queries at a cost the caller opted into.
    """
    _require('dynamodb')
    with _table_without_time_index() as db:
        folder = '/unittests/compat_scan/'
        for i in range(3):
            db[folder + f'obj{i}'] = i
            time.sleep(0.001)

        names = assert_eventually_equal(
            lambda: list(db.recent(folder, limit=2, allow_scan=True)),
            ['obj2', 'obj1'])
        assert names == ['obj2', 'obj1']

        entries = list(
            db.folder(folder, allow_scan=True).by('mtime').desc().entries())
        assert [e.key for e in entries] == ['obj2', 'obj1', 'obj0']
        assert all(e.mtime > 0 for e in entries)


# --- Section 13: the index can be switched off ---------------------------

def test_mtime_index_enabled_by_default():
    """ No config file at all must leave the index on, so the feature
    works with nothing configured. Guards against a default flip.
    """
    for db_type in ('dynamodb', 'redis', 'memory'):
        if db_type in ALL_DB_TYPES:
            assert get_db(db_type, '').mtime_index_enabled


def test_dynamodb_disabled_writes_no_timestamps():
    _require('dynamodb')
    table = _dynamodb_table_name()
    key = '/unittests/test_disabled/obj'
    with kydb_config({'dbs': {table: {'mtime-index': False}}}):
        db = kydb.connect(DB_URLS['dynamodb'])
        assert not db.mtime_index_enabled
        try:
            db[key] = 123
            assert db[key] == 123

            item = db.table.get_item(Key={'path': key})['Item']
            assert 'mtime' not in item
            assert 'ctime' not in item

            with pytest.raises(IndexNotSupported) as excinfo:
                list(db.recent('/unittests/test_disabled'))
            assert 'mtime-index' in str(excinfo.value)

            # allow_scan does not rescue it: there are no timestamps to
            # sort by, so promising an ordering would be a lie.
            with pytest.raises(IndexNotSupported):
                list(db.recent('/unittests/test_disabled', allow_scan=True))
        finally:
            db.rm_tree('/unittests/test_disabled')


def test_redis_disabled_writes_no_index_entries():
    _require('redis')
    db_name = DB_URLS['redis'].split('//', 1)[1]
    folder = '/unittests/test_redis_disabled/'
    with kydb_config({'dbs': {db_name: {'mtime-index': False}}}):
        db = kydb.connect(DB_URLS['redis'])
        assert not db.mtime_index_enabled
        try:
            db[folder + 'obj'] = 123
            assert db[folder + 'obj'] == 123

            full = db._ensure_slashes(db._get_full_path(folder))[:-1]
            assert db.connection.zcard(db._mtime_key(full)) == 0

            with pytest.raises(IndexNotSupported):
                list(db.recent(folder))
        finally:
            db.rm_tree(folder)


def test_memory_disabled_records_no_meta():
    _require('memory')
    db_name = DB_URLS['memory'].split('//', 1)[1]
    folder = '/unittests/test_memory_disabled/'
    with kydb_config({'dbs': {db_name: {'mtime-index': False}}}):
        db = kydb.connect(DB_URLS['memory'])
        assert not db.mtime_index_enabled
        db[folder + 'obj'] = 123
        assert db[folder + 'obj'] == 123

        assert list(db._folder_time_entries(folder)) == []
        with pytest.raises(IndexNotSupported):
            list(db.recent(folder, allow_scan=True))


def test_disabled_then_reenabled_lands_in_epoch_tail():
    """ Section 13.5: the switch is only safe because of the epoch tail.

    Objects written while the index was off must reappear when it is
    turned back on -- at the end, at epoch -- rather than being silently
    dropped from every recency query.
    """
    _require('dynamodb')
    table = _dynamodb_table_name()
    folder = '/unittests/test_toggle/'
    db = get_db('dynamodb', '')
    try:
        with kydb_config({'dbs': {table: {'mtime-index': False}}}):
            off_db = kydb.connect(DB_URLS['dynamodb'])
            off_db[folder + 'written_while_off'] = 1

        # Index back on (no config at all).
        kydb.api._db_cache.clear()
        db = kydb.connect(DB_URLS['dynamodb'])
        assert db.mtime_index_enabled
        db[folder + 'written_while_on'] = 2

        assert_eventually_equal(
            lambda: list(db.folder(folder).by('mtime').desc()),
            ['written_while_on', 'written_while_off'])
        entries = list(db.folder(folder).by('mtime').desc().entries())
        assert [e.key for e in entries] == [
            'written_while_on', 'written_while_off']
        assert entries[-1].mtime == 0

        # And it heals on the next write, exactly like a pre-feature row.
        db[folder + 'written_while_off'] = 3
        assert_eventually_equal(
            lambda: list(db.folder(folder).by('mtime').desc()),
            ['written_while_off', 'written_while_on'])
    finally:
        kydb.api._db_cache.clear()
        kydb.connect(DB_URLS['dynamodb']).rm_tree(folder)


# --- Section 13.2: Redis writes and deletes are one round trip ----------

def _count_round_trips(db):
    """ Count Redis round trips, treating a pipeline execute as one.

    Wraps both the direct command path and the pipeline, so a regression
    that un-batches the commands shows up as a jump in the count.
    """
    calls = []
    conn = db.connection
    original_execute_command = conn.execute_command
    original_pipeline = conn.pipeline

    def counting_execute_command(*args, **kwargs):
        calls.append(args[0] if args else None)
        return original_execute_command(*args, **kwargs)

    def counting_pipeline(*args, **kwargs):
        pipe = original_pipeline(*args, **kwargs)
        original_pipe_execute = pipe.execute

        def counting_pipe_execute(*a, **kw):
            calls.append('PIPELINE')
            return original_pipe_execute(*a, **kw)

        pipe.execute = counting_pipe_execute
        return pipe

    conn.execute_command = counting_execute_command
    conn.pipeline = counting_pipeline
    return calls, lambda: (
        setattr(conn, 'execute_command', original_execute_command),
        setattr(conn, 'pipeline', original_pipeline))


def test_redis_write_and_delete_are_single_round_trips():
    """ Maintaining the index must not cost extra round trips.

    Both sequences are unconditional with no intermediate reads, so they
    batch into one pipeline each -- cheaper than the two round trips a
    write took before the index existed.
    """
    _require('redis')
    db = get_db('redis', '')
    folder = '/unittests/test_redis_round_trips/'
    try:
        # Warm the folder so mkdir_raw's own writes are not counted.
        db[folder + 'warm'] = 0

        calls, restore = _count_round_trips(db)
        try:
            db[folder + 'obj'] = 1
        finally:
            restore()
        assert calls.count('PIPELINE') == 1

        calls, restore = _count_round_trips(db)
        try:
            db.delete(folder + 'obj')
        finally:
            restore()
        assert calls.count('PIPELINE') == 1
    finally:
        db.rm_tree(folder)


def test_redis_index_keys_cannot_collide_with_an_object_path():
    """ Index keys are `kydb:`-prefixed, and every object path starts
    with '/', so an object can never land on one.

    The suffix form (`<folder>:mtime-index`) would have let an existing
    object at that path break the next write to the folder with a
    WRONGTYPE error.
    """
    _require('redis')
    db = get_db('redis', '')
    folder = '/unittests/test_redis_collide/'
    try:
        full = db._ensure_slashes(db._get_full_path(folder))[:-1]
        for key in (db._mtime_key(full), db._mtime_val_key(full),
                    db._ctime_key(full)):
            assert not key.startswith('/')
            assert key.startswith('kydb:')

        # An object deliberately named like the old index key still works.
        db[folder + 'obj:mtime-index'] = 1
        db[folder + 'obj'] = 2
        assert db[folder + 'obj:mtime-index'] == 1
        assert set(db.recent(folder)) == {'obj', 'obj:mtime-index'}
    finally:
        db.rm_tree(folder)
