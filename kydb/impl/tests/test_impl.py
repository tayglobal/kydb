from datetime import datetime, timedelta, timezone
import decimal
import kydb
import pytest
import time
from tempfile import gettempdir
import os
from contextlib import contextmanager
from itertools import product


def get_test_db_types():
    env_val = os.environ.get('KYDB_TEST_DB_TYPES')
    if env_val:
        return [x.strip() for x in env_val.split(',') if x.strip()]
    return ['memory', 's3', 'redis', 'dynamodb', 'files', 'union']


ALL_DB_TYPES = get_test_db_types()

# ALL_DB_TYPES = ['dynamodb']
BASE_PATHS = ['', 'with_base_path']
# BASE_PATHS = ['with_base_path']

MARK_PARAMS = list(product(ALL_DB_TYPES, BASE_PATHS))

DB_URLS = {
    'memory': 'memory://cache001',
    's3': 's3://' + os.environ.get('KINYU_UNITTEST_S3_BUCKET', 'kydb-test'),
    'redis': 'redis://{}:6379'.format(
        os.environ.get('KINYU_UNITTEST_REDIS_HOST', 'localhost')),
    'dynamodb': 'dynamodb://' + os.environ.get('KINYU_UNITTEST_DYNAMODB', 'kydb-test-table'),
    'files': 'files:/' + gettempdir() + '/kydb_tests',
}

DB_URLS['union'] = DB_URLS['memory'] + ';' + DB_URLS['files']


def get_db(db_type, base_path):
    return kydb.connect(DB_URLS[db_type] + '/' + base_path)


@contextmanager
def list_dir_db(db_type: str, base_path: str):
    db = get_db(db_type, base_path)
    db['/unittests/test_list_dir/obj1'] = 1
    db['/unittests/test_list_dir/foo/obj2'] = 2
    db['/unittests/test_list_dir/foo/obj3'] = 2
    db['/unittests/test_list_dir/foo/obj4'] = 3
    db['/unittests/test_list_dir/foo/bar/obj5'] = 4
    try:
        yield db
    finally:
        db.rm_tree('/unittests/test_list_dir/')


@pytest.mark.parametrize('db_type,base_path', MARK_PARAMS)
def test_basic(db_type, base_path):
    db = get_db(db_type, base_path)
    key = '/unittests/test_basic/foo'
    db[key] = 123
    assert db[key] == 123
    assert db.read(key, reload=True) == 123
    assert 'test_basic/' in list(db.list_dir('/unittests'))
    db.rm_tree('/unittests/test_basic')
    assert not db.exists(key)


@pytest.mark.parametrize('db_type,base_path', MARK_PARAMS)
def test_slashes(db_type, base_path):
    db = get_db(db_type, base_path)
    db['unittests/test_slashes/foo'] = 123
    assert db.exists('/unittests/test_slashes/foo')
    assert db.exists('unittests/test_slashes/foo')

    db.delete('unittests/test_slashes/foo')
    assert not db.exists('/unittests/test_slashes/foo')
    assert not db.exists('unittests/test_slashes/foo')

    db['/unittests/test_slashes/foo'] = 123
    assert db.exists('/unittests/test_slashes/foo')
    assert db.exists('unittests/test_slashes/foo')

    db.rm_tree('/unittests/test_slashes')
    assert not db.exists('/unittests/test_slashes/foo')


@pytest.mark.parametrize('db_type,base_path', MARK_PARAMS)
def test_dict(db_type, base_path):
    db = get_db(db_type, base_path)
    key = '/unittests/test_dict/bar'
    val = {
        'my_int': 123,
        'my_float': 123.456,
        'my_str': 'hello',
        'my_list': [1, 2, 3],
        'my_datetime': datetime.now()
    }
    db[key] = val
    assert db[key] == val
    assert db.read(key, reload=True) == val
    db.rm_tree('/unittests/test_dict')


@pytest.mark.parametrize('db_type,base_path', MARK_PARAMS)
def test_errors(db_type, base_path):
    db = get_db(db_type, base_path)
    with pytest.raises(KeyError):
        db['does_not_exist']

    with pytest.raises(KeyError):
        db.delete('does_not_exist')


@pytest.mark.parametrize('db_type,base_path', MARK_PARAMS)
def test_bad_key(db_type, base_path):
    db = get_db(db_type, base_path)

    with pytest.raises(KeyError):
        db['.'] = 123

    with pytest.raises(KeyError):
        db['.foo'] = 123

    with pytest.raises(KeyError):
        db['/.foo'] = 123

    with pytest.raises(KeyError):
        db['/my-folder/.foo'] = 123

    with pytest.raises(KeyError):
        db['/my-folder/.another-folder/foo'] = 123


@pytest.mark.parametrize('db_type,base_path', MARK_PARAMS)
def test_mkdir(db_type, base_path):
    db = get_db(db_type, base_path)

    db.mkdir('/unittests/test_mkdir/foo')
    assert db.ls('/unittests/test_mkdir/') == ['foo/']
    assert db.is_dir('/unittests/test_mkdir')
    assert db.ls('/unittests/test_mkdir/foo') == []
    assert db.is_dir('/unittests/test_mkdir/foo')
    db.rm_tree('/unittests/test_mkdir')


@pytest.mark.parametrize('db_type',
                         set(ALL_DB_TYPES) - set(['memory', 'union']))
def test_with_basepath(db_type: str):
    base_path = 'unittests/my/base/path'
    db = get_db(db_type, base_path)
    key = '/apple'
    db[key] = 123
    assert db[key] == 123
    assert db.read(key, reload=True) == 123

    # Going to the root and including the base_path should be equivalent
    db2 = kydb.connect(DB_URLS[db_type])
    assert db2.read(base_path + key, reload=True) == 123

    db2.rm_tree('/unittests/my')


@pytest.mark.parametrize('db_type,base_path', MARK_PARAMS)
def test_rmdir_success(db_type, base_path):
    db = get_db(db_type, base_path)
    db.mkdir('/unittests_rmdir/foo')
    assert not db.exists('/unittests_rmdir/foo')
    assert db.is_dir('/unittests_rmdir')
    assert db.is_dir('/unittests_rmdir/foo')
    db.rmdir('/unittests_rmdir/foo')
    assert not db.is_dir('/unittests_rmdir/foo')

    db.rmdir('/unittests_rmdir')
    assert not db.is_dir('/unittests_rmdir')


@pytest.mark.parametrize('db_type,base_path', MARK_PARAMS)
def test_rmdir_not_empty(db_type, base_path):
    db = get_db(db_type, base_path)

    db['/unittests/test_rmdir_not_empty/foo'] = 123

    with pytest.raises(KeyError):
        db.rmdir('/unittests/test_rmdir_not_empty')


@pytest.mark.parametrize('db_type,base_path', MARK_PARAMS)
def test_rmdir_error(db_type, base_path):
    db = get_db(db_type, base_path)
    with pytest.raises(KeyError):
        db.rmdir('')

    with pytest.raises(KeyError):
        db.rmdir('.')

    with pytest.raises(KeyError):
        db.rmdir('/')

    with pytest.raises(KeyError):
        db.rmdir('does_not_exist')

    with pytest.raises(KeyError):
        db.rmdir('/does_not_exist')


@pytest.mark.parametrize('db_type,base_path', MARK_PARAMS)
def test_list_dir_with_subdir(db_type, base_path):
    with list_dir_db(db_type, base_path) as db:
        assert 'unittests/' in list(db.list_dir(''))
        assert 'unittests/' in list(db.list_dir('/'))
        assert set(['foo/', 'obj1']) \
            == set(db.list_dir('/unittests/test_list_dir'))
        assert set(['bar/', 'obj2', 'obj3', 'obj4']) == \
            set(db.list_dir('/unittests/test_list_dir/foo'))
        assert ['obj5'] == list(db.list_dir(
            '/unittests/test_list_dir/foo/bar'))


@pytest.mark.parametrize('db_type,base_path', MARK_PARAMS)
def test_list_dir_no_subdir(db_type, base_path):
    with list_dir_db(db_type, base_path) as db:
        assert 'unittests/' not in list(db.list_dir('', False))
        assert 'unittests/' not in list(db.list_dir('/', False))
        assert ['obj1'] == list(db.list_dir('/unittests/test_list_dir', False))
        assert set(['obj2', 'obj3', 'obj4']) == \
            set(db.list_dir('/unittests/test_list_dir/foo', False))
        assert ['obj5'] == list(db.list_dir(
            '/unittests/test_list_dir/foo/bar', False))


@pytest.mark.parametrize('db_type,base_path', MARK_PARAMS)
def test_pagination(db_type, base_path):
    with list_dir_db(db_type, base_path) as db:
        assert 'unittests/' in list(db.list_dir(''))
        assert 'unittests/' in list(db.list_dir('/'))
        assert set(['foo/', 'obj1']) == set(
            db.list_dir('/unittests/test_list_dir', page_size=1))
        assert set(['bar/', 'obj2', 'obj3', 'obj4']) == \
            set(db.list_dir('/unittests/test_list_dir/foo', page_size=1))
        assert ['obj5'] == list(db.list_dir(
            '/unittests/test_list_dir/foo/bar', page_size=1))


# --- DynamoDB write-path tests for the additional-index feature (stage 1) ---
#
# These assert directly against the raw table/GSI, since the query API
# (db.folder(...) / db.recent(...)) does not exist yet.

def _require_dynamodb():
    if 'dynamodb' not in ALL_DB_TYPES:
        pytest.skip('dynamodb not in KYDB_TEST_DB_TYPES')


def test_dynamodb_write_sets_mtime_and_ctime():
    _require_dynamodb()
    db = get_db('dynamodb', '')
    key = '/unittests/test_mtime_ctime/obj'
    try:
        db[key] = 123
        # contents must still round-trip correctly as Binary through
        # update_item
        assert db.read(key, reload=True) == 123

        full_path = db._get_full_path(key)
        item = db.table.get_item(Key={'path': full_path})['Item']

        assert 'mtime' in item
        assert 'ctime' in item
        # on first write mtime and ctime are set to the same timestamp
        assert item['mtime'] == item['ctime']
    finally:
        db.rm_tree('/unittests/test_mtime_ctime')


def test_dynamodb_rewrite_bumps_mtime_preserves_ctime():
    _require_dynamodb()
    db = get_db('dynamodb', '')
    key = '/unittests/test_mtime_ctime_rewrite/obj'
    try:
        db[key] = 1
        full_path = db._get_full_path(key)
        item1 = db.table.get_item(Key={'path': full_path})['Item']

        # ensure a distinct time.time_ns() value on the rewrite
        time.sleep(0.001)
        db[key] = 2
        item2 = db.table.get_item(Key={'path': full_path})['Item']

        assert item2['mtime'] > item1['mtime']
        assert item2['ctime'] == item1['ctime']
    finally:
        db.rm_tree('/unittests/test_mtime_ctime_rewrite')


def test_dynamodb_folder_meta_has_no_mtime_or_ctime():
    _require_dynamodb()
    db = get_db('dynamodb', '')
    try:
        db.mkdir('/unittests/test_folder_meta_no_mtime/foo')
        meta_path = db._get_full_path(
            db._folder_meta_path('/unittests/test_folder_meta_no_mtime/foo/'))
        item = db.table.get_item(Key={'path': meta_path})['Item']

        assert 'mtime' not in item
        assert 'ctime' not in item
        assert 'folder' in item
        assert 'contents' in item
    finally:
        db.rm_tree('/unittests/test_folder_meta_no_mtime')


# --- Query API / DynamoDB read-path tests for the additional-index
# feature (stage 2): db.folder(...) / db.recent(...).
#
# since() boundary: inclusive (>= ts) -- matches the plan (verified as
# DynamoDB Key(...).gte() behaviour) and is tested explicitly below.

@pytest.mark.parametrize('base_path', BASE_PATHS)
def test_dynamodb_recent_newest_first(base_path):
    _require_dynamodb()
    db = get_db('dynamodb', base_path)
    folder = '/unittests/test_recent_order/'
    try:
        for name in ('obj1', 'obj2', 'obj3'):
            db[folder + name] = name
            time.sleep(0.001)

        assert list(db.recent(folder, limit=10)) == ['obj3', 'obj2', 'obj1']
        assert list(db.folder(folder).by('mtime').desc()) == \
            ['obj3', 'obj2', 'obj1']
        assert list(db.folder(folder).by('mtime').asc()) == \
            ['obj1', 'obj2', 'obj3']
    finally:
        db.rm_tree(folder)


@pytest.mark.parametrize('base_path', BASE_PATHS)
def test_dynamodb_recent_limit_exact(base_path):
    _require_dynamodb()
    db = get_db('dynamodb', base_path)
    folder = '/unittests/test_recent_limit/'
    try:
        for i in range(5):
            db[folder + f'obj{i}'] = i
            time.sleep(0.001)

        assert list(db.recent(folder, limit=2)) == ['obj4', 'obj3']
        assert len(list(db.folder(folder).by('mtime').desc().limit(3))) == 3
    finally:
        db.rm_tree(folder)


def test_dynamodb_recent_limit_does_not_overfetch_pages():
    _require_dynamodb()
    db = get_db('dynamodb', '')
    folder = '/unittests/test_recent_limit_pages/'
    try:
        for i in range(5):
            db[folder + f'obj{i}'] = i
            time.sleep(0.001)

        calls = []
        original_query = db.table.query

        def counting_query(**kwargs):
            calls.append(kwargs)
            return original_query(**kwargs)

        db.table.query = counting_query
        try:
            result = list(db.folder(folder).by('mtime').desc().limit(2))
        finally:
            del db.table.query

        assert result == ['obj4', 'obj3']
        # A limit() must not fetch more DynamoDB pages than it needs: one
        # page, with the Limit sent to DynamoDB bounded by the request.
        assert len(calls) == 1
        assert calls[0]['Limit'] == 2
    finally:
        db.rm_tree(folder)


def test_dynamodb_since_boundary_is_inclusive():
    _require_dynamodb()
    db = get_db('dynamodb', '')
    folder = '/unittests/test_since_boundary/'
    try:
        db[folder + 'obj1'] = 1
        time.sleep(0.001)
        db[folder + 'obj2'] = 2
        time.sleep(0.001)
        db[folder + 'obj3'] = 3

        entries = list(db.folder(folder).by('mtime').asc().entries())
        mtimes = {e.key: e.mtime for e in entries}

        # since() is inclusive: the boundary item itself is included.
        assert set(db.folder(folder).by('mtime').since(mtimes['obj2'])) == \
            {'obj2', 'obj3'}

        # A timestamp just below obj2's mtime still includes it...
        assert set(
            db.folder(folder).by('mtime').since(mtimes['obj2'] - 1)) == \
            {'obj2', 'obj3'}

        # ...but just above it excludes it.
        assert set(
            db.folder(folder).by('mtime').since(mtimes['obj2'] + 1)) == \
            {'obj3'}
    finally:
        db.rm_tree(folder)


def test_dynamodb_entries_are_int_not_decimal():
    _require_dynamodb()
    db = get_db('dynamodb', '')
    folder = '/unittests/test_entries_int/'
    try:
        db[folder + 'obj1'] = 1

        entries = list(db.folder(folder).by('mtime').entries())
        assert len(entries) == 1
        entry = entries[0]
        assert entry.key == 'obj1'
        assert isinstance(entry.mtime, int)
        assert not isinstance(entry.mtime, decimal.Decimal)
        assert isinstance(entry.ctime, int)
        assert not isinstance(entry.ctime, decimal.Decimal)
        assert entry.mtime == entry.ctime
    finally:
        db.rm_tree(folder)


def test_dynamodb_recent_excludes_directories():
    _require_dynamodb()
    db = get_db('dynamodb', '')
    folder = '/unittests/test_recent_no_dirs/'
    try:
        db[folder + 'obj1'] = 1
        db.mkdir(folder + 'subfolder')
        db[folder + 'subfolder/obj2'] = 2

        names = list(db.folder(folder).by('mtime').desc())
        assert names == ['obj1']
        assert 'subfolder' not in names
        assert 'subfolder/' not in names
    finally:
        db.rm_tree(folder)


def test_dynamodb_recent_delete_removes_entry():
    _require_dynamodb()
    db = get_db('dynamodb', '')
    folder = '/unittests/test_recent_delete/'
    try:
        db[folder + 'obj1'] = 1
        db[folder + 'obj2'] = 2

        assert set(db.folder(folder).by('mtime')) == {'obj1', 'obj2'}

        db.delete(folder + 'obj1')

        assert set(db.folder(folder).by('mtime')) == {'obj2'}
    finally:
        db.rm_tree(folder)


def test_dynamodb_folder_items_returns_values():
    _require_dynamodb()
    db = get_db('dynamodb', '')
    folder = '/unittests/test_folder_items/'
    try:
        db[folder + 'obj1'] = {'a': 1}
        db[folder + 'obj2'] = {'b': 2}

        items = dict(db.folder(folder).by('mtime').items())
        assert items == {'obj1': {'a': 1}, 'obj2': {'b': 2}}
    finally:
        db.rm_tree(folder)


def test_dynamodb_recent_ties_paginate_without_dropping_or_duplicating():
    # Regression test for LastEvaluatedKey continuation across a run of
    # identical `mtime` values. §12 of additional_index_plan.md already
    # verified against real Moto that DynamoDB's LastEvaluatedKey on this
    # GSI carries {folder, mtime, path}, so ties page safely -- this locks
    # in that _raw_query's own pagination loop preserves that property,
    # using a synthetic multi-page response (Moto keeps 5 tiny items on
    # one real page, so a fake is needed to force >1 page here).
    _require_dynamodb()
    db = get_db('dynamodb', '')
    folder = '/unittests/test_recent_ties/'
    full_folder = db._get_full_path(folder)

    all_items = [
        {
            'path': f'{full_folder}obj{i}',
            'folder': full_folder,
            'mtime': decimal.Decimal(1000),
        }
        for i in range(5)
    ]

    def fake_paginated_query(**kwargs):
        start = 0
        esk = kwargs.get('ExclusiveStartKey')
        if esk is not None:
            for idx, it in enumerate(all_items):
                if it['path'] == esk['path']:
                    start = idx + 1
                    break
        page = all_items[start:start + 2]
        result = {'Items': page}
        if start + 2 < len(all_items):
            last = page[-1]
            result['LastEvaluatedKey'] = {
                'path': last['path'],
                'folder': last['folder'],
                'mtime': last['mtime'],
            }
        return result

    db.table.query = fake_paginated_query
    try:
        names = list(db.folder(folder).by('mtime').desc())
    finally:
        del db.table.query

    assert len(names) == 5
    assert sorted(names) == ['obj0', 'obj1', 'obj2', 'obj3', 'obj4']


def test_dynamodb_recent_rewrite_moves_to_front():
    _require_dynamodb()
    db = get_db('dynamodb', '')
    folder = '/unittests/test_recent_rewrite/'
    try:
        db[folder + 'obj1'] = 1
        time.sleep(0.001)
        db[folder + 'obj2'] = 2

        assert list(db.recent(folder, limit=10)) == ['obj2', 'obj1']

        time.sleep(0.001)
        db[folder + 'obj1'] = 'updated'

        assert list(db.recent(folder, limit=10)) == ['obj1', 'obj2']
    finally:
        db.rm_tree(folder)


def test_dynamodb_folder_query_chaining_does_not_mutate():
    _require_dynamodb()
    db = get_db('dynamodb', '')
    folder = '/unittests/test_folder_query_immutable/'
    try:
        db[folder + 'obj1'] = 1
        time.sleep(0.001)
        db[folder + 'obj2'] = 2

        base = db.folder(folder).by('mtime')
        newest_first = base.desc()
        oldest_first = base.asc()

        # Branching from `base` after building `newest_first` must not
        # have mutated it -- each chained call returns a fresh query.
        assert list(newest_first) == ['obj2', 'obj1']
        assert list(oldest_first) == ['obj1', 'obj2']
        # `base` itself carries no explicit order (defaults to ascending)
        # and is unaffected by either branch.
        assert list(base) == ['obj1', 'obj2']
    finally:
        db.rm_tree(folder)


@pytest.mark.parametrize(
    'db_type', sorted(set(ALL_DB_TYPES) - {'dynamodb', 'redis'}))
def test_folder_recent_raise_index_not_supported(db_type):
    """Without allow_scan=True, every backend other than DynamoDB (native
    index) and Redis (native sorted set) must still raise -- memory,
    files and s3 only support recency via the opt-in scan-and-sort
    fallback, and union only via its members. HTTP/HTTPS (not in
    ALL_DB_TYPES by default) stay unsupported outright, with no
    allow_scan escape hatch at all -- see test_http_recent_unsupported.
    """
    db = get_db(db_type, '')

    with pytest.raises(kydb.IndexNotSupported):
        db.folder('/unittests/whatever')

    with pytest.raises(kydb.IndexNotSupported):
        db.recent('/unittests/whatever', limit=1)


@pytest.mark.parametrize(
    'db_type', sorted(set(ALL_DB_TYPES) & {'redis'}))
def test_folder_recent_native_no_raise_without_allow_scan(db_type):
    """Redis has a genuinely native ordering index (a per-folder sorted
    set), so unlike memory/files/s3 it does NOT require allow_scan=True
    -- folder()/recent() must not raise even without it.
    """
    db = get_db(db_type, '')
    db.folder('/unittests/whatever')  # must not raise
    db.recent('/unittests/whatever', limit=1)  # must not raise


def test_dynamodb_entries_does_not_refetch_per_item():
    """entries() must be served entirely from the index page.

    ``ctime`` is projected into ``folder-time-index`` (INCLUDE), so
    exposing ``Entry.ctime`` must not cost one extra read per row --
    that would make entries() N+1 in the size of the result.
    """
    _require_dynamodb()
    db = get_db('dynamodb', '')
    folder = '/unittests/test_entries_no_refetch/'
    try:
        for i in range(5):
            db[folder + f'obj{i}'] = i
            time.sleep(0.001)

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
        try:
            entries = list(db.folder(folder).by('mtime').desc().entries())
        finally:
            del db.table.query
            del db.table.get_item

        assert [e.key for e in entries] == [
            'obj4', 'obj3', 'obj2', 'obj1', 'obj0']
        # One index query, and crucially zero per-item reads.
        assert len(query_calls) == 1
        assert get_item_calls == []
        # ctime still arrives, as int rather than Decimal.
        assert all(isinstance(e.ctime, int) for e in entries)
        assert all(isinstance(e.mtime, int) for e in entries)
        assert all(e.ctime == e.mtime for e in entries)
    finally:
        db.rm_tree(folder)


# --- Wrappers (UnionDB/CacheDB) and other backends for the
# additional-index feature (stage 3): allow_scan=True client-side
# scan-and-sort fallback for memory/files/s3, a native sorted-set index
# for Redis, UnionDB.folder() merging member dbs, and CacheDB.folder()
# delegating to persist_db.
#
# allow_scan lives on both entry points, kept consistent:
#   db.folder(folder, allow_scan=True)
#   db.recent(folder, limit=n, allow_scan=True)
# Redis is a genuine native index and ignores/does not require it.

# Backends whose folder()/recent() only work via the client-side
# allow_scan=True scan-and-sort fallback.
SCAN_RECENT_DB_TYPES = [t for t in ('memory', 'files') if t in ALL_DB_TYPES]
# Backends with a genuinely native ordering index -- no allow_scan
# needed.
NATIVE_RECENT_DB_TYPES = [t for t in ('redis',) if t in ALL_DB_TYPES]
RECENT_DB_TYPES = SCAN_RECENT_DB_TYPES + NATIVE_RECENT_DB_TYPES
RECENT_MARK_PARAMS = list(product(RECENT_DB_TYPES, BASE_PATHS))
# Backends that separately track ctime (if_not_exists-style) rather than
# falling back to mtime.
CTIME_TRACKING_DB_TYPES = [
    t for t in ('memory', 'redis') if t in RECENT_DB_TYPES]
# Backends with no stored ctime at all: mtime is reused for ctime, so a
# rewrite bumps "ctime" too. Documented, not engineered around -- see
# MemoryFolderQuery/FileFolderQuery/S3FolderQuery docstrings.
CTIME_FALLBACK_DB_TYPES = [t for t in ('files',) if t in RECENT_DB_TYPES]


def _recent_kwargs(db_type):
    """allow_scan is required for the client-side scan-and-sort fallback
    (memory/files/s3); redis is a native index and ignores it.
    """
    return {'allow_scan': True} if db_type in SCAN_RECENT_DB_TYPES else {}


@pytest.mark.parametrize('db_type,base_path', RECENT_MARK_PARAMS)
def test_recent_newest_first(db_type, base_path):
    db = get_db(db_type, base_path)
    folder = '/unittests/test_recent_newest_first/'
    kwargs = _recent_kwargs(db_type)
    try:
        for name in ('obj1', 'obj2', 'obj3'):
            db[folder + name] = name
            time.sleep(0.01)

        assert list(db.recent(folder, limit=10, **kwargs)) == \
            ['obj3', 'obj2', 'obj1']
        assert list(db.folder(folder, **kwargs).by('mtime').desc()) == \
            ['obj3', 'obj2', 'obj1']
        assert list(db.folder(folder, **kwargs).by('mtime').asc()) == \
            ['obj1', 'obj2', 'obj3']
    finally:
        db.rm_tree(folder)


@pytest.mark.parametrize('db_type,base_path', RECENT_MARK_PARAMS)
def test_recent_limit_exact(db_type, base_path):
    db = get_db(db_type, base_path)
    folder = '/unittests/test_recent_limit_exact/'
    kwargs = _recent_kwargs(db_type)
    try:
        for i in range(5):
            db[folder + f'obj{i}'] = i
            time.sleep(0.01)

        assert list(db.recent(folder, limit=2, **kwargs)) == \
            ['obj4', 'obj3']
        assert len(list(
            db.folder(folder, **kwargs).by('mtime').desc().limit(3))) == 3
    finally:
        db.rm_tree(folder)


@pytest.mark.parametrize('db_type', RECENT_DB_TYPES)
def test_recent_excludes_directories(db_type):
    db = get_db(db_type, '')
    folder = '/unittests/test_recent_excludes_directories/'
    kwargs = _recent_kwargs(db_type)
    try:
        db[folder + 'obj1'] = 1
        db.mkdir(folder + 'subfolder')
        db[folder + 'subfolder/obj2'] = 2

        names = list(db.folder(folder, **kwargs).by('mtime').desc())
        assert names == ['obj1']
        assert 'subfolder' not in names
        assert 'subfolder/' not in names
    finally:
        db.rm_tree(folder)


@pytest.mark.parametrize('db_type', RECENT_DB_TYPES)
def test_recent_delete_removes_entry(db_type):
    db = get_db(db_type, '')
    folder = '/unittests/test_recent_delete_removes_entry/'
    kwargs = _recent_kwargs(db_type)
    try:
        db[folder + 'obj1'] = 1
        db[folder + 'obj2'] = 2

        assert set(db.folder(folder, **kwargs).by('mtime')) == \
            {'obj1', 'obj2'}

        db.delete(folder + 'obj1')

        assert set(db.folder(folder, **kwargs).by('mtime')) == {'obj2'}
    finally:
        db.rm_tree(folder)


@pytest.mark.parametrize('db_type', RECENT_DB_TYPES)
def test_recent_items_returns_values(db_type):
    db = get_db(db_type, '')
    folder = '/unittests/test_recent_items/'
    kwargs = _recent_kwargs(db_type)
    try:
        db[folder + 'obj1'] = {'a': 1}
        db[folder + 'obj2'] = {'b': 2}

        items = dict(db.folder(folder, **kwargs).by('mtime').items())
        assert items == {'obj1': {'a': 1}, 'obj2': {'b': 2}}
    finally:
        db.rm_tree(folder)


@pytest.mark.parametrize('db_type', RECENT_DB_TYPES)
def test_recent_entries_first_write_mtime_equals_ctime(db_type):
    db = get_db(db_type, '')
    folder = '/unittests/test_recent_entries/'
    kwargs = _recent_kwargs(db_type)
    try:
        db[folder + 'obj1'] = 1

        entries = list(db.folder(folder, **kwargs).by('mtime').entries())
        assert len(entries) == 1
        entry = entries[0]
        assert entry.key == 'obj1'
        assert isinstance(entry.mtime, int)
        assert isinstance(entry.ctime, int)
        assert entry.mtime == entry.ctime
    finally:
        db.rm_tree(folder)


@pytest.mark.parametrize('db_type', CTIME_TRACKING_DB_TYPES)
def test_recent_rewrite_bumps_mtime_preserves_ctime(db_type):
    db = get_db(db_type, '')
    folder = '/unittests/test_recent_rewrite_ctime/'
    kwargs = _recent_kwargs(db_type)
    try:
        db[folder + 'obj1'] = 1
        entry1 = list(
            db.folder(folder, **kwargs).by('mtime').entries())[0]

        time.sleep(0.01)
        db[folder + 'obj1'] = 2
        entry2 = list(
            db.folder(folder, **kwargs).by('mtime').entries())[0]

        assert entry2.mtime > entry1.mtime
        assert entry2.ctime == entry1.ctime
    finally:
        db.rm_tree(folder)


@pytest.mark.parametrize('db_type', CTIME_FALLBACK_DB_TYPES)
def test_recent_ctime_falls_back_to_mtime_on_rewrite(db_type):
    """Files (and S3, tested separately below) have no stored ctime
    distinct from mtime -- documented fallback, not a bug: a rewrite
    bumps both together.
    """
    db = get_db(db_type, '')
    folder = '/unittests/test_recent_ctime_fallback/'
    kwargs = _recent_kwargs(db_type)
    try:
        db[folder + 'obj1'] = 1
        entry1 = list(
            db.folder(folder, **kwargs).by('mtime').entries())[0]
        assert entry1.mtime == entry1.ctime

        time.sleep(0.01)
        db[folder + 'obj1'] = 2
        entry2 = list(
            db.folder(folder, **kwargs).by('mtime').entries())[0]

        assert entry2.mtime > entry1.mtime
        assert entry2.mtime == entry2.ctime
    finally:
        db.rm_tree(folder)


@pytest.mark.parametrize('db_type', RECENT_DB_TYPES)
def test_recent_rewrite_moves_to_front(db_type):
    db = get_db(db_type, '')
    folder = '/unittests/test_recent_rewrite/'
    kwargs = _recent_kwargs(db_type)
    try:
        db[folder + 'obj1'] = 1
        time.sleep(0.01)
        db[folder + 'obj2'] = 2

        assert list(db.recent(folder, limit=10, **kwargs)) == \
            ['obj2', 'obj1']

        time.sleep(0.01)
        db[folder + 'obj1'] = 'updated'

        assert list(db.recent(folder, limit=10, **kwargs)) == \
            ['obj1', 'obj2']
    finally:
        db.rm_tree(folder)


# --- S3: LastModified is only second-resolution (both on real S3 and
# under Moto -- verified directly against Moto during design), too
# coarse to order against short wall-clock sleeps. Ordering/limit tests
# therefore patch list_objects_v2's reported LastModified rather than
# relying on real elapsed time; membership/exclusion/deletion tests
# don't depend on ordering and use real timestamps.

def _require_s3():
    if 's3' not in ALL_DB_TYPES:
        pytest.skip('s3 not in KYDB_TEST_DB_TYPES')


@contextmanager
def _s3_fake_last_modified(db, fake_times: dict):
    """Patch db.s3.list_objects_v2 to report the given per-(full S3 key)
    LastModified overrides, leaving everything else (including which
    keys are actually returned) untouched.
    """
    original_list = db.s3.list_objects_v2

    def fake_list_objects_v2(**kwargs):
        res = original_list(**kwargs)
        for item in res.get('Contents', []):
            if item['Key'] in fake_times:
                item['LastModified'] = fake_times[item['Key']]
        return res

    db.s3.list_objects_v2 = fake_list_objects_v2
    try:
        yield
    finally:
        del db.s3.list_objects_v2


def test_s3_recent_newest_first_and_limit():
    _require_s3()
    db = get_db('s3', '')
    folder = '/unittests/test_s3_recent_order/'
    prefix = db._get_full_path(folder)[1:]
    try:
        for name in ('obj1', 'obj2', 'obj3'):
            db[folder + name] = name

        base = datetime(2024, 1, 1, tzinfo=timezone.utc)
        fake_times = {
            prefix + 'obj1': base,
            prefix + 'obj2': base + timedelta(seconds=1),
            prefix + 'obj3': base + timedelta(seconds=2),
        }
        with _s3_fake_last_modified(db, fake_times):
            assert list(db.recent(folder, limit=10, allow_scan=True)) == \
                ['obj3', 'obj2', 'obj1']
            assert list(db.recent(folder, limit=2, allow_scan=True)) == \
                ['obj3', 'obj2']
            assert list(
                db.folder(folder, allow_scan=True).by('mtime').asc()) == \
                ['obj1', 'obj2', 'obj3']
    finally:
        db.rm_tree(folder)


@pytest.mark.parametrize('base_path', BASE_PATHS)
def test_s3_recent_newest_first_with_base_path(base_path):
    _require_s3()
    db = get_db('s3', base_path)
    folder = '/unittests/test_s3_recent_order_bp/'
    prefix = db._get_full_path(folder)[1:]
    try:
        for name in ('obj1', 'obj2'):
            db[folder + name] = name

        base = datetime(2024, 1, 1, tzinfo=timezone.utc)
        fake_times = {
            prefix + 'obj1': base,
            prefix + 'obj2': base + timedelta(seconds=1),
        }
        with _s3_fake_last_modified(db, fake_times):
            assert list(db.recent(folder, limit=10, allow_scan=True)) == \
                ['obj2', 'obj1']
    finally:
        db.rm_tree(folder)


def test_s3_recent_excludes_directories():
    _require_s3()
    db = get_db('s3', '')
    folder = '/unittests/test_s3_recent_no_dirs/'
    try:
        db[folder + 'obj1'] = 1
        db.mkdir(folder + 'subfolder')
        db[folder + 'subfolder/obj2'] = 2

        names = list(db.folder(folder, allow_scan=True).by('mtime').desc())
        assert names == ['obj1']
        assert 'subfolder' not in names
        assert 'subfolder/' not in names
    finally:
        db.rm_tree(folder)


def test_s3_recent_delete_removes_entry():
    _require_s3()
    db = get_db('s3', '')
    folder = '/unittests/test_s3_recent_delete/'
    try:
        db[folder + 'obj1'] = 1
        db[folder + 'obj2'] = 2

        assert set(db.folder(folder, allow_scan=True).by('mtime')) == \
            {'obj1', 'obj2'}

        db.delete(folder + 'obj1')

        assert set(db.folder(folder, allow_scan=True).by('mtime')) == \
            {'obj2'}
    finally:
        db.rm_tree(folder)


# --- UnionDB: heapq.merge over the per-db (already sorted) FolderQuery
# generators, deduplicating by name with front-db-wins -- NOT
# UnionDB.list_dir's set-union, which destroys ordering entirely. This
# is the item the plan (§8) flags as most likely to be got wrong.

def test_union_recent_merge_order_across_two_dbs():
    db1 = kydb.connect('memory://union_recent_order_db1')
    db2 = kydb.connect('memory://union_recent_order_db2')
    union = kydb.connect(
        'memory://union_recent_order_db1;memory://union_recent_order_db2')
    folder = '/unittests/test_union_recent_order/'
    try:
        # Interleave writes across the two member dbs so mtime order
        # does not simply match "all of db1 then all of db2" -- that
        # would pass even with the broken set-union approach.
        db2[folder + 'b1'] = 'b1'
        time.sleep(0.01)
        db1[folder + 'a1'] = 'a1'
        time.sleep(0.01)
        db2[folder + 'b2'] = 'b2'
        time.sleep(0.01)
        db1[folder + 'a2'] = 'a2'

        assert list(union.recent(folder, limit=10, allow_scan=True)) == \
            ['a2', 'b2', 'a1', 'b1']
        oldest_first = union.folder(folder, allow_scan=True).by('mtime')
        assert list(oldest_first.asc()) == ['b1', 'a1', 'b2', 'a2']
        # limit() must apply to the merged/deduped stream, not per-db.
        assert list(union.recent(folder, limit=2, allow_scan=True)) == \
            ['a2', 'b2']
    finally:
        db1.rm_tree(folder)
        db2.rm_tree(folder)


def test_union_recent_dedup_front_db_wins():
    db1 = kydb.connect('memory://union_recent_dedup_db1')
    db2 = kydb.connect('memory://union_recent_dedup_db2')
    union = kydb.connect(
        'memory://union_recent_dedup_db1;memory://union_recent_dedup_db2')
    folder = '/unittests/test_union_recent_dedup/'
    try:
        # Same key written to both member dbs, db2 (the back db) second
        # -- so db2's mtime is the more recent one. A naive "keep
        # whichever entry appears first in a desc-mtime merged stream"
        # would wrongly pick db2's. front-db-wins must pick db1's
        # regardless of which db's write is newer.
        db1[folder + 'shared'] = 'from-db1'
        time.sleep(0.01)
        db2[folder + 'shared'] = 'from-db2'

        entries = list(
            union.folder(folder, allow_scan=True).by('mtime').entries())
        assert len(entries) == 1
        assert entries[0].key == 'shared'

        db1_entry = list(
            db1.folder(folder, allow_scan=True).by('mtime').entries())[0]
        # The surviving entry's mtime is db1's own -- not db2's more
        # recent one.
        assert entries[0].mtime == db1_entry.mtime

        items = dict(
            union.folder(folder, allow_scan=True).by('mtime').items())
        assert items == {'shared': 'from-db1'}
    finally:
        db1.rm_tree(folder)
        db2.rm_tree(folder)


def test_union_recent_all_scan_members_supported_with_allow_scan():
    db = get_db('union', '')  # memory;files, per DB_URLS
    folder = '/unittests/test_union_recent_scan/'
    try:
        db.dbs[0][folder + 'obj1'] = 1
        time.sleep(0.01)
        db.dbs[1][folder + 'obj2'] = 2

        names = list(db.recent(folder, limit=10, allow_scan=True))
        assert names == ['obj2', 'obj1']
    finally:
        db.rm_tree(folder)


def test_union_recent_partial_support_uses_supporting_members_only():
    _require_dynamodb()
    dyn = get_db('dynamodb', '')
    mem = kydb.connect('memory://union_recent_partial_mem')
    union = kydb.connect(
        DB_URLS['dynamodb'] + ';memory://union_recent_partial_mem')
    folder = '/unittests/test_union_recent_partial/'
    try:
        dyn[folder + 'd1'] = 'd1'
        mem[folder + 'm1'] = 'm1'

        # Default (no allow_scan): dynamodb supports natively, memory
        # doesn't -- union must still work, using only dynamodb's
        # entries, not raise.
        names = list(union.folder(folder).by('mtime'))
        assert names == ['d1']

        # allow_scan=True: both members now contribute.
        names_scan = set(union.folder(folder, allow_scan=True).by('mtime'))
        assert names_scan == {'d1', 'm1'}
    finally:
        dyn.rm_tree(folder)
        mem.rm_tree(folder)


# --- CacheDB: recency delegates entirely to persist_db (the cache_db
# only holds what has been individually read, so it cannot answer a
# folder-wide question) -- matching CacheDB.list_dir.

def test_cache_db_recent_delegates_to_persist_db():
    db = kydb.connect(
        'memory://cache_recent_cache|memory://cache_recent_persist')
    folder = '/unittests/test_cache_recent/'
    try:
        for name in ('obj1', 'obj2'):
            db[folder + name] = name
            time.sleep(0.01)

        query = db.folder(folder, allow_scan=True)
        assert query._db is db.persist_db

        names = list(db.recent(folder, limit=10, allow_scan=True))
        assert names == ['obj2', 'obj1']

        items = dict(
            db.folder(folder, allow_scan=True).by('mtime').items())
        assert items == {'obj1': 'obj1', 'obj2': 'obj2'}
    finally:
        db.rm_tree(folder)


def test_cache_db_recent_raises_without_allow_scan():
    db = kydb.connect(
        'memory://cache_recent_raise_cache|'
        'memory://cache_recent_raise_persist')

    with pytest.raises(kydb.IndexNotSupported):
        db.folder('/unittests/whatever')

    with pytest.raises(kydb.IndexNotSupported):
        db.recent('/unittests/whatever', limit=1)


@pytest.mark.parametrize('db_type', sorted(
    set(ALL_DB_TYPES) & {'dynamodb', 'redis', 'memory'}))
def test_recent_mtime_is_exact_nanoseconds(db_type):
    """mtime must be exact nanoseconds on every backend that reports it.

    Redis is the interesting case: a sorted-set score is a double and is
    only exact to 2**53, so a ~19-digit nanosecond timestamp does not
    survive being stored as one. The score orders the folder, but the
    exact value must come from elsewhere -- if it ever regresses to being
    read back off the score, the timestamp silently loses precision.
    """
    db = get_db(db_type, '')
    folder = '/unittests/test_mtime_exact_ns/'
    try:
        before = time.time_ns()
        db[folder + 'obj1'] = 1
        after = time.time_ns()

        # allow_scan is a no-op on the natively-indexed backends and is
        # what Memory requires; passing it uniformly keeps this test
        # about timestamp precision rather than about capability.
        query = db.folder(folder, allow_scan=True)
        entry = list(query.by('mtime').desc().entries())[0]

        assert isinstance(entry.mtime, int)
        assert isinstance(entry.ctime, int)
        # Nanoseconds, not milliseconds/seconds: bracketed by the write.
        assert before <= entry.mtime <= after
        assert before <= entry.ctime <= after
        # Guard the actual failure mode: a value that has been through a
        # double would not survive this comparison.
        assert int(float(entry.mtime)) != entry.mtime or entry.mtime < 2 ** 53
    finally:
        db.rm_tree(folder)
