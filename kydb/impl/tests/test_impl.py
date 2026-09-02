from datetime import datetime
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


@pytest.mark.parametrize('db_type', sorted(set(ALL_DB_TYPES) - {'dynamodb'}))
def test_folder_recent_raise_index_not_supported(db_type):
    db = get_db(db_type, '')

    with pytest.raises(kydb.IndexNotSupported):
        db.folder('/unittests/whatever')

    with pytest.raises(kydb.IndexNotSupported):
        db.recent('/unittests/whatever', limit=1)
