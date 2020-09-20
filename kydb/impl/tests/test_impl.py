from datetime import datetime
import kydb
import pytest
from tempfile import gettempdir
import os
from contextlib import contextmanager
from itertools import product


def is_automated_test() -> bool:
    return os.environ.get('IS_AUTOMATED_UNITTEST')


ALL_DB_TYPES = ['memory', 'files', 'union'] \
    if is_automated_test() \
    else ['memory', 's3', 'redis', 'dynamodb', 'files', 'union']

# ALL_DB_TYPES = ['dynamodb']
BASE_PATHS = ['', 'with_base_path']
# BASE_PATHS = ['with_base_path']

MARK_PARAMS = list(product(ALL_DB_TYPES, BASE_PATHS))

DB_URLS = {
    'memory': 'memory://cache001',
    's3': 's3://' + os.environ.get('KINYU_UNITTEST_S3_BUCKET'),
    'redis': 'redis://{}:6379'.format(
        os.environ.get('KINYU_UNITTEST_REDIS_HOST')),
    'dynamodb': 'dynamodb://' + os.environ.get('KINYU_UNITTEST_DYNAMODB'),
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
