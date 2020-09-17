import kydb
from datetime import datetime
import os
import pytest
from kydb.impl.tests.test_utils import is_automated_test


def get_dynamodb_name():
    return os.environ['KINYU_UNITTEST_DYNAMODB']


def get_dynamodb():
    return kydb.connect('dynamodb://' + get_dynamodb_name())


@pytest.fixture
def list_dir_db():
    db = get_dynamodb()
    db['/unittests/test_list_dir/obj1'] = 1
    db['/unittests/test_list_dir/foo/obj2'] = 2
    db['/unittests/test_list_dir/foo/obj3'] = 2
    db['/unittests/test_list_dir/foo/obj4'] = 3
    db['/unittests/test_list_dir/foo/bar/obj5'] = 4
    yield db
    db.rm_tree('/unittests/test_list_dir/')


@pytest.fixture
def db():
    return get_dynamodb()


@pytest.mark.skipif(is_automated_test(), reason="Do not run on automated test")
def test_dynamodb_basic(db):
    assert type(db).__name__ == 'DynamoDB'
    key = '/unittests/dynamodb/foo'
    db[key] = 123
    assert db[key] == 123
    assert db.read(key, reload=True) == 123
    db.list_dir('/unittests/dynamodb')
    db.delete(key)
    assert not db.exists(key)


@pytest.mark.skipif(is_automated_test(), reason="Do not run on automated test")
def test_dynamodb_dict(db):
    key = '/unittests/dynamodb/bar'
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
    db.delete(key)


@pytest.mark.skipif(is_automated_test(), reason="Do not run on automated test")
def test_dynamodb_with_basepath():
    db = kydb.connect('dynamodb://{}/my/base/path'.format(
        get_dynamodb_name()))
    key = '/apple'
    db[key] = 123
    assert db[key] == 123
    assert db.read(key, reload=True) == 123
    db.delete(key)


@pytest.mark.skipif(is_automated_test(), reason="Do not run on automated test")
def test_list_dir_with_subdir(list_dir_db):
    db = list_dir_db

    assert 'unittests/' in list(db.list_dir(''))
    assert 'unittests/' in list(db.list_dir('/'))
    assert set(['foo/', 'obj1']) \
        == set(db.list_dir('/unittests/test_list_dir'))
    assert set(['bar/', 'obj2', 'obj3', 'obj4']) == \
        set(db.list_dir('/unittests/test_list_dir/foo'))
    assert ['obj5'] == list(db.list_dir('/unittests/test_list_dir/foo/bar'))


@pytest.mark.skipif(is_automated_test(), reason="Do not run on automated test")
def test_list_dir_no_subdir(list_dir_db):
    db = list_dir_db
    assert 'unittests/' not in list(db.list_dir('', False))
    assert 'unittests/' not in list(db.list_dir('/', False))
    assert ['obj1'] == list(db.list_dir('/unittests/test_list_dir', False))
    assert set(['obj2', 'obj3', 'obj4']) == \
        set(db.list_dir('/unittests/test_list_dir/foo', False))
    assert ['obj5'] == list(db.list_dir(
        '/unittests/test_list_dir/foo/bar', False))


@pytest.mark.skipif(is_automated_test(), reason="Do not run on automated test")
def test_pagination(list_dir_db):
    db = list_dir_db
    assert 'unittests/' in list(db.list_dir(''))
    assert 'unittests/' in list(db.list_dir('/'))
    assert set(['foo/', 'obj1']
               ) == set(db.list_dir('/unittests/test_list_dir', page_size=1))
    assert set(['bar/', 'obj2', 'obj3', 'obj4']) == \
        set(db.list_dir('/unittests/test_list_dir/foo', page_size=1))
    assert ['obj5'] == list(db.list_dir(
        '/unittests/test_list_dir/foo/bar', page_size=1))
