import kydb
from kydb.impl.redis import RedisDB
from datetime import datetime
import os
import pytest
from kydb.impl.tests.test_utils import is_automated_test


@pytest.fixture
def db():
    return kydb.connect('redis://{}:6379'.format(
        os.environ['KINYU_UNITTEST_REDIS_HOST']))


@pytest.mark.skipif(is_automated_test(), reason="Do not run on automated test")
def test_redis_basic(db):
    key = '/unittests/foo'
    db[key] = 123
    assert db[key] == 123
    assert db.read(key, reload=True) == 123
    assert db.exists(key)
    db.delete(key)
    assert not db.exists(key)


@pytest.mark.skipif(is_automated_test(), reason="Do not run on automated test")
def test_redis_dict(db):
    key = '/unittests/test_redis_dict/bar'
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
    db.rm_tree('/unittests/test_redis_dict/')


@pytest.mark.skipif(is_automated_test(), reason="Do not run on automated test")
def test_list_dir(db):
    db['/unittests/test_list_dir/list/dir/obj1'] = 123
    db['/unittests/test_list_dir/list/obj2'] = 123
    db['/unittests/test_list_dir/obj3'] = 123
    db['/unittests/obj4'] = 123
    db['obj5'] = 123
    db['/unittests/test_list_dir/obj6'] = 123
    db['/unittests/test_list_dir/obj7'] = 123

    assert 'unittests/' in list(db.list_dir(''))
    assert 'obj5' in list(db.list_dir(''))
    assert 'unittests/' in list(db.list_dir('/'))
    assert 'obj5' in list(db.list_dir('/'))
    assert 'test_list_dir/' in list(db.list_dir('/unittests'))
    assert 'obj4' in list(db.list_dir('/unittests'))

    assert list(db.list_dir('/unittests/test_list_dir/')
                ) == ['list/', 'obj3', 'obj6', 'obj7']
    assert list(db.list_dir('/unittests/test_list_dir')
                ) == ['list/', 'obj3', 'obj6', 'obj7']
    assert list(db.list_dir('unittests/test_list_dir/')
                ) == ['list/', 'obj3', 'obj6', 'obj7']
    assert list(db.list_dir('unittests/test_list_dir')
                ) == ['list/', 'obj3', 'obj6', 'obj7']
    assert list(db.list_dir('/unittests/test_list_dir/list')
                ) == ['dir/', 'obj2']
    assert list(db.list_dir('/unittests/test_list_dir/list/dir/')) == ['obj1']

    with pytest.raises(KeyError):
        list(db.list_dir('/unittests/test_list_dir/list/dir/obj1'))

    with pytest.raises(KeyError):
        db['/unittests/test_list_dir/list/dir']

    db.rm_tree('/unittests/test_list_dir')
    assert not db.is_dir('/unittests/test_list_dir')


def test_default_port():
    assert RedisDB._get_connection_kwargs(
        'my-host:1234') == {'host': 'my-host', 'port': 1234}
    assert RedisDB._get_connection_kwargs('my-host2') == {'host': 'my-host2'}
