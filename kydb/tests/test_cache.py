# TODO: Move this into a test directory
import kydb
from kydb.cache import CacheDB
import pytest
from kydb.tests.test_objdb import DBOBJ_CONFIG, Greeter


@pytest.fixture
def database_set():
    cache_db = kydb.connect('memory://cache')
    persist_db = kydb.connect('memory://persist')
    return CacheDB(cache_db, persist_db), cache_db, persist_db


def test_simple_datatype(database_set):
    db, cache_db, persist_db = database_set
    folder = '/test_simple_datatype/'
    key1 = folder + 'foo'
    val1 = 'hello'
    key2 = folder + 'bar'
    val2 = 'world'

    db[key1] = val1
    assert (db[key1] == val1)
    assert (cache_db[key1] == val1)
    assert (persist_db[key1] == val1)

    db[key2] = val2
    assert (db[key2] == val2)
    assert (cache_db[key2] == val2)
    assert (persist_db[key2] == val2)

    keys = [key1, key2]
    assert ([folder + x for x in db.ls(folder)] == keys)

    # Test that even if cache is deleted, data can recover from persist db
    cache_db.delete(key1)
    assert (not cache_db.exists(key1))
    assert ([folder + x for x in db.ls(folder)] == keys)
    assert (db[key1] == val1)

    # After reading, now the cache should have the data
    assert (cache_db.exists(key1))

    # Make sure that data is read from cache_db and not
    # persist_db, we'll delete from persist db and try to read

    persist_db.delete(key1)
    assert not persist_db.exists(key1)
    assert cache_db[key1] == val1


def test_dbobj(database_set):
    db, cache_db, persist_db = database_set
    folder = '/test_dbobj/'
    key1 = folder + 'greeter001'
    key2 = folder + 'greeter002'
    class_name = 'Greeter'

    # First test db.upload_objdb_config,
    # should upload to both cache_db and persist_db
    db.upload_objdb_config(DBOBJ_CONFIG)
    assert isinstance(persist_db.new(class_name, key1), Greeter)
    assert isinstance(cache_db.new(class_name, key1), Greeter)
    greeter = db.new(class_name, key1)
    assert isinstance(greeter, Greeter)
    assert greeter.get_stored_dict() == {'name': 'John'}

    # Persting greeter should exist in both cache_db and persist_db
    greeter.write()
    persist_db.exists(key1)
    cache_db.exists(key1)
    db.exists(key1)
    assert isinstance(db[key1], Greeter)

    # Test ls
    greeter = db.new(class_name, key2)
    greeter.write()
    assert [folder + x for x in db.ls(folder)] == [key1, key2]

    # test clear cache
    # TODO: Check why cache_db._keys() looks like the below
    # dict_keys(['/.configs/objdb', '.configs/objdb', '/test_dbobj/greeter001', '/test_dbobj/greeter002'])
    print(cache_db._cache.keys())

    # Test cache db blown away, data should still be available from persist db
    cache_db.get_cache().clear()
    assert cache_db.get_cache() == {}
    db.exists(key1)
    assert isinstance(db[key1], Greeter)
    db.exists(key2)
    assert isinstance(db[key2], Greeter)
    # After reading, cache db should now have cached the objects
    assert cache_db.get_cache() != {}

    # Check that we are indeed reading from cache_db
    # by blowing away persist_db
    persist_db.get_cache().clear()
    db.exists(key1)
    assert isinstance(db[key1], Greeter)
    db.exists(key2)
    assert isinstance(db[key2], Greeter)