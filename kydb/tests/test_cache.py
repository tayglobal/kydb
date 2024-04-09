import kydb
from kydb.tests.test_objdb import DBOBJ_CONFIG, Greeter
from kydb.objdb import DBOBJ_CONFIG_PATH
from kydb.cache import CacheDB


def test_simple_datatype():
    db = kydb.connect('memory://cache1|memory://persist1')

    folder = '/test_simple_datatype/'
    key1 = folder + 'foo'
    val1 = 'hello'
    key2 = folder + 'bar'
    val2 = 'world'

    db[key1] = val1
    assert (db[key1] == val1)
    assert (db.cache_db[key1] == val1)
    assert (db.persist_db[key1] == val1)

    db[key2] = val2
    assert (db[key2] == val2)
    assert (db.cache_db[key2] == val2)
    assert (db.persist_db[key2] == val2)

    keys = [key1, key2]
    assert ([folder + x for x in db.ls(folder)] == keys)

    # Test that even if cache is deleted, data can recover from persist db
    db.cache_db.delete(key1)
    assert (not db.cache_db.exists(key1))
    assert ([folder + x for x in db.ls(folder)] == keys)
    assert (db[key1] == val1)

    # After reading, now the cache should have the data
    assert (db.cache_db.exists(key1))

    # Make sure that data is read from cache_db and not
    # persist_db, we'll delete from persist db and try to read

    db.persist_db.delete(key1)
    assert not db.persist_db.exists(key1)
    assert db.cache_db[key1] == val1


def test_dbobj():
    db = kydb.connect('memory://cache2|memory://persist2')
    folder = '/test_dbobj/'
    key1 = folder + 'greeter001'
    key2 = folder + 'greeter002'
    class_name = 'Greeter'

    # First test db.upload_objdb_config,
    # should upload to both cache_db and persist_db
    db.upload_objdb_config(DBOBJ_CONFIG)

    assert isinstance(db.persist_db.new(class_name, key1), Greeter)
    assert isinstance(db.cache_db.new(class_name, key1), Greeter)
    greeter = db.new(class_name, key1)
    assert isinstance(greeter, Greeter)
    assert greeter.get_stored_dict() == {'name': 'John'}

    # Persting greeter should exist in both cache_db and persist_db
    greeter.write()
    db.persist_db.exists(key1)
    db.cache_db.exists(key1)
    db.exists(key1)
    assert isinstance(db[key1], Greeter)

    # Test ls
    greeter = db.new(class_name, key2)
    greeter.write()
    assert [folder + x for x in db.ls(folder)] == [key1, key2]

    # Check in-memory cache
    expected = [DBOBJ_CONFIG_PATH, key1, key2]
    assert list(db.cache_db._cache.keys()) == expected
    assert list(db.persist_db._cache.keys()) == expected

    # Test cache db blown away, data should still be available from persist db
    db.cache_db.get_cache().clear()
    assert db.cache_db.get_cache() == {}
    db.exists(key1)
    assert isinstance(db[key1], Greeter)
    db.exists(key2)
    assert isinstance(db[key2], Greeter)
    # After reading, cache db should now have cached the objects
    assert db.cache_db.get_cache() != {}

    # Check that we are indeed reading from cache_db
    # by blowing away persist_db
    db.persist_db.get_cache().clear()
    db.exists(key1)
    assert isinstance(db[key1], Greeter)
    db.exists(key2)
    assert isinstance(db[key2], Greeter)

    # Test clear_cache
    assert db.persist_db._cache != {}
    assert db.cache_db._cache != {}
    db.clear_cache()
    assert db.persist_db._cache == {}
    assert db.cache_db._cache == {}


def test_dbobj_db():
    """Test that reading key 1 has the db set to the CacheDB
    This is to make sure that we don't either have
    db.cache_db or db.persist_db as greeter.db"""
    db = kydb.connect('memory://cache3|memory://persist3')
    folder = '/test_dbobj/'
    key1 = folder + 'greeter001'
    class_name = 'Greeter'

    # First test db.upload_objdb_config,
    # should upload to both cache_db and persist_db
    db.upload_objdb_config(DBOBJ_CONFIG)

    greeter = db.new(class_name, key1)
    greeter.write()

    # Test when reading from _cache
    assert db[key1].db == db

    # Test when reading from cache_db
    db.clear_cache()
    assert db[key1].db == db

    # Test when reading from persist_db
    db.cache_db.delete(key1)
    assert not db.cache_db.exists(key1)
    assert db[key1].db == db

    # Now check that we actually have persisted it in cache_db
    db.cache_db.clear_cache()
    assert db.cache_db.exists(key1)


def test_objdb_config_cache_miss():
    db = kydb.connect('memory://cache4|memory://persist4')
    folder = '/test_dbobj/'
    key1 = folder + 'greeter001'
    class_name = 'Greeter'

    db.upload_objdb_config(DBOBJ_CONFIG)

    greeter = db.new(class_name, key1)
    greeter.write()

    # Delete the config: /.configs/objdb in cache_db
    db.cache_db.delete_raw('/.configs/objdb')

    #  Check that we can still read key1
    db.clear_cache()
    assert db[key1].db == db
