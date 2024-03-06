# TODO: Move this into a test directory
import kydb
from kydb.cache import CacheDB

if __name__ == "__main__":
    cache_db = kydb.connect('memory://cache')
    persist_db = kydb.connect('memory://persist')
    db = CacheDB(cache_db, persist_db)
    folder = '/tests/'
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
    print(keys)
    print(db.ls('/tests'))
    assert ([folder + x for x in db.ls(folder)] == keys)

    # Test that even if cache is deleted, data can recover from persist db
    cache_db.delete(key1)
    assert (not cache_db.exists(key1))
    assert ([folder + x for x in db.ls(folder)] == keys)
    assert (db[key1] == val1)
    # After reading, now the cache should have the data
    assert (cache_db.exists(key1))
