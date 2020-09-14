import kydb


def test_uniondb():
    db = kydb.connect('memory://db1;memory://db2')
    assert repr(db) == '<MemoryDB memory://db1,MemoryDB memory://db2>'
