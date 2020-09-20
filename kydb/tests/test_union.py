import kydb


def test_basic():
    db = kydb.connect('memory://union_db1;memory://union_db2')
    db1, db2 = db.dbs
    db1['/foo'] = 1
    db2['/foo'] = 2
    assert db['/foo'] == 1
    assert db.ls('/') == ['foo']

    db['/foo'] = 3
    assert db['/foo'] == 3
    assert db1['/foo'] == 3
    assert db2['/foo'] == 2

    db.delete('/foo')
    assert db['/foo'] == 2
    assert list(db.list_dir('/')) == ['foo']
    assert db.ls('/') == ['foo']

    db2.delete('/foo')
    assert list(db.list_dir('/')) == []
    assert db.ls('/') == []


def test_list_dir():
    db = kydb.connect('memory://union_db3;memory://union_db4')
    db1, db2 = db.dbs

    db1['/a/b/obj1'] = 1
    db1['/a/b/obj2'] = 2
    db1['/a/obj3'] = 3

    db2['/a/b/obj4'] = 4
    db2['/a/obj5'] = 5
    db2['obj6'] = 6

    assert set(db.ls('/')) == set(['a/', 'obj6'])
    assert set(db.ls('/a/')) == set(['b/', 'obj3', 'obj5'])
    assert set(db.ls('/a/b/')) == set(['obj1', 'obj2', 'obj4'])

    assert set(db.ls('/', False)) == set(['obj6'])
    assert set(db.ls('/a/', False)) == set(['obj3', 'obj5'])
    assert set(db.ls('/a/b/', False)) == set(['obj1', 'obj2', 'obj4'])
