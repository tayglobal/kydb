import kydb
import pytest
from kydb.exceptions import DbObjException

OBJ_MODULE_PATH = 'kydb.tests.test_objdb'
OBJ_CLASS_NAME = 'Greeter'

DBOBJ_CONFIG = {
    'BadClass': {
        'missing_module_path': 'missing',
        'class_name': 'BadClass'
    },
    'Greeter': {
        'module_path': OBJ_MODULE_PATH,
        'class_name': OBJ_CLASS_NAME
    }
}


@pytest.fixture
def db():
    db = kydb.connect('memory://unittest')
    # upload the config
    db.upload_objdb_config(DBOBJ_CONFIG)
    return db


class Greeter(kydb.DbObj):

    def init(self):
        self.greet_count = 0

    @kydb.stored
    def name(self):
        return 'John'

    def greet(self):
        self.greet_count += 1
        return 'Hello ' + self.name()


def test__get_dbobj_config_no_cfg():
    # Missing config yaml in db
    db = kydb.connect('memory://unittest')

    with pytest.raises(DbObjException):
        db._get_dbobj_config('MyClass')


def test__get_dbobj_config(db):

    # A class that doesn't exist
    with pytest.raises(KeyError):
        db._get_dbobj_config('MyClass')

    # A class that exists but malformed
    with pytest.raises(DbObjException):
        db._get_dbobj_config('BadClass')

    cfg = db._get_dbobj_config('Greeter')
    assert cfg['module_path'] == OBJ_MODULE_PATH
    assert cfg['class_name'] == OBJ_CLASS_NAME


def test_get_stored_dict(db):
    dummy = db.new('Greeter', '/unittest/dbobj/greeter001')
    assert db.is_dbobj(dummy)
    assert dummy.get_stored_dict() == {'name': 'John'}


def test_default(db):
    dummy = db.new('Greeter', '/unittest/dbobj/greeter001')
    assert dummy.name() == 'John'


def test_init_with_val(db):
    dummy = db.new('Greeter', '/unittest/dbobj/greeter001', name='Tony')
    assert dummy.name() == 'Tony'
    assert dummy.greet() == 'Hello Tony'
    assert dummy.greet_count == 1


def test_setvalue(db):
    dummy = db.new('Greeter', '/unittest/dbobj/greeter001')
    dummy.name.setvalue('Peter')
    assert dummy.name() == 'Peter'
    assert dummy.greet() == 'Hello Peter'
    assert dummy.greet_count == 1
    dummy.name.clear()
    assert dummy.name() == 'John'
    assert dummy.greet() == 'Hello John'
    assert dummy.greet_count == 2


def test_write(db):
    key = '/unittest/dbobj/greeter001'
    name = 'Jane'
    dummy = db.new('Greeter', key, name=name)
    dummy.greet()
    assert dummy.greet_count == 1
    dummy.write()
    assert db[key] == dummy
    res = db.read(key, reload=True)
    assert res.greet_count == 0
    assert res.class_name == 'Greeter'
    assert res.key == key
    assert res.db == db
    assert res.name() == name
    assert res.greet() == 'Hello ' + name
    assert res.greet_count == 1
    res.delete()
    assert not db.exists(key)


def test_union():
    db = kydb.connect('memory://db1;memory://db2')
    # upload the config
    db.upload_objdb_config(DBOBJ_CONFIG)
    key = '/unittest/dbobj/greeter001'
    obj = db.new('Greeter', key)
    assert obj.name() == 'John'

    # Object does not exist anywhere in the DB
    assert not db.exists(key)
    assert not db.dbs[0].exists(key)
    assert not db.dbs[1].exists(key)

    obj.write()
    # object exists only in front DB
    assert db.exists(key)
    assert db.dbs[0].exists(key)
    assert not db.dbs[1].exists(key)

    obj.delete()
    # Object does not exist anywhere in the DB
    assert not db.exists(key)
    assert not db.dbs[0].exists(key)
    assert not db.dbs[1].exists(key)
