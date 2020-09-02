import kydb
import pytest
from kydb.dbobj.api import DbObjManager
from kydb.exceptions import DbObjException

OBJ_MODULE_PATH = 'kydb.dbobj.tests.test_api'
OBJ_CLASS_NAME = 'DummyClass'

DBOBJ_CONFIG = '''
  BadClass:
    missing_module_path: missing
    class_name: BadClass
  DummyClass:
    module_path: {}
    class_name: {}
'''.format(OBJ_MODULE_PATH,  OBJ_CLASS_NAME)


@pytest.fixture
def db():
    db = kydb.connect('memory://unittest')
    # upload the config
    DbObjManager.upload_config(db, DBOBJ_CONFIG)
    return db


@kydb.dbobj
class DummyClass:

    @kydb.stored
    def currency(self):
        return 'USD'


def test__get_dbobj_config_no_cfg():
    # Missing config yaml in db
    db = kydb.connect('memory://unittest')

    with pytest.raises(DbObjException):
        DbObjManager._get_dbobj_config(db, 'MyClass')


def test__get_dbobj_config(db):

    # A class that doesn't exist
    with pytest.raises(KeyError):
        DbObjManager._get_dbobj_config(db, 'MyClass')

    # A class that exists but malformed
    with pytest.raises(DbObjException):
        DbObjManager._get_dbobj_config(db, 'BadClass')

    cfg = DbObjManager._get_dbobj_config(db, 'DummyClass')
    assert cfg['module_path'] == OBJ_MODULE_PATH
    assert cfg['class_name'] == OBJ_CLASS_NAME


def test_default(db):
    dummy = db.new('DummyClass', '/unittest/dbobj/dummy001')
    assert dummy.currency() == 'USD'


def test_init_with_val(db):
    dummy = db.new('DummyClass', '/unittest/dbobj/dummy001', currency='JPY')
    assert dummy.currency() == 'JPY'


def test_setvalue(db):
    dummy = db.new('DummyClass', '/unittest/dbobj/dummy001')
    dummy.currency.setvalue('EUR')
    assert dummy.currency() == 'EUR'


def test_write(db):
    key = '/unittest/dbobj/dummy001'
    currency = 'KRW'
    dummy = db.new('DummyClass', key, currency=currency)
    dummy.write()
    assert db[key] == dummy
    res = db.read(key, reload=True)
    assert res.class_name == 'DummyClass'
    assert res.key == key
    assert res.db == db
    assert res.currency() == currency
