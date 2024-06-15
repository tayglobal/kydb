# This test some of the experimental features of the kydb module.
# Which does not yet support all the implementations
from unittest import skipUnless
import os
import kydb
import pytest
from time import sleep


DYNAMODB_TABLE = os.environ.get("KINYU_UNITTEST_DYNAMODB", "")

OBJ_MODULE_PATH = "kydb.tests.test_objdb"
OBJ_CLASS_NAME = "Greeter"

DBOBJ_CONFIG = {
    "Greeter": {"module_path": OBJ_MODULE_PATH, "class_name": OBJ_CLASS_NAME}
}


@pytest.fixture
def db():
    if not DYNAMODB_TABLE:
        return

    db = kydb.connect("dynamodb://" + DYNAMODB_TABLE)
    # upload the config
    db.upload_objdb_config(DBOBJ_CONFIG)
    return db


@skipUnless(
    DYNAMODB_TABLE,
    "DynamoDB table not set. Set KINYU_UNITTEST_DYNAMODB to run this test",
)
def test_read_folder(db):
    folder = "/unittests/test_read_folder"
    for x in db.ls(folder):
        db.delete(f'{folder}/{x}')

    key1 = f"{folder}/foo"
    db[key1] = 123
    key2 = f"{folder}/bar"
    db[key2] = 234

    # Wait for db to be consistent
    sleep(0.5)

    actual = set(x for x in db.read_folder(folder))
    print(actual)
    expected = set([(key1, 123), (key2, 234)])
    assert actual == expected

    key = f"{folder}/greeter"
    dummy = db.new('Greeter', key, name='Peter')
    dummy.write()

    # Wait for db to be consistent
    sleep(0.5)

    actual = set(type(x[1]).__name__ for x in db.read_folder(folder))
    print(actual)
    expected = set(['int', 'Greeter'])
    assert actual == expected


