import kydb
from datetime import datetime
import pytest
import tempfile
import os.path
import shutil

LOCAL_DIR = tempfile.gettempdir() + '/unittests/test_fs'


@pytest.fixture
def db():
    yield kydb.connect('files:/' + LOCAL_DIR)
    shutil.rmtree(LOCAL_DIR)


def test_files_basic(db):
    key = '/unittests/files/foo'
    db[key] = 123
    assert db[key] == 123
    assert db.exists(key)
    assert os.path.exists(LOCAL_DIR + key)
    db.delete(key)
    assert not db.exists(key)


def test_files_dict(db):
    key = '/unittests/files/bar'
    val = {
        'my_int': 123,
        'my_float': 123.456,
        'my_str': 'hello',
        'my_list': [1, 2, 3],
        'my_datetime': datetime.now()
    }
    db[key] = val
    assert db[key] == val
    db.delete(key)
    assert not db.exists(key)
