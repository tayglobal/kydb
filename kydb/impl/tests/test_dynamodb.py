import kydb
from datetime import datetime
import os
import pytest
from kydb.impl.tests.test_utils import is_automated_test


def get_dynamodb_name():
    return os.environ['KINYU_UNITTEST_DYNAMODB']


@pytest.fixture
def db():
    return kydb.connect('dynamodb://' + get_dynamodb_name())


@pytest.mark.skipif(is_automated_test(), reason="Do not run on automated test")
def test_dynamodb_basic(db):
    assert type(db).__name__ == 'DynamoDB'
    key = '/unittests/dynamodb/foo'
    db[key] = 123
    assert db[key] == 123
    assert db.read(key, reload=True) == 123
    db.delete(key)
    assert not db.exists(key)


@pytest.mark.skipif(is_automated_test(), reason="Do not run on automated test")
def test_dynamodb_dict(db):
    key = '/unittests/dynamodb/bar'
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


@pytest.mark.skipif(is_automated_test(), reason="Do not run on automated test")
def test_dynamodb_with_basepath():
    db = kydb.connect('dynamodb://{}/my/base/path'.format(
        get_dynamodb_name()))
    key = '/apple'
    db[key] = 123
    assert db[key] == 123
    assert db.read(key, reload=True) == 123


@pytest.mark.skipif(is_automated_test(), reason="Do not run on automated test")
def test_dynamodb_errors(db):
    with pytest.raises(KeyError):
        db['does_not_exist']

    db.delete('does_not_exist')  # Does not raise though it does not exist
