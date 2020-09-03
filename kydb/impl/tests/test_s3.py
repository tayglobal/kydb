import kydb
from datetime import datetime
import pytest
import os
from kydb.impl.tests.test_utils import is_automated_test


@pytest.fixture
def db():
    return kydb.connect('s3://' + os.environ['KINYU_UNITTEST_S3_BUCKET'])


def test__get_s3_path():
    S3_BUCKET = 'my_dummy_bucket'
    db = kydb.connect('s3://' + S3_BUCKET)
    assert db._get_s3_path('foo') == db.url + '/foo'
    assert db._get_s3_path('/foo') == db.url + '/foo'

    db = kydb.connect('s3://' + S3_BUCKET + '/base/path')
    assert db._get_s3_path('foo') == db.url + '/foo'
    assert db._get_s3_path('/foo') == db.url + '/foo'


@pytest.mark.skipif(is_automated_test(), reason="Do not run on automated test")
def test_s3_basic(db):
    key = '/unittests/s3/foo'
    db[key] = 123
    assert db[key] == 123
    assert db.exists(key)
    db.delete(key)
    assert not db.exists(key)


@pytest.mark.skipif(is_automated_test(), reason="Do not run on automated test")
def test_s3_dict(db):
    key = '/unittests/s3/bar'
    val = {
        'my_int': 123,
        'my_float': 123.456,
        'my_str': 'hello',
        'my_list': [1, 2, 3],
        'my_datetime': datetime.now()
    }
    db[key] = val
    assert db[key] == val
