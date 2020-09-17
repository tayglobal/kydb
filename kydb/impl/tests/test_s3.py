import kydb
from datetime import datetime
import pytest
import os
from kydb.impl.tests.test_utils import is_automated_test


@pytest.fixture
def db():
    return kydb.connect('s3://' + os.environ['KINYU_UNITTEST_S3_BUCKET'])


@pytest.fixture
def list_dir_db():
    db = kydb.connect('s3://' + os.environ['KINYU_UNITTEST_S3_BUCKET'])
    db['/unittests/test_list_dir/obj1'] = 1
    db['/unittests/test_list_dir/foo/obj2'] = 2
    db['/unittests/test_list_dir/foo/obj3'] = 2
    db['/unittests/test_list_dir/foo/obj4'] = 3
    db['/unittests/test_list_dir/foo/bar/obj5'] = 4
    yield db
    db.rm_tree('/unittests/test_list_dir/')


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
    db.rm_tree('/unittests/s3/')
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
    db.rm_tree('/unittests/s3/')


@pytest.mark.skipif(is_automated_test(), reason="Do not run on automated test")
def test_deep_folder_1obj(db):
    db['/unittests/s3/deep/folder/with/1/obj'] = 123
    assert list(db.list_dir('/unittests/s3/deep')) == ['folder/']
    assert list(db.list_dir('/unittests/s3/deep/folder')) == ['with/']
    assert list(db.list_dir('/unittests/s3/deep/folder/with')) == ['1/']
    assert list(db.list_dir('/unittests/s3/deep/folder/with/1')) == ['obj']
    assert db.exists('/unittests/s3/deep/folder/with/1/obj')
    db.delete('/unittests/s3/deep/folder/with/1/obj')
    assert not db.exists('/unittests/s3/deep/folder/with/1/obj')


@pytest.mark.skipif(is_automated_test(), reason="Do not run on automated test")
def test_list_dir_with_subdir(list_dir_db):
    db = list_dir_db

    assert 'unittests/' in list(db.list_dir(''))
    assert 'unittests/' in list(db.list_dir('/'))
    assert set(['foo/', 'obj1']) == \
        set(db.list_dir('/unittests/test_list_dir'))
    assert set(['bar/', 'obj2', 'obj3', 'obj4']) == \
        set(db.list_dir('/unittests/test_list_dir/foo'))
    assert ['obj5'] == list(db.list_dir('/unittests/test_list_dir/foo/bar'))


@pytest.mark.skipif(is_automated_test(), reason="Do not run on automated test")
def test_list_dir_no_subdir(list_dir_db):
    db = list_dir_db
    assert 'unittests/' not in list(db.list_dir('', False))
    assert 'unittests/' not in list(db.list_dir('/', False))
    assert ['obj1'] == list(db.list_dir('/unittests/test_list_dir', False))
    assert set(['obj2', 'obj3', 'obj4']) == \
        set(db.list_dir('/unittests/test_list_dir/foo', False))
    assert ['obj5'] == list(db.list_dir(
        '/unittests/test_list_dir/foo/bar', False))


@pytest.mark.skipif(is_automated_test(), reason="Do not run on automated test")
def test_pagination(list_dir_db):
    db = list_dir_db
    assert 'unittests/' in list(db.list_dir(''))
    assert 'unittests/' in list(db.list_dir('/'))
    assert set(['foo/', 'obj1']
               ) == set(db.list_dir('/unittests/test_list_dir', page_size=1))
    assert set(['bar/', 'obj2', 'obj3', 'obj4']) == \
        set(db.list_dir('/unittests/test_list_dir/foo', page_size=1))
    assert ['obj5'] == list(db.list_dir(
        '/unittests/test_list_dir/foo/bar', page_size=1))
