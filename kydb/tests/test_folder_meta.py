from kydb.folder_meta import FolderMetaMixin
from kydb import BaseDB


class DummyDb(FolderMetaMixin, BaseDB):
    def __init__(self, url: str):
        super().__init__(url)
        self.cache = {}

    def get_raw(self, key):
        return self.cache[key]

    def set_raw(self, key, value):
        self.cache[key] = value


def test_mkdir_shallow():
    db = DummyDb('memory://test-mkdir')
    db.mkdir('/my-folder1')
    db.mkdir('/my-folder2')
    assert db['/.folder-meta'] == {'my-folder1', 'my-folder2'}
    assert db.list_subdir_as_list('/') == ['my-folder1', 'my-folder2']
    assert db.list_subdir_as_list('') == ['my-folder1', 'my-folder2']


def test_mkdir_deep():
    db = DummyDb('memory://test-mkdir')
    db.mkdir('/foo/bar/baz/data')
    assert db.list_subdir_as_list('/') == ['foo']
    assert db.list_subdir_as_list('/foo') == ['bar']
    assert db.list_subdir_as_list('/foo/bar') == ['baz']
    assert db.list_subdir_as_list('/foo/bar/baz') == ['data']


def test_setitem():
    db = DummyDb('memory://test-mkdir')
    key = '/path/to/my/item'
    db[key] = 123
    assert db.list_subdir_as_list('/') == ['path']
    assert db.list_subdir_as_list('/path') == ['to']
    assert db.list_subdir_as_list('/path/to') == ['my']
    assert db.list_subdir_as_list('/path/to/my/item') == []

    assert db.read(key, reload=True) == 123
