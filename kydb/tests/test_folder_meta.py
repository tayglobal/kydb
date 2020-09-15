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
    folder_meta_path = list(db.cache.keys())
    expected = ['/.folder-my-folder1', '/.folder-my-folder2']
    assert folder_meta_path == expected


def test_mkdir_deep():
    db = DummyDb('memory://test-mkdir')
    db.mkdir('/foo/bar/baz/data')
    folder_meta_path = list(db.cache.keys())
    expected = ['/.folder-foo', '/foo/.folder-bar',
                '/foo/bar/.folder-baz',
                '/foo/bar/baz/.folder-data']
    assert folder_meta_path == expected


def test_setitem():
    db = DummyDb('memory://test-mkdir')
    key = '/path/to/my/item'
    db[key] = 123
    folder_meta_path = list(db.cache.keys())

    expected = ['/.folder-path', '/path/.folder-to',
                '/path/to/.folder-my',
                '/path/to/my/item']

    assert folder_meta_path == expected

    assert db.read(key, reload=True) == 123
