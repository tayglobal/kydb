from kydb.base import BaseDB
from kydb.exceptions import IndexNotSupported
from kydb.query import ScanFolderQuery
import pathlib
import os
import os.path


class FileFolderQuery(ScanFolderQuery):
    """ Client-side scan-and-sort over the filesystem, sorted by
    ``st_mtime_ns``. Files have no separate, portable creation-time
    attribute that survives a rewrite (``st_ctime`` is a metadata-change
    time, not a creation time, and is not comparable across platforms),
    so ``ctime`` falls back to ``mtime`` here -- documented, not
    engineered around, matching S3 below.
    """

    def _raw_entries(self):
        folder = self._db._get_fs_path(
            self._db._ensure_slashes(
                self._db._get_full_path(self._folder)))
        try:
            for filename in os.listdir(folder):
                path = os.path.join(folder, filename)
                if os.path.isdir(path):
                    continue
                mtime_ns = os.stat(path).st_mtime_ns
                yield filename, mtime_ns, mtime_ns
        except FileNotFoundError:
            raise KeyError(folder)


class FileDB(BaseDB):
    """
    Example::

    db = kydb('files://tmp/foo/bar') # must be absolute path

    would read and write files under /tmp/foo/bar
    """

    def __init__(self, url: str):
        """
        :param url: str: the URL starting with file://
        """
        super().__init__(url)

    def get_raw(self, key: str):
        try:
            return open(self._get_fs_path(key), 'rb').read()
        except FileNotFoundError:
            raise KeyError(key)

    def mkdir_raw(self, folder: str):
        folder = self._get_fs_path(folder)
        pathlib.Path(folder).mkdir(parents=True, exist_ok=True)

    def set_raw(self, key: str, value):
        """
        save value to file system

        :param key: str: the path to the file including base_path,
                         but excluding the db_name. e.g.
                         for kydb.connect(files://tmp/foo/bar)
                         key would be /foo/bar
        :param value: The raw, pickled data.
        """
        fullpath = self._get_fs_path(key)
        folder = fullpath.rsplit('/', 1)[0]
        pathlib.Path(folder).mkdir(parents=True, exist_ok=True)
        with open(fullpath, 'wb') as f:
            f.write(value)

    def delete_raw(self, key: str):
        """
        Delete a the file from filesystem
        To be implemented by derived class

        :param key: str: the path to the file including base_path,
                         but excluding the db_name. e.g.
                         for kydb.connect(files://tmp/foo/bar)
                         key would be /foo/bar

        """
        os.remove(self._get_fs_path(key))

    def _get_fs_path(self, key: str):
        return '/' + self.db_name + key

    def is_dir_raw(self, folder: str) -> bool:
        """ Is this a directory?

        :param folder: Returns True if is directory
        """
        path = self._get_fs_path(folder)
        return os.path.isdir(path)

    def list_dir_raw(self, folder: str, include_dir: bool, page_size: int):
        folder = self._get_fs_path(folder)
        try:
            for filename in os.listdir(folder):
                path = self._ensure_slashes(folder) + filename
                if os.path.isdir(path):
                    if include_dir:
                        yield filename + '/'
                else:
                    yield filename
        except FileNotFoundError:
            raise KeyError(folder)

    def rmdir_raw(self, folder: str):
        try:
            os.rmdir(self._get_fs_path(folder))
        except OSError:
            raise KeyError('Cannot remove folder: ' + folder)

    def exists_raw(self, key) -> bool:
        path = self._get_fs_path(key)
        return os.path.exists(path) and not os.path.isdir(path)

    def folder(self, folder: str, allow_scan: bool = False) \
            -> FileFolderQuery:
        """ Implements folder in KYDBInterface.

        FileDB has no server-side ordering index -- this is a
        client-side scan-and-sort of the directory (via ``st_mtime``),
        so it requires the caller to opt in with ``allow_scan=True``.
        """
        if not allow_scan:
            raise IndexNotSupported(
                f'{type(self).__name__} does not support folder()/'
                "recent() natively; pass allow_scan=True to opt into "
                'an O(n) client-side scan-and-sort of the folder')
        return FileFolderQuery(self, folder, allow_scan=True)

    def reindex(self, folder: str) -> int:
        """No-op: filesystem ``st_mtime_ns`` already covers old files."""
        return 0
