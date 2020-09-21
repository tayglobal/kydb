from kydb.base import BaseDB
import pathlib
import os
import os.path


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
