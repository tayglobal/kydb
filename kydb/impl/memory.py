from kydb.base import BaseDB
from kydb.folder_meta import FolderMetaMixin
import re


class MemoryDB(FolderMetaMixin, BaseDB):
    __cache = {}

    def __init__(self, url: str):
        super().__init__(url)
        self.__cache[self.db_name] = {}

    def get_raw(self, key):
        if self.base_path != '/' and \
                key == self._folder_meta_path(self.base_path, ''):
            raise KeyError(key)

        return self.__cache[self.db_name][key]

    def folder_meta_set_raw(self, key: str, value):
        self.__cache[self.db_name][key] = value

    def delete_raw(self, key: str):
        del self.__cache[self.db_name][key]

    def get_cache(self):
        return self.__cache[self.db_name]

    @staticmethod
    def _list_dir_regex(folder: str):
        return re.compile(f'^{folder}([^/]+)$')

    def list_dir_meta_folder(self, folder: str, page_size: int):
        folder = self._ensure_slashes(folder)
        pattern = self._list_dir_regex(folder)
        for path in self.__cache[self.db_name].keys():
            match = pattern.search(path)
            if match:
                yield match.groups(0)[0]
