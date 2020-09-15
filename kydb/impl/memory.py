from kydb.base import BaseDB
from kydb.folder_meta import FolderMetaMixin


class MemoryDB(FolderMetaMixin, BaseDB):
    __cache = {}

    def __init__(self, url: str):
        super().__init__(url)
        self.__cache[self.db_name] = {}

    def get_raw(self, key):
        return self.__cache[self.db_name][key]

    def set_raw(self, key, value):
        self.__cache[self.db_name][key] = value

    def delete_raw(self, key: str):
        del self.__cache[self.db_name][key]

    def list_obj_raw(self, folder: str):
        folder = self._ensure_slashes(folder)
        num_chars = len(folder)
        for path in self.__cache[self.db_name].keys():
            if path.startswith(folder) and '/' not in path[num_chars:]:
                yield path.rsplit('/', 1)[1]
