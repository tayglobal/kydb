from kydb.base import BaseDB
from kydb.folder_meta import FolderMetaMixin
from redis.exceptions import ResponseError
import redis


class RedisDB(FolderMetaMixin, BaseDB):

    def __init__(self, url: str):
        super().__init__(url)

        self.connection = redis.Redis(
            **self._get_connection_kwargs(self.db_name))

    @staticmethod
    def _get_connection_kwargs(db_name: str):
        if ':' in db_name:
            host, port = db_name.split(':')
            kwargs = {
                'host': host,
                'port': int(port, 10)
            }
        else:
            kwargs = {
                'host': db_name
            }

        return kwargs

    def get_raw(self, key: str):
        try:
            res = self.connection.get(key)
        except ResponseError:
            raise KeyError(f'{key} is not a valid key')

        if not res:
            raise KeyError(key)

        return res

    def folder_meta_set_raw(self, key: str, value):
        folder, obj = key.rsplit('/', 1)
        self.connection.hset(folder, obj, '.')
        self.connection.set(key, value)

    def delete_raw(self, key: str):
        self.connection.delete(key)
        folder, obj = key.rsplit('/', 1)
        self.connection.hdel(folder, obj)

    def list_dir_meta_folder(self, folder: str, page_size: int):
        folder = self._ensure_slashes(folder)[:-1]
        try:
            for key in self.connection.hgetall(folder).keys():
                yield key.decode()
        except ResponseError:
            raise KeyError(f'{folder} is not a valid folder')
