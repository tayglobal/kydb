from .config import DB_MODULES
from .base import BaseDB, IDB
import importlib
from typing import Tuple


class UnionDB:
    def __init__(self, dbs: Tuple[BaseDB]):
        self.dbs = dbs

    def upload_objdb_config(self, config):
        """Upload ObjDB Conifg

        :param config: The config dict

        updates only the FrontDB
        """
        self.dbs[0].upload_objdb_config(config)

    def new(self, *args, **kwargs):
        """ Creates a new object

        Calls new on the FrontDB
        """
        return self.dbs[0].new(*args, **kwargs)

    def exists(self, key: str):
        """ key exists  in any of the union of of databases

        :param key: the key to check existance of
        """
        return any(db.exists(key) for db in self.dbs)

    def __getitem__(self, key: str):
        """ get the object based on key


        :param key: the key to get

        attempts to resolve object from FrontDB to the back.
        If not found, raise ``KeyError``
        """
        for db in self.dbs:
            try:
                return db[key]
            except KeyError:
                pass

        raise KeyError(key)

    def __setitem__(self, key, value):
        """ set the object based on key


        :param key: the key to set

        saves the object in the FrontDB
        """
        print(f'setting {key} to {value}')
        print(f'db = {self.dbs[0]}')
        self.dbs[0][key] = value


_db_cache = {}


def connect(url: str) -> IDB:
    dbs = [_connect(x) for x in url.split(';')]
    if len(dbs) == 1:
        return dbs[0]
    return UnionDB(dbs)


def _connect(url: str) -> BaseDB:
    global _db_cache
    if url not in _db_cache:
        db_cls = _resolve_db_class(url)
        _db_cache[url] = db_cls(url)

    return _db_cache[url]


def _resolve_db_class(url: str):
    db_type = url.split(':', 1)[0]
    assert db_type in DB_MODULES, \
        '{} is not one of the valid db types: {}'.format(
            db_type,
            list(iter(DB_MODULES.keys())))

    class_name = DB_MODULES[db_type]
    module_path = __name__.rsplit('.', 1)[0] + '.impl.' + db_type
    m = importlib.import_module(module_path)
    return getattr(m, class_name)
