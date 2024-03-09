from .config import DB_MODULES
from .base import BaseDB
from .interface import KYDBInterface
from .union import UnionDB
from .cache import CacheDB
import importlib


_db_cache = {}


def connect(url: str) -> KYDBInterface:
    if "|" in url:
        dbs = [_connect(x) for x in url.split("|")]
        if len(dbs) != 2:
            raise ValueError("CacheDB expects exactly 2 databases")

        return CacheDB(*dbs)

    dbs = [_connect(x) for x in url.split(";")]
    if len(dbs) == 1:
        return dbs[0]
    return UnionDB(dbs)


def _connect(url: str) -> BaseDB:
    if url not in _db_cache:
        db_cls = _resolve_db_class(url)
        _db_cache[url] = db_cls(url)

    return _db_cache[url]


def _resolve_db_class(url: str):
    db_type = url.split(":", 1)[0]
    assert db_type in DB_MODULES, "{} is not one of the valid db types: {}".format(
        db_type, list(iter(DB_MODULES.keys()))
    )

    class_name = DB_MODULES[db_type]
    module_path = __name__.rsplit(".", 1)[0] + ".impl." + db_type
    m = importlib.import_module(module_path)
    return getattr(m, class_name)
