
from .base import BaseDB
from .interface import KYDBInterface
from typing import Tuple
from contextlib import ExitStack


def front_db_func(self, func_name, *args, **kwargs):
    return getattr(self.dbs[0], func_name)(*args, **kwargs)


def first_success_db_func(self, func_name, *args, **kwargs):
    first_error = None
    for db in self.dbs:
        try:
            return getattr(db, func_name)(*args, **kwargs)
        except KeyError as err:
            if not first_error:
                first_error = err
            continue

    raise first_error


def any_db_func(self, func_name, *args, **kwargs):
    return any(getattr(db, func_name)(*args, **kwargs) for db in self.dbs)


def all_db_func(self, func_name, *args, **kwargs):
    for db in self.dbs:
        getattr(db, func_name)(*args, **kwargs)


def create_func(func_prototype, func_name):
    # Using partial loses the self when constructing
    # class using type. So use this function
    def f(self, *args, **kwargs):
        return func_prototype(self, func_name, *args, **kwargs)

    return f


UNION_DB_BASE_FUNCS = [
    ('__getitem__', first_success_db_func),
    ('__setitem__', front_db_func),
    ('delete', front_db_func),
    ('rmdir', front_db_func),
    ('rm_tree', front_db_func),
    ('new', front_db_func),
    ('exists', any_db_func),
    ('refresh', all_db_func),
    ('read', first_success_db_func),
    ('mkdir', front_db_func),
    ('is_dir', any_db_func),
    ('upload_objdb_config', front_db_func)
]

UnionDBBase = type(
    'UnionDBBase',
    (KYDBInterface,),
    {k: create_func(v, k) for k, v in UNION_DB_BASE_FUNCS}
)


class UnionDB(UnionDBBase):
    """UnionDB


The URL used on *connect* can be a semi-colon separated string.

This would create a Union Database.

Connecting::

    db = kydb.connect('memory://unittest;s3://my-unittest-fixture')

OR::

    db = kydb.connect('redis://hotfixes.epythoncloud.io;'
                      '6379;dynamodb://my-prod-src-db')

Reading and writing::

    db1, db2 = db.dbs
    db1['/foo'] = 1
    db2['/bar'] = 2

    (db['/foo'], db['/bar']) # return (1, 2)

    # Although db2 has /foo, it is db1's /foo that the union returns
    db2['/foo'] = 3
    db['/foo'] # return 1

    # writing always happens on the front db
    db['/foo'] = 4
    db1['/foo'] # returns 4
    db2['/foo'] # returns 3
    """

    def __init__(self, dbs: Tuple[BaseDB]):
        self.dbs = dbs

    def cache_context(self) -> 'KYDBInterface':
        with ExitStack() as stack:
            for db in self.dbs:
                stack.enter_context(db.cache_context())

        return stack

    def list_dir(self, folder: str, include_dir=True, page_size=200):
        res = set()
        for db in self.dbs:
            try:
                res.update(db.list_dir(folder, include_dir, page_size))
            except KeyError:
                pass

        for key in res:
            yield key

    def ls(self, folder: str, include_dir=True):
        return list(self.list_dir(folder, include_dir))

    def __repr__(self):
        """
        The representation of the db.

        i.e. <UnionDB redis://my-redis-host/source;
              kydb.S3 s3://my-s3-prod-source>
        """
        return f'<{type(self).__name__} ' + \
            ';'.join(db.url for db in self.dbs) + '>'
