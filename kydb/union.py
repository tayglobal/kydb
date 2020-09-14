
from .base import BaseDB
from typing import Tuple


class UnionDB:
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

    def __repr__(self):
        """
        The representation of the db.

        i.e. <UnionDB redis://my-redis-host/source;
              kydb.S3 s3://my-s3-prod-source>
        """
        return f'<{type(self).__name__} ' + \
            ';'.join(db.url for db in self.dbs) + '>'
