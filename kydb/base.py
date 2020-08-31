from abc import ABC
import pickle
import importlib
from typing import Tuple


class IDB(ABC):
    """
    The interface that all KYDB adheres to
    """

    def __getitem__(self, key: str):
        """
        Get data from the DB based on key
        To be implemented by derived class

        :param key: str:  The key to get.
        """
        raise NotImplementedError()

    def __setitem__(self, key: str, value):
        """
        Set data from the DB based on key
        To be implemented by derived class

        :param key: str:  The key to set.
        """
        raise NotImplementedError()

    def delete(self, key: str):
        """
        Delete a key from the db.
        To be implemented by derived class

        :param key: str:  The key to delete.

        """
        raise NotImplementedError()


class BaseDB(IDB):
    """
    All implementations derives from this base class except for UnionDB.

    This class and all derived classes are never instantiated directly.

Instead use the ``connect``. i.e.::

    import kydb

    db = kydb.connect('dynamodb://my-table')
    """

    def __init__(self, url: str):
        self.db_type = url.split(':', 1)[0]
        self.db_name, self.base_path = self._get_name_and_basepath(url)
        self.url = url
        self._cache = {}

    @staticmethod
    def _get_name_and_basepath(url: str) -> Tuple[str, str]:
        """
        get the db_name and base_path

        :param url: str:
        :returns: a tuple (db_name, base_path)

        """
        base_path = '/'
        parts = url.split('/', 3)
        num_parts = len(parts)
        assert num_parts in (3, 4)

        db_name = parts[2]
        if len(parts) == 4:
            base_path += parts[-1]

        if base_path[-1] != '/':
            base_path += '/'

        return db_name, base_path

    def _get_full_path(self, key: str) -> str:
        """
        get the fullpath from key including base_path.

        :param key: str:
        :returns: The fullpath from key including base_path.

        """
        if key.startswith('/'):
            key = key[1:]

        return self.base_path + key

    def _serialise(self, obj):
        """
        Serialises the object.

        :param obj: object to serialise
        :returns: The obj pickled

        """
        return pickle.dumps(obj)

    def _deserialise(self, data):
        """
        Deserialise the data

        :param obj:
        :returns: Unpickle data
        """
        return pickle.loads(data)

    def exists(self, key) -> bool:
        """
        Check if a key exists in the DB

        :param key: the key
        :returns: True if key exists, False otherwise.

        Note that base implementation is to try and get the object
        derived db would either return the object or must raise
        KeyError if not found.

        Returns False if KeyError is raised, True otherwise

There are also side effects. The result is cached so that::

        if db.exists(key):
            db[key]

would only hit the DB once.
        """
        # TODO: defer this implmementaion to derived class as exist()
        # should not need to load the object
        try:
            self[key]
            return True
        except KeyError:
            return False

    def __getitem__(self, key: str):
        """
        Get data from the DB based on key

        :param key: str:  The key to get.
        """
        return self.read(key)

    def refresh(self, key=None):
        """
        Flush the cache

        :param key: Optionally choose which key to flush (Default value = None)

        """
        if key:
            del self._cache[key]
        else:
            self._cache = {}

    def read(self, key: str, reload=False):
        """
        Read object from DB given the key.
        If key has been read before, this call would simply
        return the cached value. Use reload to force reloading
        from DB.

        :param key: str: key to DB
        :param reload:  Optionally force reloading of the object
                        from db (Default value = False)
        :returns: The object from DB

        """
        path = self._get_full_path(key)
        res = None if reload else self._cache.get(path)
        if not res:
            res = self._deserialise(self.get_raw(path))

        self._cache[key] = res
        return res

    def __setitem__(self, key: str, value):
        """
        Set data from the DB based on key

        :param key: str:  The key to set.
        :param value: str:  The python object
        """
        self._cache[key] = value
        self.set_raw(self._get_full_path(key), self._serialise(value))

    def get_raw(self, key: str):
        """
        Get data from the DB based on key.

        This is to be implemented by derived class.

        :param key: str:  The key to get, including base_path.
        :returns: str: The raw, pickled data.
        """
        raise NotImplementedError()

    def set_raw(self, key: str, value):
        """
        Set data from the DB based on key.

        This is to be implemented by derived class.

        :param key: str:  The key to set, including base_path.
        :param value: The raw, pickled data.
        """
        raise NotImplementedError()

    def delete_raw(self, key: str):
        """
        Delete data from the DB based on key

        This is to be implemented by derived class.

        :param key: str:  The key to get, including base_path.

        """
        raise NotImplementedError()

    def delete(self, key: str):
        """
        Delete data from the DB based on key

        :param key: str:  The key to get
        """
        del self._cache[key]
        self.delete_raw(self._get_full_path(key))

    def new(self, class_path: str, key: str, **kwargs):
        """
        Create a new object on the DB.
        The object is not persisted until obj.put() is called.

        :param class_path: str: path to the class. i.e. path.to.module.MyClass
        :param key: str: The key to persist on the DB
        :param kwargs: kwargs passed into constructor of the class
        :returns: an obj of type defined by class_path
        """
        module_path, class_name = class_path.rsplit('.', 1)
        m = importlib.import_module(module_path)
        cls = getattr(m, class_name)
        return cls(self, key, **kwargs)

    def __repr__(self):
        """
        The representation of the db.

        i.e. <kydb.RedisDB redis://my-redis-host/source>
        """
        return f'<{type(self).__name__} {self.url}>'
