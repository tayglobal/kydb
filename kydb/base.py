from abc import ABC
import pickle
from typing import Tuple
from .objdb import ObjDBMixin
from .cache_context import cache_context


class IDB(ABC):
    """The interface that all KYDB adheres to
    """

    def __getitem__(self, key: str):
        """Get data from the DB based on key

        To be implemented by derived class

        :param key: str:  The key to get.

example::

    db[key] # returns the object with key

        """
        raise NotImplementedError()

    def __setitem__(self, key: str, value):
        """Set data from the DB based on key
        To be implemented by derived class

        :param key: str:  The key to set.

example::

    db[key] = value # sets key to value
        """
        raise NotImplementedError()

    def list_dir(self, folder: str, include_dir=True):
        """ List the folder

        :param folder: The folder to lsit
        :parm include_dir: include subfolders

        Note Folders always ends with ``/``
        Objects does not
        """
        NotImplementedError()

    def ls(self, folder: str, include_dir=True):
        """ See list_dir """
        self.list_dir(folder, include_dir)

    def delete(self, key: str):
        """
        Delete a key from the db.
        To be implemented by derived class

        :param key: str:  The key to delete.

example::

    db.delete(key) # Deletes data with key
        """
        raise NotImplementedError()

    def new(self, class_name: str, key: str, **kwargs):
        """
        Create a new object on the DB.
        The object is not persisted until obj.write() is called.

        :param class_name: str: name of the class.
                                This name must be in the config registry
        :param key: str: The key to persist on the DB
        :param kwargs: the stored attributes to set on the obj
        :returns: an obj of type defined by class_name

example::

    obj = db.new('MyClass', key, foo=3)
        """


class BaseDB(IDB, ObjDBMixin):
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
        """ Deserialise the data

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

        :param key: str:  The key to get.

example::

    db[key] # returns the object with key
        """
        return self.read(key)

    def refresh(self, key=None):
        """
        Flush the cache

        :param key: Optionally choose which key to flush (Default value = None)

example::

    obj = db.new('MyClass', key)
    obj.write()
    db[key] # read from cache
    db.refresh() # Or db.refresh(key)
    db[key] # read from DB

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

example::

    obj = db.new('MyClass', key)
    obj.write()
    db.read(key) # read from cache
    db.read(key, reload=True) # Force loading from DB

        """
        path = self._get_full_path(key)
        res = None if reload else self._cache.get(path)
        if not res:
            res = self._deserialise(self.get_raw(path))
            if self.is_data_dbobj(res):
                res = self.read_dbobj(res)

        self._cache[key] = res
        return res

    def mkdir(self, folder: str):
        if not folder or folder == '/':
            raise ValueError('Cannot make folder: ' + folder)

        folders = self._ensure_slashes(folder)[1:-1].split('/')
        self._mkdir(folders)

    def _mkdir(self, folder: str):
        raise NotImplementedError()

    def is_dir(self, folder: str) -> bool:
        """ Is this a directory?

        :param folder: Returns True if is directory
        """
        raise NotImplementedError()

    def __setitem__(self, key: str, value):
        """
        Set data from the DB based on key

        :param key: str:  The key to set.
        :param value:  The python object

example::

    db[key] = value # sets key to value

Note: key cannot have any component that has
a dot (.) prefix.

i.e. the below are illegal and would raise KeyError

::

    db['.foo'] = 123 # raises KeyError
    db['/.foo'] = 123 # raises KeyError
    db['/my/folder/.foo'] 123 # raises KeyError
    db['/my-folder/.another-folder/foo'] = 123 # raises KeyError

        """
        self.set(key, value)

    def set(self, key: str, value, system_obj=False):
        """Set data from the DB based on key

        :param key: str:  The key to set.
        :param value:  The python object
        :param system_obj: bool: True if a system object

        Same as __setitem__ except it can write system objects
        i.e. object with a (.) dot prefix.

        Note: only use this if you know what you're doing
        """
        if not system_obj and \
                any(x for x in key.rsplit('/') if x.startswith('.')):
            raise KeyError('Cannot have dot (.) prefix in path, '
                           'got :' + key)

        self._cache[key] = value
        if self.is_dbobj(value):
            self.write_dbobj(value)
        else:
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

        :param key: str:  The key to delete

        """
        raise NotImplementedError()

    def delete(self, key: str):
        """
        Delete data from the DB based on key

        :param key: str:  The key to get

example::

    db.delete(key) # Deletes data with key
        """
        if key in self._cache:
            del self._cache[key]

        self.delete_raw(self._get_full_path(key))

    def rmdir(self, key: str):
        """
        Delete folder based on key

        :param key: str:  The key to folder to delete

example::

    db.rmdir(folder) # Deletes folder with key
        """
        raise NotImplementedError()

    def rm_tree(self, key: str):
        """ recursively delete folder

        WARNING: Be careful when using this
        """
        if not self.is_dir(key):
            raise KeyError('{} is not a directory'.format(key))

        folder = self._ensure_slashes(key)
        objs = list(self.list_dir(key))
        for obj in objs:
            path = folder + obj
            if obj.endswith('/'):
                self.rm_tree(path)
            else:
                self.delete(path)

        self.rmdir(key)

    def new(self, class_name: str, key: str, **kwargs):
        """
        Create a new object on the DB.
        The object is not persisted until obj.write() is called.

        :param class_name: str: name of the class.
                                This name must be in the config registry
        :param key: str: The key to persist on the DB
        :param kwargs: the stored attributes to set on the obj
        :returns: an obj of type defined by class_name

example::

    obj = db.new('MyClass', key, foo=3)
        """
        return self.db_obj_new(class_name, key, kwargs)

    def cache_context(self) -> IDB:
        """ returns the cache context

        See :ref:`Cache Context`
        """
        return cache_context(self)

    def __repr__(self):
        """
        The representation of the db.

        i.e. <kydb.RedisDB redis://my-redis-host/source>
        """
        return f'<{type(self).__name__} {self.url}>'

    @staticmethod
    def _ensure_slashes(s: str):
        """Ensures s starts with / and ends with /

        :param s: The string to pass in

        Add slash in front or behind if needed
        """
        if not s.endswith('/'):
            s += '/'

        if not s.startswith('/'):
            s = '/' + s

        return s
