from abc import ABC


class KYDBInterface(ABC):
    """The interface that all KYDB adheres to

    This class and all derived classes are never instantiated directly.

Instead use the ``connect``. i.e.::

    import kydb

    db = kydb.connect('dynamodb://my-table')

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
        raise NotImplementedError()

    def set(self, key: str, value, system_obj=False):
        """Set data from the DB based on key

        :param key: str:  The key to set.
        :param value:  The python object
        :param system_obj: bool: True if a system object

        Same as __setitem__ except it can write system objects
        i.e. object with a (.) dot prefix.

        Note: only use this if you know what you're doing
        """
        raise NotImplementedError()

    def list_dir(self, folder: str, include_dir=True, page_size=200):
        """ List the folder

        :param folder: The folder to lsit
        :parm include_dir: include subfolders
        :parm page_size: The number of items to fetch at a time from DB
                         The result would be identical, only controls
                         performance

        Note Folders always ends with ``/``
        Objects does not
        """
        raise NotImplementedError()

    def ls(self, folder: str, include_dir=True):
        """ Similar to list_dir, but returns a list (not generator)

        :param folder: The folder to lsit
        :parm include_dir: include subfolders
        :parm page_size: The number of items to fetch at a time from DB
                         The result would be identical, only controls
                         performance

        Note Folders always ends with ``/``
        mbjects does not
        """
        raise NotImplementedError()

    def delete(self, key: str):
        """
        Delete a key from the db.
        To be implemented by derived class

        :param key: str:  The key to delete.

example::

    db.delete(key) # Deletes data with key
        """
        raise NotImplementedError()

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

.. warning::

    Be careful when using this.
    For example ``rm_tree('/')`` would wipe out the entire database!

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
        raise NotImplementedError()

    def exists(self, key) -> bool:
        """
        Check if a key exists in the DB

        :param key: the key
        :returns: True if key exists, False otherwise.

        Example:

::

    db['/my/key'] = 123
    db.exists('/my/key') # returns True

        """
        raise NotImplementedError()

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
        raise NotImplementedError()

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
        raise NotImplementedError()

    def mkdir(self, folder: str):
        """ Make a directory (recursively if required)

        :param folder: The folder path.

example::

    db.mkdir('/foo/bar')
    db.ls('/foo') # returns ['bar/']

        """

    def is_dir(self, folder: str) -> bool:
        """ Is this a directory?

        :param folder: Returns True if is directory

example::

    db.mkdir('/foo/bar')
    db.is_dir('/foo/bar') # returns True

        """
        raise NotImplementedError()

    def cache_context(self) -> 'KYDBInterface':
        """ returns the cache context

        See :ref:`Cache Context`
        """
        raise NotImplementedError()

    def __repr__(self):
        """ The representation of the db.

        kydb.connect('s3://my-db')
        # displays <S3DB s3://my-db>

        kydb.connect('redis://my-cache;dynamodb://my-db')
        # displays <UnionDB redis://my-cache;dynamodb://my-db>
        """
        raise NotImplementedError()

    def upload_objdb_config(self, config):
        """Upload ObjDB config to KYDB

        :param config: The config dict

           This should only need to be done when new classes are registered or
           existing ones changes path.

::

    db = kydb.connect('memory://decorated_py_obj')

    db.upload_objdb_config({
        'Greeter': {
            'module_path': 'path.to.module',
            'class_name': 'Greeter'
        }
    })
        """
        raise NotImplementedError()
