from abc import ABC
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - import for type checking only
    from .query import FolderQuery
from .exceptions import IndexNotSupported


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

    def set(self, key: str, value, system_obj=False, *, index=None):
        """Set data from the DB based on key

        :param key: str:  The key to set.
        :param value:  The python object
        :param system_obj: bool: True if a system object
        :param index: dict: Optional, keyword-only mapping of index name
                      to index value, recording business keys the object
                      can later be queried and ordered by::

                          db.set('/signups/anna', booking,
                                 index={'class_date': 20260905})

                          db.folder('/signups').by('class_date') \\
                            .since(20260905).until(20260905).items()

        Same as __setitem__ except it can write system objects
        i.e. object with a (.) dot prefix, and can record index values.

        Note: only use this if you know what you're doing

**Index values are ``int``, and only ``int``.**
A date is ``int(d.strftime('%Y%m%d'))``, a datetime an epoch value, a
rank or priority already numeric. Restricting v1 to integers buys one
comparison semantic across every backend: a Redis sorted-set score is a
double, so ordering strings would need a parallel lexicographic read
path with different bound semantics on the same builder. ``bool`` is
rejected explicitly -- it is an ``int`` subclass, and a boolean in a
business ordering is a mistake, not an intent. A bad value raises
``TypeError`` naming the index; a bad or reserved name (``path``,
``folder``, ``contents``, ``mtime``, ``ctime``, or anything not
matching ``[A-Za-z][A-Za-z0-9_]*``) raises ``ValueError``. Both are
raised before anything is written.

**A rewrite preserves index values it does not mention.**
::

    db.set('/signups/anna', rec, index={'class_date': 20260905})
    db.set('/signups/anna', updated_rec)                       # still 20260905
    db.set('/signups/anna', rec, index={'class_date': None})   # now cleared

The alternative -- an unmentioned index is cleared -- would mean every
incidental rewrite anywhere in an application silently drops the object
out of the index. Clearing is therefore explicit, with ``None``.

**Backends that cannot store index values raise.**
Files and S3 have nowhere to put a caller-supplied attribute that the
rest of kydb models, so ``set(index=...)`` raises
``IndexNotSupported`` there rather than accepting the value and
discarding it. Accepting-and-discarding is the one behaviour that lets a
bug reach production undetected: the writes all succeed, and only the
query -- later, elsewhere -- comes back empty.
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

    def folder(self, folder: str, allow_scan: bool = False) -> 'FolderQuery':
        """ Build a lazy recency (or other index) query on a folder.

        :param folder: The folder to query. Same folder-relative
                       semantics as ``list_dir``.
        :param allow_scan: Opt into an O(n) client-side scan-and-sort on
                       backends with no server-side ordering index
                       (Memory, Files, S3), and on DynamoDB when the
                       table has no ``folder-time-index``. Ignored on
                       Redis, which is always native, and on backends
                       that are unsupported outright (HTTP/HTTPS), which
                       always raise regardless of this flag.
        :returns: A :class:`kydb.query.FolderQuery` -- a lazy, immutable
                  query builder. Each chained call returns a new query,
                  so the object can be safely reused/branched.

        Note there is no ``include_dir`` option: directories are not
        indexed, so recency queries only ever return objects.

        Backends without a server-side ordering index raise
        ``IndexNotSupported`` unless ``allow_scan=True`` is passed.

        **Objects with no timestamp.** An object written before the
        index existed -- or while it was switched off in config -- has
        no recorded ``mtime``. It is still returned, reported at
        ``mtime == ctime == 0``: after every indexed object under
        ``desc()``, before every one under ``asc()``, and excluded by any
        ``since(ts)`` with ``ts > 0``. No migration is needed, and each
        such object moves into place the first time it is rewritten.

        **User indexes are strictly sparse -- no epoch tail.** An object
        with no value for the queried *user* index does not appear at
        all, in either direction, and no fallback read of ``list_dir``
        is made. This is the deliberate difference from the ``mtime``
        behaviour described above: a missing write time still means
        something -- *older than anything tracked* -- whereas a missing
        ``class_date`` does not mean the object is a signup for an
        infinitely distant past class, it means it is not a signup at
        all. Fabricating a position for it would put junk in every
        business query. A pleasant consequence is that user indexes are
        *cheaper* than ``mtime``: ``asc()`` carries no O(folder) penalty,
        because there is no epoch tail to discover first.

        **Switching the index off.** Setting ``mtime-index: false`` in
        the per-db kydb config stops the index being maintained on
        write. ``folder()`` and ``recent()`` then raise
        ``IndexNotSupported``, and ``allow_scan=True`` does not override
        that -- with nothing recording timestamps there is nothing to
        sort by.

        It gates ``mtime`` and only ``mtime``. The check runs when the
        query runs, not when ``folder()`` builds it, because ``by()`` is
        chained onto the object ``folder()`` returns -- so ``folder()``
        cannot yet know which index was wanted. ``by('<user index>')``
        keeps working: a business key is a value the caller supplied and
        stored on the object, not a timestamp the backend stamped, so
        the setting has nothing to say about it. ``set(index={...})``
        records values throughout for the same reason.

example::

    db.folder('/my/folder').by('mtime').desc().limit(10)   # names, newest first
    db.folder('/my/folder').by('mtime').since(ts).items()  # (name, value) pairs
    db.folder('/my/folder').by('mtime').desc().entries()   # .key, .mtime, .ctime
    db.folder('/my/folder', allow_scan=True).by('mtime')   # client-side scan

    # a closed range -- both bounds inclusive, so this is one exact day
    db.folder('/signups').by('class_date').since(20260905).until(20260905)

        """
        raise IndexNotSupported(
            f'{type(self).__name__} does not support folder()/recent() '
            'recency queries (no server-side ordering index)')

    def recent(self, folder: str, limit: int = None,
               allow_scan: bool = False):
        """ The most recently modified objects in a folder, newest first.

        :param folder: The folder to query.
        :param limit: Optionally cap the number of results.
        :param allow_scan: Same meaning as on :meth:`folder` -- opt into
                       an O(n) client-side scan-and-sort on backends
                       with no server-side ordering index.
        :returns: A lazy generator of names (``str``), newest first.

        Sugar for
        ``db.folder(folder, allow_scan=allow_scan).by('mtime').desc().limit(limit)``.
        Raises ``IndexNotSupported`` on backends without a server-side
        ordering index, unless ``allow_scan=True`` is passed.

        See :meth:`folder` for how objects with no recorded timestamp are
        ordered, and for the ``mtime-index`` config setting.

example::

    db.recent('/my/folder', limit=10)
    db.recent('/my/folder', limit=10, allow_scan=True)

        """
        raise IndexNotSupported(
            f'{type(self).__name__} does not support folder()/recent() '
            'recency queries (no server-side ordering index)')

    def reindex(self, folder: str) -> int:
        """Add missing recency timestamps to objects in ``folder``.

        :param folder: The folder to reindex. Subfolders are not traversed.
        :returns: The number of objects newly added to the recency index.

        Existing ``mtime`` and ``ctime`` values are preserved. Legacy objects
        have no recoverable write time, so this deliberately records the time
        of reindexing. That moves them ahead of genuinely older indexed
        objects; callers should normally keep the truthful epoch-tail
        behaviour and use this only when migration-day ordering is preferred.
        """
        raise IndexNotSupported(
            f'{type(self).__name__} does not maintain a recency index')

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

    def clear_cache(self):
        """Clear the cache

        This is useful when you want to clear the cache from memory

        Note: This is different to CacheDB where the cache is a database
        """
        raise NotImplementedError()
