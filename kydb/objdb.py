from .exceptions import DbObjException
from .dbobj import IS_DB_OBJ
import importlib
import pickle


DBOBJ_CONFIG_PATH = '/.configs/objdb'


class ObjDBMixin:
    """
    A Mixin class that BaseDB inherit from.

    This provides functionality for decorated python objects
    to be stored in KYDB
    """

    def upload_objdb_config(self, config):
        """ Implements upload_objdb_config from KYDBInterface """
        self.set(DBOBJ_CONFIG_PATH, config, system_obj=True)

    def _get_dbobj_config(self, class_name: str):
        try:
            config = self[DBOBJ_CONFIG_PATH]
        except KeyError:
            raise DbObjException('Missing config file '
                                 + DBOBJ_CONFIG_PATH
                                 + '. Please ensure it exists in ' + str(self))

        cfg = config[class_name]
        if not ('module_path' in cfg and 'class_name' in cfg):
            raise DbObjException('config must have module_path '
                                 'and class_name, instead got '
                                 + str(config))

        return cfg

    def db_obj_new(self, class_name: str, key: str, kwargs: dict):
        config = self._get_dbobj_config(class_name)
        m = importlib.import_module(config['module_path'])
        cls = getattr(m, config['class_name'])
        return cls(self, class_name, key, **kwargs)

    @staticmethod
    def is_data_dbobj(data) -> bool:
        return isinstance(data, dict) and data.get(IS_DB_OBJ, False)

    @staticmethod
    def is_dbobj(obj) -> bool:
        return getattr(obj, IS_DB_OBJ, False)

    def read_dbobj(self, data):
        meta = data['meta']
        return self.db_obj_new(meta['class_name'],
                               meta['key'], data['data'])

    def write_dbobj(self, obj, index=None):
        """ Serialise a :class:`kydb.dbobj.DbObj` and store it.

        ``index`` is the validated ``{name: int or None}`` mapping from
        :meth:`kydb.base.BaseDB.set`, so a ``DbObj`` can carry business
        keys exactly as a plain value can::

            booking = db.new('Booking', '/signups/anna')
            db.set(booking.key, booking, index={'class_date': 20260905})

        The value is pickled here rather than by ``BaseDB._serialise``
        -- a ``DbObj`` is stored as its ``get_stored_dict()`` plus the
        metadata needed to rebuild it -- which is why the index has to
        be threaded through this path explicitly. It is passed on only
        when there is something to pass, so a backend that never
        supports index values keeps being called with the two-argument
        ``set_raw`` it has always implemented.
        """
        data = {
            IS_DB_OBJ: True,
            'meta': {
                'key': obj.key,
                'class_name': obj.class_name,
            },
            'data': obj.get_stored_dict()
        }
        path = self._get_full_path(obj.key)
        raw = pickle.dumps(data)
        if index:
            obj.db.set_raw(path, raw, index=index)
        else:
            obj.db.set_raw(path, raw)
