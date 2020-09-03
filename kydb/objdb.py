from .exceptions import DbObjException
from .dbobj import IS_DB_OBJ
import importlib
import pickle


DBOBJ_CONFIG_PATH = '.configs/objdb'


class ObjDBMixin:
    """
    A Mixin class that BaseDB inherit from.

    This provides functionality for decorated python objects
    to be stored in KYDB
    """

    def upload_objdb_config(self, config):
        """Upload ObjDB config to KYDB

           This should only need to be done when new classes are registered or existing ones changes path.

::

    db = kydb.connect('memory://decorated_py_obj')

    db.upload_objdb_config({
        'Greeter': {
            'module_path': 'path.to.module',
            'class_name': 'Greeter'
        }
    })
        """
        self[DBOBJ_CONFIG_PATH] = config

    def _get_dbobj_config(self, class_name: str):
        try:
            config = self[DBOBJ_CONFIG_PATH]
        except KeyError:
            raise DbObjException('Missing config file '
                                 + DBOBJ_CONFIG_PATH
                                 + 'Please ensure it exists in ' + str(self))

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

    @staticmethod
    def write_dbobj(obj):
        data = {
            IS_DB_OBJ: True,
            'meta': {
                'key': obj.key,
                'class_name': obj.class_name,
            },
            'data': obj.get_stored_dict()
        }
        obj.db.set_raw(obj.key, pickle.dumps(data))
