from kydb.exceptions import DbObjException
import importlib
import yaml
import pickle


DBOBJ_CONFIG_PATH = 'dbobj_config.yaml'
IS_DB_OBJ = '__is_dbobj__'


class DbObjManager:
    @staticmethod
    def upload_config(db, config):
        db[DBOBJ_CONFIG_PATH] = config

    @staticmethod
    def _get_dbobj_config(db, class_name: str):
        DBOBJ_CONFIG_PATH = 'dbobj_config.yaml'
        try:
            config_yaml = db[DBOBJ_CONFIG_PATH]
        except KeyError:
            raise DbObjException('Missing config file '
                                 + DBOBJ_CONFIG_PATH
                                 + 'Please ensure it exists in ' + str(db))

        config = yaml.safe_load(config_yaml)

        cfg = config[class_name]
        if not ('module_path' in cfg and 'class_name' in cfg):
            raise DbObjException('config must have module_path '
                                 'and class_name, instead got '
                                 + str(config))

        return cfg

    @classmethod
    def db_obj_new(cls, db, class_name: str, key: str, kwargs: dict):
        config = cls._get_dbobj_config(db, class_name)
        m = importlib.import_module(config['module_path'])
        klass = getattr(m, config['class_name'])
        return klass(db, class_name, key, **kwargs)

    @staticmethod
    def is_data_dbobj(data) -> bool:
        return isinstance(data, dict) and data.get(IS_DB_OBJ, False)

    @staticmethod
    def is_dbobj(obj) -> bool:
        return getattr(obj, IS_DB_OBJ, False)

    @classmethod
    def read_dbobj(cls, db, data):
        meta = data['meta']
        return cls.db_obj_new(db, meta['class_name'],
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


class StoredValue:
    def __init__(self, default_func):
        self._default_func = default_func
        self._value = None

    def get_default(self, obj):
        return self._default_func(obj)

    def setvalue(self, value):
        self._value = value

    def __call__(self):
        return self._value


def dbobj(cls):
    stored_attrs = [x for x in dir(cls) if isinstance(
        getattr(cls, x), StoredValue)]

    def get_stored_dict(self):
        return {x: getattr(self, x)() for x in self._stored_attrs}

    def init(self, db, class_name: str, key: str, **kwargs):
        self.class_name = class_name
        self.db = db
        self.key = key
        for attr in self._stored_attrs:
            sv = getattr(self, attr)
            v = kwargs.get(attr)
            if not v:
                v = sv.get_default(self)

            sv.setvalue(v)

    def write(self):
        self.db[self.key] = self

    extras = {
        IS_DB_OBJ: True,
        '__init__': init,
        '_stored_attrs': stored_attrs,
        'get_stored_dict': get_stored_dict,
        'write': write
    }

    return type(cls.__name__, (cls, ), extras)


def stored(f):
    return StoredValue(f)
