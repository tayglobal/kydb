IS_DB_OBJ = '__is_dbobj__'


class StoredValue:
    def __init__(self, default_func):
        self._default_func = default_func
        self._value = None

    def get_default(self, obj):
        return self._default_func(obj)

    def setvalue(self, value):
        self._value = value

    def clear(self):
        self._value = self.get_default(self)

    def __call__(self):
        return self._value


class DbObj:
    def __init__(self, db, class_name: str, key: str, **kwargs):
        self.class_name = class_name
        self.db = db
        self.key = key

        self._stored_attrs = [x for x in dir(self) if isinstance(
            getattr(self, x), StoredValue)]

        for attr in self._stored_attrs:
            sv = getattr(self, attr)
            v = kwargs.get(attr)
            if not v:
                v = sv.get_default(self)

            sv.setvalue(v)

    def get_stored_dict(self):
        return {x: getattr(self, x)() for x in self._stored_attrs}

    def write(self):
        self.db[self.key] = self


def stored(f):
    return StoredValue(f)
