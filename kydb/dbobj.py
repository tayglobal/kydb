IS_DB_OBJ = '__is_dbobj__'


class StoredValue:
    """Any function decorated with @kydb.stored would
        turn the function into a StoredValue

    For example, the method ``name`` would turn into a
        ``StoredValue``  because of the decorator.

::

    class Greeter(kydb.DbObj):

        @kydb.stored
        def name(self):
            return 'John'
    """

    def __init__(self, default_func):
        self._default_func = default_func
        self._value = None

    def get_default(self, obj):
        return self._default_func(obj)

    def setvalue(self, value):
        """Set the value
        :params value: the value to set

example::

    greeter = db.new('Greeter', key)
    greeter.name.setvalue('Jane')
    greeter.name() # returns 'Jane'

        """
        self._value = value

    def clear(self):
        """Clears the value.

        This sets the value back to default.

example::

    greeter = db.new('Greeter', key) # Default is 'John'
    greeter.name.setvalue('Jane')
    greeter.name() # returns 'Jane'
    greater.clear()
    greeter.name() # returns 'John'

        """
        self._value = self.get_default(self)

    def __call__(self):
        return self._value


class DbObj:
    """Derive this class to enable it for Python Object DB API

    example

::

    class Greeter(kydb.DbObj):

        def init(self):
            self.greet_count = 0

        @kydb.stored
        def name(self):
            return 'John'

        def greet(self):
            self.greet_count += 1
            return 'Hello ' + self.name()

    """

    def __init__(self, db, class_name: str, key: str, **kwargs):
        self.class_name = class_name
        self.db = db
        self.key = key

        self._stored_attrs = [x for x in dir(self) if isinstance(
            getattr(self, x), StoredValue)]

        setattr(self, IS_DB_OBJ, True)

        for attr in self._stored_attrs:
            sv = getattr(self, attr)
            v = kwargs.get(attr)
            if not v:
                v = sv.get_default(self)

            sv.setvalue(v)

        self.init()

    def init(self):
        """ Implement this to add additional initialisation

        example

::

    class MyClass(kydb.DbObj):
        def init(self):
            self._some_network_resource = get_network_resource()
        """
        pass

    def get_stored_dict(self):
        """The dictionary to be serialised and stored in DB.

        Example:

::

    class Greeter(kydb.DbObj):

        def init(self):
            self.greet_count = 0 ##  Not persisted

        @kydb.stored
        def name(self):
            return 'John'

        def greet(self):
            self.greet_count += 1
            return 'Hello ' + self.name()

    greeter.get_stored_dict() # returns {'name': 'Mary'}
    """
        return {x: getattr(self, x)() for x in self._stored_attrs}

    def write(self):
        self.db[self.key] = self

    def delete(self):
        self.db.delete(self.key)


def stored(f):
    return StoredValue(f)
