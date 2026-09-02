Example
=======
   
Connecting
----------

Connect to KYDB with AWS S3 as the implementation.

Note there are other implementations. See :ref:`implementations-page`

::

    import kydb
    db = kydb.connect('s3://my-kydb-bucket')
    
Literals as Data
----------------

Writing to DB::

    key = '/mytest/foo'
    db[key] = 123

Reading from DB::

    db[key] # returns 123

Deleting::

    db.delete[key]

The above actually performed no read from the S3 bucket because of the cache.
Let's force reload when we read::

    db.read(key, reload=True) # returns 123
    
Vanilla Python Objects
----------------------

Any pickleable object can be stored in the KYDB::

    key = '/mytest/bar'
    val = {
        'my_int': 123,
        'my_float': 123.456,
        'my_str': 'hello',
        'my_list': [1, 2, 3],
        'my_datetime': datetime.now()
    }
    db[key] = val
    
    assert db.read(key, reload=True) == val
    

Object Hierarchy
================

Document-oriented databases typically does not have a concept of filesystem-like hierarchy.
Depending on the implementation there are ways to emulate the behavior.
There may even be needs for some meta data objects to support the idea of an empty folder.
We will not go into these implementation details here. However the below demonstrates the API.

An empty folder can be created using ``mkdir``:

::

    db.mkdir('/empty-folder')
    db.ls('/') # returns ['empty-folder/']

If an object is inserted inside some folder hierarchy, the folders would be created:

::

    db['/foo/bar/baz'] = 123
    db.ls('/') # returns ['empty-folder/', 'foo/']
    db.ls('/foo') # returns ['bar/'] 
    

``rmdir`` can be used to remove an empty folder:

::

    db.rmdir('/empty-folder')
    db.ls('/') # returns ['foo/']
   
To recursively remove all contents of a folder similar to the UNIX command ``rm -rf`` use ``rm_tree``:

::

    db.rm_tree('/foo')
    db.ls('/') # []

Python Object DB
================

This requires a config file in the database to map class name to fully qualified path.

The advantage of this is:

  * When classes are refactored to a different folder, deserialising from DB still works.
  
  * Clean API to define what attribute should be serialised.
  
  * Provide default value and ability to reset back to that value.



Upload the config. This should only need to be done when new classes are registered or existing ones changes path.

::

    db = kydb.connect('memory://decorated_py_obj')
    
    db.upload_objdb_config({
        'Greeter': {
            'module_path': 'path.to.module',
            'class_name': 'Greeter'
        }
    })
    
Create a file ``path/to/module.py``

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

Let's use the Greeter to greet. Since we left the name as default, we will get ``Hello John`` from ``greet()``.

::

    key = '/hello-world/greeter001'
    greeter = db.new('Greeter', key)
    greeter.name() # returns 'John'
    greeter.greet() # returns 'Hello John'
    
    
If we initialise the name to ``Tony`` then ``greet()`` would return ``Hello Tony``

::

    greeter = db.new('Greeter', key, name='Tony')
    greeter.greet() # returns 'Hello Tony'
    
We can also set the name

::

    greeter.name.setvalue('Jane')
    greeter.name() # returns 'Jane'
    greeter.greet() # returns 'Hello Jane'
    
    
Notice the attribute ``greet_count`` which would increment when ``Greeter`` greets.

::

    greeter = db.new('Greeter', key, name='Mary')
    greeter.greet_count # return 1

However we want to persist only ``name``. we can check that is the case.

::

    greeter.get_stored_dict() # returns {'name': 'Mary'}
    
Now let's persist it and read it back.

::

    db[key] = greeter
    
    # read it back, dictionary format:
    mary = db[key]
    # Still returns 1 because of cache.
    mary.greet_count # Returns 1
    
    # Force relaoding of the object from DB and the count would be back to 0
    mary = db.read(key, reload=True)
    mary.greet_count # returns 0
    mary.name() # returns 'Mary'



