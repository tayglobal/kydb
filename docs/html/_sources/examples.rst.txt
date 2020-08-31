Example
=======
   
Connect to KYDB with AWS S3 as the implementation::

    from kinyu.db.api import kydb
    db = kydb.connect('s3://my-kydb-bucket')

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

A bit more complicated types::

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
