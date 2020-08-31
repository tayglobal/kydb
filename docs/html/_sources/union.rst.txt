Union
=====

The URL used on *connect* can be a semi-colon separated string.

This would create a Union Database.

Connecting::

    db = kydb.connect('memory://unittest;s3://my-unittest-fixture')
    
OR::

    db = kydb.connect('redis://hotfixes.epythoncloud.io:6379;dynamodb://my-prod-src-db')

Reading and writing::

    db1, db2 = db.dbs
    db1['/foo'] = 1
    db2['/bar'] = 2
    
    (db['/foo'], db['/bar']) # return (1, 2)
    
    # Although db2 has /foo, it is db1's /foo that the union returns
    db2['/foo'] = 3
    db['/foo'] # return 1
    
    # writing always happens on the front db
    db['/foo'] = 4
    db1['/foo'] # returns 4
    db2['/foo'] # returns 3