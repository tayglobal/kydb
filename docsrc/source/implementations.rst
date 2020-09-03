.. _implementations-page:

DB Implementations
==================

DynamoDB
--------

::

    db = kydb.connect('dynamodb://kydb')

Redis
-----

::

    db = kydb.connect('redis://cache.epythoncloud.io:6379')

Or simply::

    db = kydb.connect('redis://cache.epythoncloud.io')

S3
--

::

    db = kydb.connect('s3://my-kydb-bucket')

In-Memory
---------

::

    db = kydb.connect('memory://cache001')

HTTP/HTTPS
----------

::

    db = kydb.connect('http://my-source-host') # HTTP
    db = kydb.connect('https://my-source-host') # HTTPS
    
File system
-----------

::

    db = kydb.connect('files://tmp/foo/bar')
