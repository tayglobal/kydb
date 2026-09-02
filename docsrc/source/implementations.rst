.. _implementations-page:

DB Implementations
==================

S3
--

::

    db = kydb.connect('s3://my-kydb-bucket')

DynamoDB
--------

::

    db = kydb.connect('dynamodb://kydb')
    
.. note::

    The dynamodb must have:

    #. ``path`` as primary key
    #. An index ``folder-index`` with partition key ``folder``
    #. An index ``folder-time-index`` with partition key ``folder`` and
       sort key ``mtime`` (Number), projected ``KEYS_ONLY``


Redis
-----

::

    db = kydb.connect('redis://cache.epythoncloud.io:6379')

Or simply::

    db = kydb.connect('redis://cache.epythoncloud.io')

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
