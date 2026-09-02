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
    #. An index ``folder-index`` with partition key ``folder``, projection
       ``INCLUDE`` with non-key attributes ``mtime`` and ``ctime``
    #. An index ``folder-time-index`` with partition key ``folder`` and
       sort key ``mtime`` (Number), projection ``INCLUDE`` with
       non-key attribute ``ctime``

    Only the first two are needed to read and write. ``folder-time-index``
    serves :meth:`~kydb.interface.KYDBInterface.folder` /
    :meth:`~kydb.interface.KYDBInterface.recent` recency queries, so an
    existing table keeps working on upgrade without any schema change --
    those queries raise ``IndexNotSupported`` until the index is added
    (or fall back to a scan with ``allow_scan=True``).

    No data migration is needed either. Objects written before the index
    existed carry no ``mtime``, and are reported at ``mtime == 0``,
    ordered after everything indexed; each one moves into place the first
    time it is rewritten.


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
