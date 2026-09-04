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
    #. Per business key written with ``set(..., index={'<name>': 1})``,
       an index ``folder-<name>-index`` with partition key ``folder`` and
       sort key ``<name>`` (Number), projection ``INCLUDE`` with non-key
       attributes ``mtime`` and ``ctime`` -- for example
       ``folder-class_date-index`` for ``by('class_date')``. ``<name>``
       must also appear in the table's ``AttributeDefinitions`` as a
       Number.

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

    The business-key indexes are equally optional, and are never needed to
    *write* one: ``update_item`` just sets an attribute. An application can
    start recording values before the index exists, and they are all there
    when it is added. Until then only ``by('<name>')`` fails, with an
    ``IndexNotSupported`` naming the index to create; ``allow_scan=True``
    cannot serve it, because ``folder-index`` projects no user attribute.
    Each of these indexes is sparse, so an object written with no value
    for it simply is not in it. DynamoDB allows 20 GSIs per table, two of
    which are kydb's own, leaving 18 business keys.


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
