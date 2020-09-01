KYDB (Kinyu Database)
=====================

+-----------------+----------------+---------------+------------+
| :ref:`genindex` |:ref:`modindex` | :ref:`search` | `GitHub`_. |
+-----------------+----------------+---------------+------------+

.. _GitHub: https://github.com/tayglobal/kydb

Introduction
------------

An abstraction layer for NoSQL Database clients.

 * Simple factory. A single URL would define the database or union.
 
 * Caching
 
 * Union: i.e. multiple databases where:
 
   * Read would look for the object in order
   
   * Write always writes to the first (front) db


Installation
------------

.. code-block:: bash

    pip3 install kydb

   
What does it look like?
-----------------------

Connect to KYDB with AWS S3 as the implementation::

    import kydb
    db = kydb.connect('s3://my-kydb-bucket')

Writing to DB::

    key = '/mytest/foo'
    db[key] = 123

Reading from DB::

    db[key] # returns 123
    
Find out more
-------------

   
.. toctree::
   examples
   implementations
   api
   base_path
   union
   developer
