KYDB (Kinyu Database)
=====================

.. image:: _static/images/GitHub-Mark-32px.png 
    :target: `GitHub`_
    
.. _GitHub: https://github.com/tayglobal/kydb

Introduction
------------

An abstraction layer for NoSQL Database clients.

 * Simple factory. A single URL would define the database or union.
 
 * Caching
 
 * Union: i.e. multiple databases where:
 
   * Read would look for the object in order
   
   * Write always writes to the first (front) db
   
 * Serialisable objects. Any python object can be stored in the DB with
 
   * With option to decorate which property to persist.
                         

Find out more
-------------
   
.. toctree::
   install
   examples
   implementations
   api
   base_path
   union
   cache_context
   developer
