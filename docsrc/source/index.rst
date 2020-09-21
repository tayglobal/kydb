KYDB (Kinyu Database)
=====================

.. image:: _static/images/GitHub-Mark-32px.png 
    :target: `GitHub`_
    
.. _GitHub: https://github.com/tayglobal/kydb

Introduction
------------

An abstraction layer for NoSQL Database with features used in the financial services industry.

 * Simple factory. A single URL would define the database or union.
 
 * Filesystem-like heirachy for objects.
 
 * Caching and cache context.
 
 * Union: i.e. multiple databases where:
 
   * Read would look for the object in order
   
   * Write always writes to the first (front) db
   
 * Serialisable objects. Any python object can be stored in the DB with
 
   * With option to use decorators to give finer control over serialisation.
                         

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
