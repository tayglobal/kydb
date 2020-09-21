# Introduction

An abstraction layer for NoSQL Database with features used in the financial services industry.

 * Simple factory. A single URL would define the database or union.
 
 * Filesystem-like heirachy for objects.
 
 * Caching and cache context.
 
 * Union: i.e. multiple databases where:
 
   * Read would look for the object in order
   
   * Write always writes to the first (front) db
   
 * Serialisable objects. Any python object can be stored in the DB with
 
   * With option to use decorators to give finer control over serialisation.


See [Documentation](https://kydb.readthedocs.io/en/latest/).

