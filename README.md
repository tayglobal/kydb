# Introduction

This is just a simple wrapper for various NoSQL Database. Currently it offers:

 * Simple factory. A single URL would define the database or union.
 * Caching
 * Union: i.e. multiple databases where:
   * Read would look for the object in order
   * Write always writes to the first (front) db

   
## What does it look like?

Connect to KYDB with AWS S3 as the implementation::

    from kinyu.db.api import kydb
    db = kydb.connect('s3://my-kydb-bucket')

Writing to DB::

    key = '/mytest/foo'
    db[key] = 123

Reading from DB::

    db[key] # returns 123
    
    
