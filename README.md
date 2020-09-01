# Introduction

An abstraction layer for NoSQL Database clients.

 * Simple factory. A single URL would define the database or union.
 * Caching
 * Union: i.e. multiple databases where:
   * Read would look for the object in order
   * Write always writes to the first (front) db

See [Documentation](https://kydb.readthedocs.io/en/latest/).

## Installation

```bash
pip3 install kydb
```

   
## What does it look like?

Connect to KYDB with AWS S3 as the implementation

```python
import kydb
db = kydb.connect('s3://my-kydb-bucket')
```

Writing to DB

```python
key = '/mytest/foo'
db[key] = 123
```

Reading from DB

```python
db[key] # returns 123
```    
    
