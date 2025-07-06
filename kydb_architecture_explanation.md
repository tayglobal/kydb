# How kydb Works: Architecture and Implementation Guide

## Overview

**kydb** (Kinyu Database) is a NoSQL database abstraction layer designed specifically for financial services applications. It provides a unified interface for multiple database backends while adding advanced features like caching, database unions, and object persistence.

## Core Architecture

### 1. Database Connection Factory (`api.py`)

The entry point is the `connect()` function which acts as a factory for database connections:

```python
import kydb
db = kydb.connect('redis://localhost:6379/0')
```

**URL Parsing Logic:**
- **Single Database**: `connect('redis://localhost:6379/0')` → Single database instance
- **Union Database**: `connect('redis://cache;s3://bucket')` → UnionDB with multiple backends
- **Cache Database**: `connect('redis://cache|s3://bucket')` → CacheDB with cache + persistence

**Database Type Resolution:**
- Extracts database type from URL prefix (e.g., 'redis' from 'redis://...')
- Dynamically imports the appropriate implementation from `kydb.impl.*`
- Maintains connection cache to avoid duplicate connections

### 2. Database Interface (`interface.py`)

`KYDBInterface` defines the contract that all database implementations must follow:

**Core Operations:**
- `__getitem__` / `__setitem__`: Dictionary-like access (`db[key] = value`)
- `read()` / `write()`: Explicit read/write operations
- `delete()`: Remove objects
- `exists()`: Check key existence

**Directory Operations:**
- `list_dir()` / `ls()`: List folder contents
- `mkdir()`: Create directories
- `rmdir()` / `rm_tree()`: Remove directories

**Object Management:**
- `new()`: Create new database objects
- `refresh()`: Clear cache
- `cache_context()`: Cache management

### 3. Base Database Implementation (`base.py`)

`BaseDB` provides the foundation for all database implementations:

**Key Features:**

#### Path Management
- **Base Path**: Each database can have a base path (e.g., `/app/data/`)
- **Full Path Resolution**: Converts relative keys to full paths
- **URL Parsing**: Extracts database name and base path from connection strings

#### Serialization
- **Pickle-based**: Uses Python's pickle for object serialization
- **Raw Data Handling**: Separates raw data operations from object handling
- **Database Object Support**: Special handling for custom database objects

#### Caching
- **In-Memory Cache**: `_cache` dictionary for frequently accessed objects
- **Cache Management**: `refresh()` and `clear_cache()` methods
- **Configurable**: Can be disabled or customized per implementation

#### Abstract Methods (Implemented by Backends)
- `get_raw()` / `set_raw()`: Raw data access
- `delete_raw()`: Raw data deletion
- `list_dir_raw()`: Directory listing
- `mkdir_raw()` / `rmdir_raw()`: Directory management

### 4. Database Backend Implementations (`impl/`)

Each database type has its own implementation:

#### Memory Database (`memory.py`)
- **Use Case**: Testing and development
- **Storage**: In-memory dictionary
- **Features**: Fast, no persistence

#### S3 Database (`s3.py`)
- **Use Case**: Object storage, large files
- **Backend**: AWS S3 via boto3
- **Features**: Scalable, durable, slow access

#### Redis Database (`redis.py`)
- **Use Case**: Caching, session storage
- **Backend**: Redis server
- **Features**: Fast, volatile, pub/sub

#### DynamoDB Database (`dynamodb.py`)
- **Use Case**: Document storage, web applications
- **Backend**: AWS DynamoDB
- **Features**: Managed, scalable, consistent

#### File System Database (`files.py`)
- **Use Case**: Local development, file-based storage
- **Backend**: Local filesystem
- **Features**: Simple, persistent, portable

### 5. Advanced Features

#### Union Database (`union.py`)
Combines multiple databases with intelligent routing:

**Read Strategy**: First-found wins
- Searches databases in order until key is found
- Faster databases should be listed first

**Write Strategy**: Always writes to front database
- All writes go to the first database in the union
- Ensures write consistency

**Example Usage:**
```python
db = kydb.connect('redis://fast-cache;s3://slow-storage')
db['key'] = 'value'  # Writes to Redis
result = db['key']   # Reads from Redis (fast)
```

#### Cache Database (`cache.py`)
Two-tier caching system:

**Architecture**: Cache + Persistence
- **Cache DB**: Fast access (Redis, Memory)
- **Persist DB**: Durable storage (S3, DynamoDB)

**Read Strategy**: Cache-first
1. Check cache database
2. If miss, read from persistent database
3. Write to cache for future access

**Write Strategy**: Write-through
1. Write to cache database
2. Write to persistent database
3. Both must succeed

**Example Usage:**
```python
db = kydb.connect('redis://cache|s3://storage')
db['key'] = 'value'  # Writes to both Redis and S3
result = db['key']   # Reads from Redis (fast)
```

#### Database Objects (`dbobj.py`)
Custom Python objects with database persistence:

**Key Features:**
- **Decorators**: `@stored` marks methods for persistence
- **Automatic Serialization**: Object state is automatically saved
- **Lazy Loading**: Objects are loaded on-demand
- **Method Preservation**: Non-stored methods remain functional

**Example Usage:**
```python
class User(kydb.DbObj):
    def init(self):
        self.login_count = 0  # Not persisted
    
    @kydb.stored
    def email(self):
        return "user@example.com"
    
    @kydb.stored
    def preferences(self):
        return {"theme": "dark"}
    
    def login(self):
        self.login_count += 1
        return f"Welcome {self.email()}"

# Usage
user = db.new('User', '/users/123', email='john@example.com')
user.write()  # Persists to database
```

#### Object Database Mixin (`objdb.py`)
Provides object management capabilities:

**Configuration Management:**
- **Config Path**: `/.configs/objdb` stores class metadata
- **Dynamic Loading**: Classes are loaded by module path
- **Registration**: `upload_objdb_config()` registers new classes

**Object Lifecycle:**
- **Creation**: `new()` creates instances
- **Serialization**: Converts objects to storage format
- **Deserialization**: Reconstructs objects from storage

### 6. Directory Management

#### Folder Metadata (`folder_meta.py`)
Simulates directory structure on flat storage systems:

**How It Works:**
- **Metadata Objects**: Creates `.folder-*` objects to represent directories
- **Recursive Creation**: `mkdir()` creates parent directories automatically
- **Listing**: `list_dir()` filters metadata objects to show clean directory structure

**Example:**
```python
db.mkdir('/app/users')
# Creates: /.folder-app, /app/.folder-users

db.ls('/app')
# Returns: ['users/']
```

### 7. Configuration System

#### Database Module Registry (`config.py`)
Maps URL prefixes to implementation classes:

```python
DB_MODULES = {
    'memory': 'MemoryDB',
    'redis': 'RedisDB', 
    'dynamodb': 'DynamoDB',
    's3': 'S3DB',
    'http': 'HttpDB',
    'files': 'FileDB'
}
```

#### Environment Configuration
- **`KYDB_CONFIG_PATH`**: Path to YAML configuration file
- **Database-specific Settings**: Connection parameters, credentials

### 8. Error Handling

#### Custom Exceptions (`exceptions.py`)
- **`DbObjException`**: Object-related errors
- **Standard Exceptions**: `KeyError` for missing keys, `ValueError` for invalid inputs

#### Graceful Degradation
- **Network Failures**: Retry logic in implementations
- **Missing Keys**: Clear error messages
- **Configuration Issues**: Helpful debugging information

## Usage Patterns

### Basic Operations
```python
import kydb

# Connect to database
db = kydb.connect('redis://localhost:6379/0')

# Dictionary-like access
db['user:123'] = {'name': 'John', 'age': 30}
user = db['user:123']

# Directory operations
db.mkdir('/users')
db.ls('/users')

# Object management
db.delete('user:123')
```

### Advanced Patterns
```python
# Union database for tiered storage
db = kydb.connect('redis://cache;s3://storage')

# Cache database for performance
db = kydb.connect('redis://cache|dynamodb://main')

# Custom objects
class Account(kydb.DbObj):
    @kydb.stored
    def balance(self):
        return 0.0

account = db.new('Account', '/accounts/123', balance=1000.0)
account.write()
```

## Summary

kydb provides a powerful abstraction layer that:

1. **Unifies Multiple Backends**: Single interface for Redis, S3, DynamoDB, etc.
2. **Adds Advanced Features**: Caching, unions, object persistence
3. **Maintains Simplicity**: Dictionary-like access patterns
4. **Supports Complex Patterns**: Multi-tier storage, custom objects
5. **Handles Edge Cases**: Directory simulation, error handling, configuration

The architecture follows solid design principles with clear separation of concerns, making it easy to extend with new database backends while maintaining backward compatibility.