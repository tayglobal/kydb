# KYDB (Kinyu Database) - Project Overview

## What is KYDB?

KYDB is a Python library that provides a **NoSQL database abstraction layer** designed specifically for the financial services industry. The name "Kinyu" (金融) means "finance" in Japanese, reflecting its target domain.

## Core Purpose

The project aims to provide a unified interface for working with different NoSQL databases through a simple, filesystem-like API. Instead of learning different database-specific APIs, developers can use a single interface to interact with various backend storage systems.

## Key Features

### 1. **Simple Factory Pattern**
- A single URL defines the database connection
- Examples: `dynamodb://my-table`, `s3://my-bucket`, `redis://localhost:6379`
- Supports multiple databases in a single connection string

### 2. **Filesystem-like Hierarchy**
- Objects are organized in a hierarchical structure similar to a file system
- Use paths like `/my/folder/object` to organize data
- Supports operations like `mkdir`, `ls`, `rmdir`, `rm_tree`

### 3. **Caching System**
- Built-in caching layer for improved performance
- Cache context management for transaction-like operations
- Automatic cache invalidation and refresh capabilities

### 4. **Union Database Support**
- Combine multiple databases where:
  - **Read operations** search databases in order until found
  - **Write operations** always go to the first (primary) database
- Syntax: `redis://cache;dynamodb://primary-db`

### 5. **Serializable Objects**
- Store any Python object in the database
- Decorator-based control over serialization
- Automatic object persistence and retrieval

## Supported Database Backends

Based on the implementation directory, KYDB supports:

- **DynamoDB** - AWS NoSQL database
- **S3** - AWS object storage
- **Redis** - In-memory data store
- **HTTP/HTTPS** - RESTful API endpoints
- **Files** - Local filesystem storage
- **Memory** - In-memory storage for testing

## Main API

The primary interface is through the `connect()` function:

```python
import kydb

# Single database
db = kydb.connect('dynamodb://my-table')

# Union of databases (cache + primary)
db = kydb.connect('redis://cache;dynamodb://primary')

# Cache configuration
db = kydb.connect('redis://cache|dynamodb://primary')
```

## Usage Patterns

### Basic Operations
```python
# Store data
db['/my/key'] = my_object

# Retrieve data
obj = db['/my/key']

# Check existence
if db.exists('/my/key'):
    # do something

# List directory
files = db.ls('/my/folder')
```

### Object Management
```python
# Create new object
obj = db.new('MyClass', '/my/key', attribute=value)
obj.write()  # Persist to database

# Read with caching
obj = db.read('/my/key')  # From cache
obj = db.read('/my/key', reload=True)  # Force DB read
```

## Target Use Cases

Given its focus on financial services, KYDB is designed for:

- **Financial data storage** - Market data, transactions, portfolios
- **High-performance applications** - With caching and union database patterns
- **Multi-environment deployments** - Different backends for different environments
- **Object persistence** - Complex financial models and calculations

## Development Status

- **Version**: 0.7.5 (Alpha stage)
- **Python Requirements**: >= 3.10
- **License**: MIT
- **Documentation**: Available at [https://kydb.readthedocs.io/](https://kydb.readthedocs.io/)

## Dependencies

The project depends on:
- `boto3` - AWS SDK for DynamoDB and S3 support
- `redis` - Redis client
- `pyyaml` - YAML configuration support
- `requests` - HTTP client functionality

## Summary

KYDB is a sophisticated abstraction layer that simplifies NoSQL database interactions for financial applications. It provides a unified, filesystem-like interface across multiple database backends, with advanced features like caching, database unions, and object serialization - all designed to handle the complex data management needs of financial services.