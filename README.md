# tap-mongodb-v2

Singer tap for MongoDB with flexible schema inference.

## Features

- **Flexible schema inference**: Handles inconsistent data types across documents
- **Multiple strategies**: `raw`, `flexible`, `strict`
- **Replication methods**: `FULL_TABLE`, `INCREMENTAL`
- **Type conversion**: Automatic conversion of MongoDB types (ObjectId, datetime)

## Installation

```bash
pip install -e .
```

## Configuration

```yaml
host: localhost
port: 27017
username: user
password: pass
database: mydb
auth_source: admin
collections: ["collection1", "collection2"]  # Optional, defaults to all
strategy: flexible  # raw, flexible, strict
infer_schema_max_docs: 1000
replication_method: FULL_TABLE  # or INCREMENTAL
replication_key: _id
```

## Strategies

- **raw**: Minimal schema, all fields as strings, additionalProperties: true
- **flexible**: Infers types but uses string for conflicting types (recommended)
- **strict**: Strict type inference from sample documents

## Usage

```bash
tap-mongodb-v2 --config config.json --discover > catalog.json
tap-mongodb-v2 --config config.json --catalog catalog.json
```
