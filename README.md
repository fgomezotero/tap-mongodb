# tap-mongodb

Singer tap for MongoDB with flexible schema inference and per-stream configuration.

## Features

- ✅ **Flexible schema inference**: Discovers all fields and emits them as strings for maximum compatibility
- ✅ **Multiple strategies**: `raw`, `flexible`, `strict`
- ✅ **Replication methods**: `FULL_TABLE`, `INCREMENTAL`
- ✅ **Per-stream configuration**: Different settings for each collection
- ✅ **Auto-detection**: Automatically detects standalone vs replica set
- ✅ **Connection string support**: MongoDB URI and SRV records
- ✅ **SSL/TLS support**: Secure connections
- ✅ **Replication key validation**: Automatic validation and indexing checks
- ✅ **Custom filters**: Native MongoDB query filters per stream
- ✅ **Field projections**: Select specific fields to extract
- ✅ **Batch processing**: Memory-efficient extraction with configurable batch size
- ✅ **Type conversion**: Automatic conversion of MongoDB types (ObjectId, datetime, Decimal128, etc.)
- ✅ **Schema caching**: Schema is inferred once and cached for the entire run
- ✅ **Index-based sampling**: Schema inference uses `_id` index for fast sampling

## Installation

```bash
cd tap-mongodb
poetry install
```

Or via pip:

```bash
pip install git+https://github.com/fgomezotero/tap-mongodb.git
```

## Configuration

### Connection String (Recommended)

```json
{
  "connection_string": "mongodb://<user>:<password>@<host>:27017/?authSource=admin",
  "database": "my_database",
  "collections": ["events", "transactions"],
  "strategy": "flexible"
}
```

### Host/Port (Auto-detection)

The tap automatically detects if MongoDB is standalone or replica set:

```json
{
  "host": "<hostname>",
  "port": 27017,
  "username": "<user>",
  "password": "<password>",
  "database": "my_database",
  "auth_source": "admin",
  "collections": ["events", "transactions"],
  "strategy": "flexible"
}
```

### Standalone Server (Explicit)

For faster connection to standalone servers:

```json
{
  "host": "<hostname>",
  "port": 27017,
  "username": "<user>",
  "password": "<password>",
  "database": "my_database",
  "auth_source": "admin",
  "directConnection": true,
  "collections": ["events"]
}
```

### Replica Set

```json
{
  "connection_string": "mongodb://<user>:<password>@<host1>:27017,<host2>:27017,<host3>:27017/?authSource=admin&replicaSet=<rs_name>",
  "database": "my_database",
  "collections": ["events"]
}
```

### MongoDB Atlas (SRV)

```json
{
  "connection_string": "mongodb+srv://<user>:<password>@<cluster>.mongodb.net/?retryWrites=true&w=majority",
  "database": "my_database",
  "ssl": true
}
```

### SSL/TLS Configuration

```json
{
  "host": "<hostname>",
  "port": 27017,
  "database": "my_database",
  "ssl": true,
  "ssl_cert_reqs": "CERT_REQUIRED",
  "ssl_ca_certs": "/path/to/ca.pem"
}
```

## Configuration Options

### Connection Settings

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `connection_string` | string | No | - | MongoDB connection string (overrides host/port/username/password) |
| `host` | string | No* | - | MongoDB host |
| `port` | integer | No | 27017 | MongoDB port |
| `username` | string | No | - | MongoDB username |
| `password` | string | No | - | MongoDB password |
| `database` | string | Yes | - | MongoDB database name |
| `auth_source` | string | No | admin | Authentication database |
| `directConnection` | boolean | No | Auto-detect | Force direct connection (standalone). If not set, auto-detects. |
| `serverSelectionTimeoutMS` | integer | No | 5000 | Timeout for server selection in milliseconds |
| `connectTimeoutMS` | integer | No | 10000 | Connection timeout in milliseconds |

*Required if `connection_string` is not provided

### SSL/TLS Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `ssl` | boolean | false | Use SSL/TLS connection |
| `ssl_cert_reqs` | string | - | SSL certificate requirements (CERT_NONE, CERT_OPTIONAL, CERT_REQUIRED) |
| `ssl_ca_certs` | string | - | Path to CA certificate file |

### Collection Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `collections` | array | - | List of collections to extract. If not provided, discovers all collections |

### Schema Inference Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `strategy` | string | flexible | Schema inference strategy: `raw`, `flexible`, or `strict` |
| `infer_schema_max_docs` | integer | 100 | Maximum documents to sample for schema inference (uses `_id` index) |

### Replication Settings (Global Defaults)

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `replication_method` | string | FULL_TABLE | Default replication method: `FULL_TABLE` or `INCREMENTAL` |
| `replication_key` | string | - | Default replication key field for INCREMENTAL replication |
| `start_date` | datetime | - | Default start date for filtering records |

### Per-Stream Configuration

| Option | Type | Description |
|--------|------|-------------|
| `stream_configs` | object | Per-stream configuration overrides. Key is collection name |
| `stream_configs.<collection>.replication_method` | string | Replication method for this stream |
| `stream_configs.<collection>.replication_key` | string | Replication key for this stream |
| `stream_configs.<collection>.start_date` | datetime | Start date for this stream |
| `stream_configs.<collection>.filter_field` | string | Field to use for start_date filtering |
| `stream_configs.<collection>.filters` | object | MongoDB query filters (native MongoDB syntax) |
| `stream_configs.<collection>.projection` | object | MongoDB projection (1 to include, 0 to exclude) |

### Performance Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `batch_size` | integer | 1000 | Number of documents to fetch per batch from MongoDB cursor |
| `max_record_per_run` | integer | - | Maximum number of documents emitted per stream in a single run |
| `validate_replication_keys` | boolean | true | Validate that replication keys exist and are indexed |

### Retry Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `max_retries` | integer | 3 | Maximum number of retries for transient errors |
| `retry_delay` | integer | 5 | Initial delay between retries in seconds |
| `retry_backoff` | integer | 2 | Backoff multiplier for retry delays |

## Schema Strategies

### `raw` Strategy

Minimal schema — only declares `_id`. The tap emits all fields in RECORD messages but the SCHEMA message only declares `_id`. Use this with targets that don't need schema for DDL (e.g., target-jsonl).

```json
{
  "type": "object",
  "properties": {
    "_id": {"type": ["string", "null"]}
  },
  "additionalProperties": true
}
```

### `flexible` Strategy (Recommended)

Discovers all field names by sampling recent documents (using `_id` descending index) and declares every field as `["string", "null"]`. All values are converted to strings during extraction. This ensures:

- SQL targets (ClickHouse, Postgres) create columns for all fields
- No type mismatch errors when MongoDB has mixed types across documents
- Downstream transformations (dbt) can cast to proper types

Best for: ELT pipelines with a medallion architecture (bronze = raw strings, silver = typed by dbt).

### `strict` Strategy

Infers actual types (integer, number, boolean, string) from sample documents. Fails if a field has inconsistent types across documents. Best for homogeneous collections where you want native types in the target.

## Per-Stream Configuration

Configure different settings for each collection:

```json
{
  "connection_string": "mongodb://<user>:<password>@<host>:27017/?authSource=admin",
  "database": "my_database",
  "collections": ["events", "customers", "audit_log"],
  "strategy": "flexible",
  "infer_schema_max_docs": 500,
  "stream_configs": {
    "events": {
      "replication_method": "INCREMENTAL",
      "replication_key": "_id",
      "filters": {
        "created_at": {
          "$gte": {"$dateFromString": {"dateString": "2024-01-01T00:00:00Z"}},
          "$lt": {"$dateFromString": {"dateString": "2025-01-01T00:00:00Z"}}
        }
      }
    },
    "customers": {
      "replication_method": "FULL_TABLE",
      "projection": {
        "password_hash": 0,
        "api_key": 0
      }
    },
    "audit_log": {
      "replication_method": "INCREMENTAL",
      "replication_key": "_id",
      "filters": {
        "severity": {"$in": ["error", "critical"]}
      }
    }
  }
}
```

## Usage

### Discovery

```bash
tap-mongodb --config config.json --discover > catalog.json
```

### Extraction

```bash
tap-mongodb --config config.json --catalog catalog.json
```

### With Meltano

```bash
meltano invoke tap-mongodb --discover
meltano run tap-mongodb target-jsonl
meltano run tap-mongodb target-clickhouse
```

## Examples

### Simple Full Table Extraction

```json
{
  "connection_string": "mongodb://<user>:<password>@<host>:27017/?authSource=admin",
  "database": "my_database",
  "collections": ["customers"]
}
```

### Incremental by `_id`

Uses MongoDB's native `_id` index for efficient incremental extraction:

```json
{
  "connection_string": "mongodb://<user>:<password>@<host>:27017/?authSource=admin",
  "database": "my_database",
  "collections": ["orders"],
  "replication_method": "INCREMENTAL",
  "replication_key": "_id",
  "strategy": "flexible",
  "infer_schema_max_docs": 500
}
```

### Date Range Filter with `$dateFromString`

Filter by date fields using MongoDB query syntax. The tap resolves `$dateFromString` to native datetime objects:

```json
{
  "connection_string": "mongodb://<user>:<password>@<host>:27017/?authSource=admin",
  "database": "my_database",
  "collections": ["events"],
  "stream_configs": {
    "events": {
      "replication_method": "INCREMENTAL",
      "replication_key": "_id",
      "filters": {
        "event_date": {
          "$gte": {"$dateFromString": {"dateString": "2024-06-01T00:00:00Z"}},
          "$lt": {"$dateFromString": {"dateString": "2024-07-01T00:00:00Z"}}
        }
      }
    }
  }
}
```

### Limit Records Per Run

Useful for CI/CD tests or throttling extraction:

```json
{
  "connection_string": "mongodb://<user>:<password>@<host>:27017/?authSource=admin",
  "database": "my_database",
  "collections": ["orders"],
  "batch_size": 500,
  "max_record_per_run": 1000,
  "stream_configs": {
    "orders": {
      "replication_method": "INCREMENTAL",
      "replication_key": "_id"
    }
  }
}
```

### Field Projections (Include/Exclude)

```json
{
  "connection_string": "mongodb://<user>:<password>@<host>:27017/?authSource=admin",
  "database": "my_database",
  "collections": ["customers"],
  "stream_configs": {
    "customers": {
      "projection": {
        "_id": 1,
        "name": 1,
        "email": 1,
        "created_at": 1
      }
    }
  }
}
```

## Replication Key Validation

When `validate_replication_keys` is enabled (default), the tap will:

1. ✅ Check if the replication key field exists in the collection
2. ✅ Validate the field type is appropriate for incremental replication
3. ✅ Check if the field is indexed (warns if not)
4. ✅ Provide recommendations for creating indexes

Example output:
```
INFO Stream my_database_orders: Replication key '_id' is indexed ✓
WARNING Stream my_database_logs: Replication key 'timestamp' is not indexed.
Consider adding an index: db.logs.createIndex({timestamp: 1})
```

## Best Practices

1. **Use `_id` as replication key** — always indexed, supports efficient incremental extraction
2. **Use `flexible` strategy for SQL targets** — ensures all columns are created in the target DDL
3. **Use `raw` strategy for file targets** — minimal overhead when DDL is not needed
4. **Increase `infer_schema_max_docs`** for collections with rare fields (e.g., 500-10000)
5. **Use connection strings** — more flexible and supports SRV records
6. **Enable SSL/TLS** in production environments
7. **Use projections** to exclude sensitive or unnecessary fields
8. **Use filters** to extract only relevant data ranges
9. **Use `max_record_per_run`** for CI/CD smoke tests

## Troubleshooting

### Connection Issues

```bash
# Enable debug logging
tap-mongodb --config config.json --discover --log-level DEBUG
```

### Schema Fields Missing

If the target reports "No schema for record field", increase sampling:

```json
{
  "infer_schema_max_docs": 1000
}
```

The sampling uses the `_id` index (descending) so increasing this value has minimal performance impact.

### Replication Key Not Indexed

If you see warnings about unindexed replication keys:

```javascript
// Create index in MongoDB shell
db.collection.createIndex({replication_key: 1})

// Verify
db.collection.getIndexes()
```

### Memory Issues

For large documents, reduce batch size:

```json
{
  "batch_size": 100,
  "max_record_per_run": 5000
}
```

## License

Apache-2.0
