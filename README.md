# tap-mongodb

Singer tap for MongoDB with flexible schema inference and per-stream configuration.

## Features

- ✅ **Flexible schema inference**: Handles inconsistent data types across documents
- ✅ **Multiple strategies**: `raw`, `flexible`, `strict`
- ✅ **Replication methods**: `FULL_TABLE`, `INCREMENTAL`
- ✅ **Per-stream configuration**: Different settings for each collection
- ✅ **Auto-detection**: Automatically detects standalone vs replica set
- ✅ **Standalone support**: Optimized for standalone MongoDB servers
- ✅ **Replica set support**: Full support for replica sets with multiple members
- ✅ **Connection string support**: MongoDB URI and SRV records
- ✅ **SSL/TLS support**: Secure connections
- ✅ **Replication key validation**: Automatic validation and indexing checks
- ✅ **Custom filters**: Native MongoDB query filters per stream
- ✅ **Field projections**: Select specific fields to extract
- ✅ **Batch processing**: Memory-efficient extraction
- ✅ **Type conversion**: Automatic conversion of MongoDB types (ObjectId, datetime, Decimal128, etc.)

## Installation

```bash
cd tap-mongodb
poetry install
```

## Configuration

### Basic Configuration (Auto-detection)

The tap automatically detects if MongoDB is standalone or replica set:

```json
{
  "host": "localhost",
  "port": 27017,
  "username": "user",
  "password": "pass",
  "database": "mydb",
  "auth_source": "admin",
  "collections": ["users", "orders"],
  "strategy": "flexible"
}
```

### Standalone Server (Explicit)

For faster connection to standalone servers:

```json
{
  "host": "localhost",
  "port": 27017,
  "username": "user",
  "password": "pass",
  "database": "mydb",
  "auth_source": "admin",
  "directConnection": true,
  "serverSelectionTimeoutMS": 5000,
  "collections": ["users", "orders"]
}
```

### Replica Set

```json
{
  "connection_string": "mongodb://user:pass@host1:27017,host2:27017,host3:27017/?authSource=admin&replicaSet=rs0",
  "database": "mydb",
  "serverSelectionTimeoutMS": 30000,
  "collections": ["users", "orders"]
}
```

### Connection String (Recommended)

```json
{
  "connection_string": "mongodb://user:pass@localhost:27017/?authSource=admin",
  "database": "mydb",
  "collections": ["users", "orders"]
}
```

### MongoDB Atlas (SRV)

```json
{
  "connection_string": "mongodb+srv://user:pass@cluster.mongodb.net/?retryWrites=true&w=majority",
  "database": "mydb",
  "ssl": true
}
```

### SSL/TLS Configuration

```json
{
  "host": "localhost",
  "port": 27017,
  "database": "mydb",
  "ssl": true,
  "ssl_cert_reqs": "CERT_REQUIRED",
  "ssl_ca_certs": "/path/to/ca.pem"
}
```

### Per-Stream Configuration

Configure different settings for each collection:

```json
{
  "connection_string": "mongodb://localhost:27017",
  "database": "mydb",
  "collections": ["processed_data", "users", "logs"],
  "strategy": "flexible",
  "stream_configs": {
    "processed_data": {
      "replication_method": "INCREMENTAL",
      "replication_key": "fechaHoraTrazabilidad",
      "start_date": "2024-01-01T00:00:00Z",
      "filter_field": "fechaHoraTrazabilidad",
      "filters": {
        "status": {"$in": ["active", "pending"]},
        "deleted": {"$ne": true}
      },
      "projection": {
        "_id": 1,
        "fechaHoraTrazabilidad": 1,
        "data": 1
      }
    },
    "users": {
      "replication_method": "FULL_TABLE",
      "projection": {
        "password": 0,
        "secret_key": 0
      }
    },
    "logs": {
      "replication_method": "INCREMENTAL",
      "replication_key": "_id",
      "filters": {
        "level": {"$in": ["error", "warning"]}
      }
    }
  }
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
| `infer_schema_max_docs` | integer | 1000 | Maximum documents to sample for schema inference |

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
| `batch_size` | integer | 1000 | Number of documents to fetch per batch |
| `max_record_per_run` | integer | - | Maximum number of documents emitted per stream in a single run |
| `validate_replication_keys` | boolean | true | Validate that replication keys exist and are indexed |

`batch_size` controls how many documents MongoDB fetches from the server at a time. `max_record_per_run` caps how many documents the tap emits for each stream during a run, so you can throttle extraction volume without changing MongoDB cursor batching.

## Schema Strategies

### `raw` Strategy
Minimal schema with all fields as strings. Best for maximum flexibility.

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
Infers types from sample documents but uses string for conflicting types. Best balance between type safety and flexibility.

### `strict` Strategy
Strict type inference from sample documents. Best for consistent data structures.

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
meltano run tap-mongodb target-clickhouse
```

## Examples

### Example 1: Simple Full Table Extraction

```json
{
  "connection_string": "mongodb://localhost:27017",
  "database": "mydb",
  "collections": ["users"]
}
```

### Example 2: Incremental Replication

```json
{
  "connection_string": "mongodb://localhost:27017",
  "database": "mydb",
  "collections": ["orders"],
  "stream_configs": {
    "orders": {
      "replication_method": "INCREMENTAL",
      "replication_key": "updated_at"
    }
  }
}
```

### Example 3: Filtered Extraction with Projections

```json
{
  "connection_string": "mongodb://localhost:27017",
  "database": "mydb",
  "collections": ["logs"],
  "stream_configs": {
    "logs": {
      "filters": {
        "level": "error",
        "timestamp": {"$gte": {"$date": "2024-01-01T00:00:00Z"}}
      },
      "projection": {
        "_id": 1,
        "timestamp": 1,
        "message": 1,
        "level": 1
      }
    }
  }
}
```

### Example 4: Multiple Collections with Different Settings

```json
{
  "connection_string": "mongodb://localhost:27017",
  "database": "mydb",
  "collections": ["users", "orders", "logs"],
  "replication_method": "FULL_TABLE",
  "stream_configs": {
    "users": {
      "projection": {"password": 0}
    },
    "orders": {
      "replication_method": "INCREMENTAL",
      "replication_key": "created_at",
      "start_date": "2024-01-01T00:00:00Z",
      "filter_field": "created_at"
    },
    "logs": {
      "filters": {"level": {"$in": ["error", "warning"]}}
    }
  }
}
```

### Example 5: Limit Records Emitted Per Run

```json
{
  "connection_string": "mongodb://localhost:27017",
  "database": "mydb",
  "collections": ["orders"],
  "batch_size": 500,
  "max_record_per_run": 1000,
  "stream_configs": {
    "orders": {
      "replication_method": "INCREMENTAL",
      "replication_key": "updated_at"
    }
  }
}
```

In this example, MongoDB still fetches up to 500 documents per cursor batch, but the tap stops after emitting 1,000 records for the `orders` stream in that run.

## Replication Key Validation

When `validate_replication_keys` is enabled (default), the tap will:

1. ✅ Check if the replication key field exists in the collection
2. ✅ Validate the field type is appropriate for incremental replication
3. ✅ Check if the field is indexed (warns if not)
4. ✅ Provide recommendations for creating indexes

Example output:
```
INFO Stream mydb_orders: Replication key 'updated_at' is indexed ✓
WARNING Stream mydb_logs: Replication key 'timestamp' is not indexed. 
Consider adding an index for better performance: db.logs.createIndex({timestamp: 1})
```

## Best Practices

1. **Use connection strings**: More flexible and supports SRV records
2. **Enable SSL/TLS**: For production environments
3. **Use per-stream configuration**: Different collections have different needs
4. **Index replication keys**: Critical for incremental replication performance
5. **Use projections**: Reduce data transfer by selecting only needed fields
6. **Use filters**: Extract only relevant data
7. **Start with flexible strategy**: Best balance for most use cases
8. **Monitor batch size**: Adjust based on document size and memory

## Troubleshooting

### Connection Issues

```bash
# Test connection
tap-mongodb --config config.json --test

# Enable debug logging
tap-mongodb --config config.json --log-level DEBUG
```

### Replication Key Issues

If you see warnings about replication keys:

```bash
# Create index in MongoDB
db.collection.createIndex({replication_key: 1})

# Verify index
db.collection.getIndexes()
```

### Memory Issues

If extraction uses too much memory:

```json
{
  "batch_size": 500,
  "max_record_per_run": 10000,
  "infer_schema_max_docs": 100
}
```

## Migration from tap-mongodb-v2

1. Rename package: `tap-mongodb-v2` → `tap-mongodb`
2. Update configuration to use `stream_configs` for per-collection settings
3. Remove global `filter_field` - move to `stream_configs`
4. Consider using `connection_string` instead of individual parameters
5. Review and enable `validate_replication_keys`

## License

Apache-2.0
