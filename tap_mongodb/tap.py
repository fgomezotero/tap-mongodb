"""MongoDB tap class."""
from __future__ import annotations
from typing import List
import time
from singer_sdk import Tap
from singer_sdk import typing as th
from tap_mongodb.streams import MongoDBStream


class TapMongoDB(Tap):
    """MongoDB tap class."""
    
    name = "tap-mongodb"
    
    config_jsonschema = th.PropertiesList(
        # Connection settings
        th.Property(
            "connection_string",
            th.StringType,
            description="MongoDB connection string (mongodb:// or mongodb+srv://). If provided, overrides host/port/username/password.",
        ),
        th.Property(
            "host",
            th.StringType,
            description="MongoDB host",
        ),
        th.Property(
            "port",
            th.IntegerType,
            default=27017,
            description="MongoDB port",
        ),
        th.Property(
            "username",
            th.StringType,
            description="MongoDB username",
        ),
        th.Property(
            "password",
            th.StringType,
            secret=True,
            description="MongoDB password",
        ),
        th.Property(
            "database",
            th.StringType,
            required=True,
            description="MongoDB database name",
        ),
        th.Property(
            "auth_source",
            th.StringType,
            default="admin",
            description="Authentication database",
        ),
        
        # SSL/TLS settings
        th.Property(
            "ssl",
            th.BooleanType,
            default=False,
            description="Use SSL/TLS connection",
        ),
        th.Property(
            "ssl_cert_reqs",
            th.StringType,
            description="SSL certificate requirements (CERT_NONE, CERT_OPTIONAL, CERT_REQUIRED)",
        ),
        th.Property(
            "ssl_ca_certs",
            th.StringType,
            description="Path to CA certificate file",
        ),
        
        # Collection settings
        th.Property(
            "collections",
            th.ArrayType(th.StringType),
            description="List of collections to extract. If not provided, discovers all collections.",
        ),
        
        # Schema inference settings
        th.Property(
            "strategy",
            th.StringType,
            default="flexible",
            description="Schema inference strategy: raw, flexible, or strict",
        ),
        th.Property(
            "infer_schema_max_docs",
            th.IntegerType,
            default=100,
            description="Maximum documents to sample for schema inference (uses _id index)",
        ),
        
        # Replication settings (global defaults)
        th.Property(
            "replication_method",
            th.StringType,
            default="FULL_TABLE",
            description="Default replication method: FULL_TABLE or INCREMENTAL",
        ),
        th.Property(
            "replication_key",
            th.StringType,
            description="Default replication key field for INCREMENTAL replication",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="Default start date for filtering records",
        ),
        
        # Per-stream configuration
        th.Property(
            "stream_configs",
            th.ObjectType(
                additional_properties=th.ObjectType(
                    th.Property("replication_method", th.StringType),
                    th.Property("replication_key", th.StringType),
                    th.Property("start_date", th.DateTimeType),
                    th.Property("filter_field", th.StringType),
                    th.Property("filters", th.ObjectType()),
                    th.Property("projection", th.ObjectType()),
                )
            ),
            description="Per-stream configuration overrides. Key is collection name.",
        ),
        
        # Connection behavior settings
        th.Property(
            "directConnection",
            th.BooleanType,
            description="Force direct connection (standalone). If not set, auto-detects deployment type.",
        ),
        th.Property(
            "serverSelectionTimeoutMS",
            th.IntegerType,
            default=5000,
            description="Timeout for server selection in milliseconds",
        ),
        th.Property(
            "connectTimeoutMS",
            th.IntegerType,
            default=10000,
            description="Connection timeout in milliseconds",
        ),
        
        # Performance settings
        th.Property(
            "batch_size",
            th.IntegerType,
            default=1000,
            description="Number of documents to fetch per batch",
        ),
        th.Property(
            "max_record_per_run",
            th.IntegerType,
            description="Maximum number of documents to emit per stream run",
        ),
        th.Property(
            "validate_replication_keys",
            th.BooleanType,
            default=True,
            description="Validate that replication keys exist and are indexed",
        ),
        
        # Retry settings
        th.Property(
            "max_retries",
            th.IntegerType,
            default=3,
            description="Maximum number of retries for transient errors",
        ),
        th.Property(
            "retry_delay",
            th.IntegerType,
            default=5,
            description="Initial delay between retries in seconds",
        ),
        th.Property(
            "retry_backoff",
            th.IntegerType,
            default=2,
            description="Backoff multiplier for retry delays",
        ),
    ).to_dict()
    
    def __init__(self, *args, **kwargs):
        """Initialize tap."""
        self._client = None
        super().__init__(*args, **kwargs)
    
    def _detect_deployment_type(self, client):
        """Detect if MongoDB is standalone or replica set."""
        try:
            is_master = client.admin.command("isMaster")
            
            if "setName" in is_master:
                self.logger.info(f"Detected replica set: {is_master['setName']}")
                return "replicaset"
            else:
                self.logger.info("Detected standalone MongoDB server")
                return "standalone"
        except Exception as e:
            self.logger.warning(f"Could not detect deployment type: {e}")
            return "unknown"
    
    def _get_client(self):
        """Get or create MongoDB client."""
        if self._client is None:
            from pymongo import MongoClient
            import ssl as ssl_module
            
            connection_string = self.config.get("connection_string")
            
            # Base connection parameters
            kwargs = {
                "serverSelectionTimeoutMS": self.config.get("serverSelectionTimeoutMS", 5000),
                "connectTimeoutMS": self.config.get("connectTimeoutMS", 10000),
            }
            
            # SSL/TLS configuration
            if self.config.get("ssl"):
                kwargs["ssl"] = True
                if self.config.get("ssl_cert_reqs"):
                    cert_reqs = getattr(ssl_module, self.config["ssl_cert_reqs"])
                    kwargs["ssl_cert_reqs"] = cert_reqs
                if self.config.get("ssl_ca_certs"):
                    kwargs["ssl_ca_certs"] = self.config["ssl_ca_certs"]
            
            if connection_string:
                # Use connection string
                self.logger.info("Connecting using connection string")
                
                # Add directConnection if specified
                if self.config.get("directConnection") is not None:
                    kwargs["directConnection"] = self.config["directConnection"]
                
                self._client = MongoClient(connection_string, **kwargs)
            else:
                # Use individual parameters
                self.logger.info(f"Connecting to {self.config.get('host')}:{self.config.get('port', 27017)}")
                
                kwargs.update({
                    "host": self.config.get("host"),
                    "port": self.config.get("port", 27017),
                    "username": self.config.get("username"),
                    "password": self.config.get("password"),
                    "authSource": self.config.get("auth_source", "admin"),
                })
                
                # Handle directConnection parameter
                if self.config.get("directConnection") is not None:
                    kwargs["directConnection"] = self.config["directConnection"]
                    if self.config["directConnection"]:
                        self.logger.info("Using directConnection=True (standalone mode)")
                else:
                    # Auto-detect: try with directConnection first for standalone
                    self.logger.info("Auto-detecting deployment type...")
                    try:
                        # Try standalone first (faster for standalone servers)
                        test_kwargs = kwargs.copy()
                        test_kwargs["directConnection"] = True
                        test_kwargs["serverSelectionTimeoutMS"] = 2000
                        test_client = MongoClient(**test_kwargs)
                        test_client.admin.command("ping")
                        test_client.close()
                        
                        kwargs["directConnection"] = True
                        self.logger.info("Auto-detected: standalone server")
                    except Exception:
                        # If standalone fails, try replica set mode
                        self.logger.info("Standalone connection failed, trying replica set mode...")
                        kwargs["readPreference"] = "secondaryPreferred"
                
                self._client = MongoClient(**kwargs)
            
            # Test connection and detect deployment type
            try:
                self._client.admin.command("ping")
                deployment_type = self._detect_deployment_type(self._client)
                self.logger.info(f"Successfully connected to MongoDB ({deployment_type})")
            except Exception as e:
                self.logger.error(f"Failed to connect to MongoDB: {e}")
                self.logger.error("Troubleshooting tips:")
                self.logger.error("  - For standalone servers, add: 'directConnection': true")
                self.logger.error("  - For replica sets, ensure all members are accessible")
                self.logger.error("  - Check firewall rules and network connectivity")
                raise
        
        return self._client
    
    def discover_streams(self) -> List[MongoDBStream]:
        """Return a list of discovered streams."""
        start_time = time.time()
        
        client = self._get_client()
        db = client[self.config["database"]]
        
        # Get collections from config or discover all
        config_collections = self.config.get("collections")
        if config_collections:
            collections = config_collections
        else:
            collections = db.list_collection_names()
            self.logger.info(f"Discovered {len(collections)} collections")
        
        streams = []
        stream_configs = self.config.get("stream_configs", {})
        
        for collection_name in collections:
            collection = db[collection_name]
            
            # Get stream-specific config or use defaults
            stream_config = stream_configs.get(collection_name, {})
            
            # Merge with global defaults
            merged_config = {
                "replication_method": stream_config.get(
                    "replication_method",
                    self.config.get("replication_method", "FULL_TABLE")
                ),
                "replication_key": stream_config.get(
                    "replication_key",
                    self.config.get("replication_key")
                ),
                "start_date": stream_config.get(
                    "start_date",
                    self.config.get("start_date")
                ),
                "filter_field": stream_config.get("filter_field"),
                "filters": stream_config.get("filters", {}),
                "projection": stream_config.get("projection"),
                "max_record_per_run": self.config.get("max_record_per_run"),
            }
            
            stream = MongoDBStream(
                tap=self,
                name=f"{self.config['database']}_{collection_name}",
                collection=collection,
                stream_config=merged_config,
            )
            
            # Validate replication key if enabled
            if self.config.get("validate_replication_keys", True):
                stream.validate_replication_key()
            
            streams.append(stream)
        
        duration = time.time() - start_time
        self.logger.info(f"Stream discovery completed in {duration:.2f}s ({len(streams)} streams)")
        
        return streams
    
    def cleanup(self):
        """Close MongoDB connection."""
        if hasattr(self, '_client') and self._client:
            self.logger.info("Closing MongoDB connection")
            self._client.close()
            self._client = None
    
    def __del__(self):
        """Ensure cleanup on deletion."""
        self.cleanup()
