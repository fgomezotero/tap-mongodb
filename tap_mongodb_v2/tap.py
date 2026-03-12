"""MongoDB tap class."""
from __future__ import annotations
from typing import List
from singer_sdk import Tap
from singer_sdk import typing as th
from tap_mongodb_v2.streams import MongoDBStream


class TapMongoDB(Tap):
    """MongoDB tap class."""
    
    name = "tap-mongodb-v2"
    
    config_jsonschema = th.PropertiesList(
        th.Property("host", th.StringType, required=True),
        th.Property("port", th.IntegerType, default=27017),
        th.Property("username", th.StringType),
        th.Property("password", th.StringType, secret=True),
        th.Property("database", th.StringType, required=True),
        th.Property("auth_source", th.StringType, default="admin"),
        th.Property("collections", th.ArrayType(th.StringType)),
        th.Property("strategy", th.StringType, default="flexible"),
        th.Property("infer_schema_max_docs", th.IntegerType, default=1000),
        th.Property("replication_method", th.StringType, default="FULL_TABLE"),
        th.Property("replication_key", th.StringType, default="_id"),
        th.Property("start_date", th.DateTimeType),
        th.Property("filter_field", th.StringType),
    ).to_dict()
    
    def discover_streams(self) -> List[MongoDBStream]:
        """Return a list of discovered streams."""
        from pymongo import MongoClient
        
        # Don't close client - streams need it
        if not hasattr(self, '_client'):
            self._client = MongoClient(
                host=self.config["host"],
                port=self.config["port"],
                username=self.config.get("username"),
                password=self.config.get("password"),
                authSource=self.config.get("auth_source", "admin"),
            )
        
        db = self._client[self.config["database"]]
        
        # Get collections from config or discover all
        config_collections = self.config.get("collections")
        if config_collections:
            collections = config_collections
        else:
            collections = db.list_collection_names()
        
        streams = []
        for collection_name in collections:
            collection = db[collection_name]
            stream = MongoDBStream(
                tap=self,
                name=f"{self.config['database']}_{collection_name}",
                collection=collection,
            )
            streams.append(stream)
        
        return streams
