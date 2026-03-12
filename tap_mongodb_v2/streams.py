"""MongoDB stream class."""
from __future__ import annotations
from typing import Any, Dict, Iterable, Optional
from bson import ObjectId
from datetime import datetime
from pymongo.collection import Collection
from singer_sdk import Stream


class MongoDBStream(Stream):
    """MongoDB stream class."""
    
    primary_keys = ["_id"]
    replication_key = None
    
    def __init__(self, tap, name: str, collection: Collection):
        """Initialize stream."""
        super().__init__(tap, name=name, schema=None)
        self._collection = collection
        self._strategy = tap.config.get("strategy", "flexible")
        self._infer_max_docs = tap.config.get("infer_schema_max_docs", 1000)
        
        if tap.config.get("replication_method") == "INCREMENTAL":
            self.replication_key = tap.config.get("replication_key", "_id")
    
    @property
    def schema(self) -> dict:
        """Return schema for stream."""
        if self._strategy == "raw":
            return self._raw_schema()
        elif self._strategy == "flexible":
            return self._flexible_schema()
        else:
            return self._strict_schema()
    
    def _raw_schema(self) -> dict:
        """Return minimal schema - all fields as strings."""
        return {
            "type": "object",
            "properties": {
                "_id": {"type": ["string", "null"]},
            },
            "additionalProperties": True,
        }
    
    def _flexible_schema(self) -> dict:
        """Infer schema but use string for conflicting types."""
        try:
            sample_docs = list(self._collection.find().limit(self._infer_max_docs))
        except Exception as e:
            self.logger.warning(f"Failed to sample documents: {e}")
            return self._raw_schema()
        
        if not sample_docs:
            return self._raw_schema()
        
        field_types = {}
        for doc in sample_docs:
            for key, value in doc.items():
                if key not in field_types:
                    field_types[key] = set()
                if value is not None:
                    field_types[key].add(type(value).__name__)
        
        properties = {}
        for field, types in field_types.items():
            if field == "_id":
                properties[field] = {"type": ["string", "null"]}
            elif len(types) > 1:
                properties[field] = {"type": ["string", "null"]}
            elif len(types) == 0:
                properties[field] = {"type": ["string", "null"]}
            else:
                type_name = list(types)[0]
                json_type = self._python_to_json_type(type_name)
                properties[field] = {"type": [json_type, "null"]}
        
        return {
            "type": "object",
            "properties": properties,
            "additionalProperties": True,
        }
    
    def _strict_schema(self) -> dict:
        """Infer strict schema from sample documents."""
        sample_docs = list(self._collection.find().limit(self._infer_max_docs))
        
        if not sample_docs:
            return self._raw_schema()
        
        field_types = {}
        for doc in sample_docs:
            for key, value in doc.items():
                if key not in field_types:
                    field_types[key] = type(value).__name__
        
        properties = {}
        for field, type_name in field_types.items():
            if field == "_id":
                properties[field] = {"type": "string"}
            else:
                json_type = self._python_to_json_type(type_name)
                properties[field] = {"type": [json_type, "null"]}
        
        return {
            "type": "object",
            "properties": properties,
        }
    
    def _python_to_json_type(self, python_type: str) -> str:
        """Convert Python type to JSON Schema type."""
        mapping = {
            "str": "string",
            "int": "integer",
            "float": "number",
            "bool": "boolean",
            "dict": "object",
            "list": "array",
            "NoneType": "null",
            "ObjectId": "string",
            "datetime": "string",
        }
        return mapping.get(python_type, "string")
    
    def _convert_value(self, value: Any) -> Any:
        """Convert MongoDB types to JSON-serializable types."""
        if isinstance(value, ObjectId):
            return str(value)
        elif isinstance(value, datetime):
            return value.isoformat()
        elif isinstance(value, dict):
            return {k: self._convert_value(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [self._convert_value(item) for item in value]
        # Don't convert to string in flexible mode - keep original types
        return value
    
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Return records from MongoDB collection."""
        bookmark = self.get_starting_replication_key_value(context)
        
        query = {}
        
        # Apply start_date filter if configured
        start_date = self._tap.config.get("start_date")
        filter_field = self._tap.config.get("filter_field")
        
        if start_date and filter_field:
            from dateutil import parser
            start_dt = parser.parse(start_date) if isinstance(start_date, str) else start_date
            
            # Check field type in a sample document
            sample = self._collection.find_one({filter_field: {"$exists": True}})
            if sample and filter_field in sample:
                field_value = sample[filter_field]
                
                # Only apply filter if field is datetime type in MongoDB
                if isinstance(field_value, datetime):
                    query[filter_field] = {"$gte": start_dt}
                    self.logger.info(f"Applying date filter on {filter_field} >= {start_dt}")
                else:
                    self.logger.warning(
                        f"Field '{filter_field}' is not datetime type in MongoDB (found {type(field_value).__name__}). "
                        f"Date filter will be ignored. Convert field to datetime in MongoDB to use date filtering."
                    )
        
        # Apply incremental replication filter
        if bookmark and self.replication_key:
            if self.replication_key in query:
                query[self.replication_key]["$gt"] = bookmark
            else:
                query[self.replication_key] = {"$gt": bookmark}
        
        for record in self._collection.find(query):
            converted_record = {}
            for key, value in record.items():
                converted_record[key] = self._convert_value(value)
            yield converted_record
