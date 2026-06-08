"""MongoDB stream class."""
from __future__ import annotations
from typing import Any, Dict, Iterable, Optional
import time
import json
from bson import ObjectId
from datetime import datetime
from pymongo.collection import Collection
from pymongo.errors import AutoReconnect, NetworkTimeout, ServerSelectionTimeoutError
from singer_sdk import Stream
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)
import logging


class MongoDBStream(Stream):
    """MongoDB stream class."""
    
    primary_keys = ["_id"]
    replication_key = None
    
    def __init__(
        self,
        tap,
        name: str,
        collection: Collection,
        stream_config: Optional[Dict] = None,
    ):
        """Initialize stream."""
        super().__init__(tap, name=name, schema=None)
        self._collection = collection
        self._stream_config = stream_config or {}
        
        # Schema inference settings
        self._strategy = tap.config.get("strategy", "flexible")
        self._infer_max_docs = tap.config.get("infer_schema_max_docs", 1000)
        
        # Replication settings
        replication_method = self._stream_config.get("replication_method", "FULL_TABLE")
        if replication_method == "INCREMENTAL":
            self.replication_key = self._stream_config.get("replication_key")
            if not self.replication_key:
                self.logger.warning(
                    f"Stream {name}: INCREMENTAL replication requires replication_key. "
                    f"Falling back to FULL_TABLE."
                )
        
        # Performance settings
        self._batch_size = tap.config.get("batch_size", 1000)
        self._max_record_per_run = self._stream_config.get(
            "max_record_per_run",
            tap.config.get("max_record_per_run"),
        )
        
        # Retry settings
        self._max_retries = tap.config.get("max_retries", 3)
        self._retry_delay = tap.config.get("retry_delay", 5)
        self._retry_backoff = tap.config.get("retry_backoff", 2)
        
        # Metrics
        self._metrics = {
            "records_extracted": 0,
            "bytes_extracted": 0,
            "extraction_time": 0,
            "errors": 0,
            "retries": 0,
        }
    
    def validate_replication_key(self):
        """Validate replication key exists and is appropriate for incremental replication."""
        if not self.replication_key:
            return
        
        # Check if field exists in collection
        sample = self._collection.find_one({self.replication_key: {"$exists": True}})
        
        if not sample:
            self.logger.warning(
                f"Stream {self.name}: Replication key '{self.replication_key}' not found in any documents. "
                f"Incremental replication may not work correctly."
            )
            return
        
        # Check field type
        field_value = sample.get(self.replication_key)
        field_type = type(field_value).__name__
        
        valid_types = ["datetime", "ObjectId", "int", "float", "str"]
        if field_type not in valid_types:
            self.logger.warning(
                f"Stream {self.name}: Replication key '{self.replication_key}' has type '{field_type}'. "
                f"Expected one of: {valid_types}. Incremental replication may not work correctly."
            )
        
        # Check if field is indexed
        indexes = self._collection.index_information()
        is_indexed = False
        
        for index_name, index_info in indexes.items():
            index_keys = [key[0] for key in index_info.get("key", [])]
            if self.replication_key in index_keys:
                is_indexed = True
                break
        
        if not is_indexed:
            self.logger.warning(
                f"Stream {self.name}: Replication key '{self.replication_key}' is not indexed. "
                f"Consider adding an index for better performance: "
                f"db.{self._collection.name}.createIndex({{{self.replication_key}: 1}})"
            )
        else:
            self.logger.info(
                f"Stream {self.name}: Replication key '{self.replication_key}' is indexed ✓"
            )
    
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
            # Apply projection if configured
            projection = self._stream_config.get("projection")
            sample_docs = list(
                self._collection.find({}, projection).limit(self._infer_max_docs)
            )
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
            elif types & {"dict", "list"}:
                properties[field] = {"type": ["string", "null"]}
            elif len(types) > 1:
                # Multiple types found - use string for flexibility
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
        projection = self._stream_config.get("projection")
        sample_docs = list(
            self._collection.find({}, projection).limit(self._infer_max_docs)
        )
        
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
            "dict": "string",
            "list": "string",
            "NoneType": "null",
            "ObjectId": "string",
            "datetime": "string",
            "Decimal128": "number",
            "Binary": "string",
            "Code": "string",
            "Timestamp": "string",
        }
        return mapping.get(python_type, "string")
    
    def _json_default(self, value: Any) -> str:
        """Return a JSON-serializable representation for MongoDB values."""
        if isinstance(value, ObjectId):
            return str(value)
        elif isinstance(value, datetime):
            return value.isoformat()
        elif hasattr(value, "__str__") and type(value).__module__ == "bson":
            return str(value)
        raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")
    
    def _convert_value(self, value: Any) -> Any:
        """Convert MongoDB types to JSON-serializable types."""
        if isinstance(value, ObjectId):
            return str(value)
        elif isinstance(value, datetime):
            return value.isoformat()
        elif isinstance(value, list):
            return json.dumps(value, default=self._json_default)
        elif isinstance(value, dict):
            return json.dumps(value, default=self._json_default)
        elif hasattr(value, "__str__") and type(value).__module__ == "bson":
            # Handle other BSON types (Decimal128, Binary, etc.)
            return str(value)
        return value
    
    def _build_query(self, bookmark: Any) -> dict:
        """Build MongoDB query with filters and replication key."""
        query = {}
        
        # Apply custom filters from stream config
        custom_filters = self._stream_config.get("filters", {})
        if custom_filters:
            query.update(custom_filters)
            self.logger.info(f"Applying custom filters: {custom_filters}")
        
        # Apply start_date filter if configured
        start_date = self._stream_config.get("start_date")
        filter_field = self._stream_config.get("filter_field")
        
        if start_date and filter_field:
            from dateutil import parser
            start_dt = parser.parse(start_date) if isinstance(start_date, str) else start_date
            
            # Check field type in a sample document
            sample = self._collection.find_one({filter_field: {"$exists": True}})
            if sample and filter_field in sample:
                field_value = sample[filter_field]
                
                # Only apply filter if field is datetime type in MongoDB
                if isinstance(field_value, datetime):
                    if filter_field in query:
                        # Merge with existing filter
                        if isinstance(query[filter_field], dict):
                            query[filter_field]["$gte"] = start_dt
                        else:
                            query[filter_field] = {"$gte": start_dt}
                    else:
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
                # Merge with existing filter
                if isinstance(query[self.replication_key], dict):
                    query[self.replication_key]["$gt"] = bookmark
                else:
                    query[self.replication_key] = {"$gt": bookmark}
            else:
                query[self.replication_key] = {"$gt": bookmark}
            self.logger.info(f"Applying incremental filter: {self.replication_key} > {bookmark}")
        
        return query
    
    def _execute_query_with_retry(self, query: dict, projection: Optional[dict] = None):
        """Execute MongoDB query with retry logic."""
        @retry(
            stop=stop_after_attempt(self._max_retries),
            wait=wait_exponential(
                multiplier=self._retry_delay,
                max=self._retry_delay * (self._retry_backoff ** self._max_retries)
            ),
            retry=retry_if_exception_type((AutoReconnect, NetworkTimeout, ServerSelectionTimeoutError)),
            before_sleep=before_sleep_log(self.logger, logging.WARNING),
        )
        def _execute():
            return self._collection.find(query, projection).batch_size(self._batch_size)
        
        try:
            return _execute()
        except Exception as e:
            self._metrics["errors"] += 1
            self.logger.error(f"Failed to execute query after {self._max_retries} retries: {e}")
            raise
    
    def _log_metrics(self):
        """Log extraction metrics."""
        records = self._metrics["records_extracted"]
        duration = self._metrics["extraction_time"]
        bytes_size = self._metrics["bytes_extracted"]
        errors = self._metrics["errors"]
        retries = self._metrics["retries"]
        
        if duration > 0:
            rate = records / duration
            mb_size = bytes_size / (1024 * 1024)
            
            self.logger.info(
                f"Stream {self.name} extraction completed: "
                f"{records:,} records, {mb_size:.2f} MB, {duration:.1f}s "
                f"({rate:.0f} records/sec)"
            )
            
            if retries > 0:
                self.logger.info(f"Total retries: {retries}")
            if errors > 0:
                self.logger.warning(f"Total errors: {errors}")
    
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Return records from MongoDB collection."""
        start_time = time.time()
        bookmark = self.get_starting_replication_key_value(context)
        
        # Build query
        query = self._build_query(bookmark)
        
        # Get projection
        projection = self._stream_config.get("projection")
        
        # Log query info
        self.logger.info(f"Querying collection {self._collection.name}")
        if query:
            self.logger.info(f"Query: {query}")
        if projection:
            self.logger.info(f"Projection: {projection}")
        
        # Execute query with retry logic
        try:
            cursor = self._execute_query_with_retry(query, projection)
        except Exception as e:
            self.logger.error(f"Failed to execute query: {e}")
            raise
        
        record_count = 0
        bytes_count = 0
        last_log_time = start_time
        
        for record in cursor:
            if (
                self._max_record_per_run is not None
                and record_count >= self._max_record_per_run
            ):
                self.logger.info(
                    f"Reached max_record_per_run={self._max_record_per_run} for stream {self.name}"
                )
                break

            converted_record = {}
            for key, value in record.items():
                converted_record[key] = self._convert_value(value)
            
            record_count += 1
            bytes_count += len(str(converted_record).encode('utf-8'))
            
            # Log progress every 10 seconds or 10000 records
            current_time = time.time()
            if record_count % 10000 == 0 or (current_time - last_log_time) >= 10:
                elapsed = current_time - start_time
                rate = record_count / elapsed if elapsed > 0 else 0
                self.logger.info(
                    f"Progress: {record_count:,} records extracted ({rate:.0f} records/sec)"
                )
                last_log_time = current_time
            
            yield converted_record
        
        # Update metrics
        self._metrics["records_extracted"] = record_count
        self._metrics["bytes_extracted"] = bytes_count
        self._metrics["extraction_time"] = time.time() - start_time
        
        # Log final metrics
        self._log_metrics()
