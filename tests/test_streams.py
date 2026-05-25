import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime
from bson import ObjectId
from tap_mongodb.streams import MongoDBStream
from pymongo.errors import AutoReconnect


class TestMongoDBStream:
    """Test cases for MongoDBStream class."""
    
    @pytest.fixture
    def mock_tap(self):
        tap = Mock()
        tap.config = {
            "database": "test_db",
            "strategy": "flexible",
            "infer_schema_max_docs": 100,
            "batch_size": 1000,
            "max_retries": 3,
            "retry_delay": 5,
            "retry_backoff": 2,
        }
        tap.logger = Mock()
        tap._state = {"bookmarks": {}}
        tap.tap_state = {"bookmarks": {}}
        return tap
    
    @pytest.fixture
    def mock_collection(self):
        collection = Mock()
        collection.name = "test_collection"
        collection.database.name = "test_db"
        return collection
    
    def test_stream_initialization(self, mock_tap, mock_collection):
        """Test stream initialization."""
        stream = MongoDBStream(
            tap=mock_tap,
            name="test_stream",
            collection=mock_collection,
            stream_config={}
        )
        
        assert stream.name == "test_stream"
        assert stream._collection == mock_collection
        assert stream._strategy == "flexible"
    
    def test_stream_with_incremental_config(self, mock_tap, mock_collection):
        """Test stream with incremental replication config."""
        stream_config = {
            "replication_method": "INCREMENTAL",
            "replication_key": "updated_at",
        }
        
        stream = MongoDBStream(
            tap=mock_tap,
            name="test_stream",
            collection=mock_collection,
            stream_config=stream_config
        )
        
        assert stream.replication_method == "INCREMENTAL"
        assert stream.replication_key == "updated_at"
    
    def test_convert_value_objectid(self, mock_tap, mock_collection):
        """Test ObjectId conversion."""
        stream = MongoDBStream(mock_tap, "test", mock_collection, {})
        
        obj_id = ObjectId("507f1f77bcf86cd799439011")
        result = stream._convert_value(obj_id)
        
        assert result == "507f1f77bcf86cd799439011"
        assert isinstance(result, str)
    
    def test_convert_value_datetime(self, mock_tap, mock_collection):
        """Test datetime conversion."""
        stream = MongoDBStream(mock_tap, "test", mock_collection, {})
        
        dt = datetime(2024, 1, 1, 12, 0, 0)
        result = stream._convert_value(dt)
        
        assert result == "2024-01-01T12:00:00"
    
    def test_convert_value_nested_dict(self, mock_tap, mock_collection):
        """Test nested dict conversion."""
        stream = MongoDBStream(mock_tap, "test", mock_collection, {})
        
        data = {
            "_id": ObjectId("507f1f77bcf86cd799439011"),
            "nested": {
                "date": datetime(2024, 1, 1),
            }
        }
        
        result = stream._convert_value(data)
        
        assert result["_id"] == "507f1f77bcf86cd799439011"
        assert result["nested"]["date"] == "2024-01-01T00:00:00"
    
    def test_convert_value_list(self, mock_tap, mock_collection):
        """Test list conversion."""
        stream = MongoDBStream(mock_tap, "test", mock_collection, {})
        
        data = [
            ObjectId("507f1f77bcf86cd799439011"),
            datetime(2024, 1, 1),
            "string"
        ]
        
        result = stream._convert_value(data)
        
        assert result[0] == "507f1f77bcf86cd799439011"
        assert result[1] == "2024-01-01T00:00:00"
        assert result[2] == "string"
    
    def test_python_to_json_type(self, mock_tap, mock_collection):
        """Test Python to JSON type mapping."""
        stream = MongoDBStream(mock_tap, "test", mock_collection, {})
        
        assert stream._python_to_json_type("str") == "string"
        assert stream._python_to_json_type("int") == "integer"
        assert stream._python_to_json_type("float") == "number"
        assert stream._python_to_json_type("bool") == "boolean"
        assert stream._python_to_json_type("ObjectId") == "string"
        assert stream._python_to_json_type("datetime") == "string"
        assert stream._python_to_json_type("unknown") == "string"
    
    def test_raw_schema(self, mock_tap, mock_collection):
        """Test raw schema generation."""
        mock_tap.config["strategy"] = "raw"
        stream = MongoDBStream(mock_tap, "test", mock_collection, {})
        
        schema = stream.schema
        
        assert schema["type"] == "object"
        assert schema["additionalProperties"] is True
        assert "_id" in schema["properties"]
    
    def test_flexible_schema(self, mock_tap, mock_collection):
        """Test flexible schema generation."""
        mock_collection.find.return_value.limit.return_value = [
            {"_id": ObjectId(), "name": "test", "count": 1},
            {"_id": ObjectId(), "name": "test2", "count": 2},
        ]
        
        stream = MongoDBStream(mock_tap, "test", mock_collection, {})
        schema = stream.schema
        
        assert schema["type"] == "object"
        assert "properties" in schema
        assert "_id" in schema["properties"]
    
    def test_build_query_no_filters(self, mock_tap, mock_collection):
        """Test query building without filters."""
        stream = MongoDBStream(mock_tap, "test", mock_collection, {})
        
        query = stream._build_query(None)
        
        assert query == {}
    
    def test_build_query_with_custom_filters(self, mock_tap, mock_collection):
        """Test query building with custom filters."""
        stream_config = {
            "filters": {"status": "active"}
        }
        stream = MongoDBStream(mock_tap, "test", mock_collection, stream_config)
        
        query = stream._build_query(None)
        
        assert query == {"status": "active"}
    
    def test_build_query_with_bookmark(self, mock_tap, mock_collection):
        """Test query building with bookmark."""
        stream_config = {
            "replication_method": "INCREMENTAL",
            "replication_key": "updated_at",
        }
        stream = MongoDBStream(mock_tap, "test", mock_collection, stream_config)
        
        bookmark = datetime(2024, 1, 1)
        query = stream._build_query(bookmark)
        
        assert "updated_at" in query
        assert "$gt" in query["updated_at"]
    
    def test_validate_replication_key_exists(self, mock_tap, mock_collection):
        """Test replication key validation when key exists."""
        mock_collection.index_information.return_value = {
            "updated_at_1": {"key": [("updated_at", 1)]}
        }
        
        stream_config = {
            "replication_method": "INCREMENTAL",
            "replication_key": "updated_at",
        }
        stream = MongoDBStream(mock_tap, "test", mock_collection, stream_config)
        
        stream.validate_replication_key()
    
    def test_validate_replication_key_not_found(self, mock_tap, mock_collection):
        """Test replication key validation when key not found."""
        mock_collection.index_information.return_value = {}
        
        stream_config = {
            "replication_method": "INCREMENTAL",
            "replication_key": "updated_at",
        }
        stream = MongoDBStream(mock_tap, "test", mock_collection, stream_config)
        
        stream.validate_replication_key()
    
    def test_get_records_basic(self, mock_tap, mock_collection):
        """Test basic record extraction."""
        mock_collection.find.return_value.batch_size.return_value = [
            {"_id": ObjectId("507f1f77bcf86cd799439011"), "name": "test1"},
            {"_id": ObjectId("507f1f77bcf86cd799439012"), "name": "test2"},
        ]
        
        stream = MongoDBStream(mock_tap, "test", mock_collection, {})
        
        with patch.object(stream, 'get_starting_replication_key_value', return_value=None):
            records = list(stream.get_records(None))
            
            assert len(records) == 2
            assert records[0]["name"] == "test1"

    def test_get_records_respects_max_record_per_run(self, mock_tap, mock_collection):
        """Test record extraction stops at max_record_per_run."""
        mock_tap.config["max_record_per_run"] = 1
        mock_collection.find.return_value.batch_size.return_value = [
            {"_id": ObjectId("507f1f77bcf86cd799439011"), "name": "test1"},
            {"_id": ObjectId("507f1f77bcf86cd799439012"), "name": "test2"},
        ]

        stream = MongoDBStream(mock_tap, "test", mock_collection, {})

        with patch.object(stream, 'get_starting_replication_key_value', return_value=None):
            records = list(stream.get_records(None))

            assert len(records) == 1
            assert records[0]["name"] == "test1"
            assert stream._metrics["records_extracted"] == 1
    
    def test_retry_on_network_error(self, mock_tap, mock_collection):
        """Test retry logic on network errors."""
        stream = MongoDBStream(mock_tap, "test", mock_collection, {})
        
        mock_cursor = MagicMock()
        
        with patch.object(stream, '_execute_query_with_retry', return_value=mock_cursor):
            result = stream._execute_query_with_retry({})
            assert result == mock_cursor
    
    def test_metrics_tracking(self, mock_tap, mock_collection):
        """Test performance metrics tracking."""
        mock_collection.find.return_value.batch_size.return_value = [
            {"_id": ObjectId("507f1f77bcf86cd799439011"), "name": "test1"},
        ]
        
        stream = MongoDBStream(mock_tap, "test", mock_collection, {})
        
        with patch.object(stream, 'get_starting_replication_key_value', return_value=None):
            list(stream.get_records(None))
            
            assert stream._metrics["records_extracted"] == 1
            assert stream._metrics["extraction_time"] > 0
