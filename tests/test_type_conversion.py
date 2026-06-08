"""Tests for type conversion."""
import json
import pytest
from unittest.mock import Mock, MagicMock
from datetime import datetime
from bson import ObjectId, Decimal128
from tap_mongodb.streams import MongoDBStream


@pytest.fixture
def mock_tap():
    """Mock tap instance."""
    tap = Mock()
    tap.config = {
        "strategy": "flexible",
        "infer_schema_max_docs": 100,
        "batch_size": 1000,
        "max_retries": 3,
        "retry_delay": 1,
        "retry_backoff": 2,
    }
    tap.logger = Mock()
    return tap


@pytest.fixture
def mock_collection():
    """Mock MongoDB collection."""
    collection = MagicMock()
    collection.name = "test_collection"
    return collection


@pytest.fixture
def stream(mock_tap, mock_collection):
    """Create stream instance."""
    return MongoDBStream(
        tap=mock_tap,
        name="test_stream",
        collection=mock_collection,
    )


class TestTypeConversion:
    """Test type conversion functionality."""
    
    def test_convert_objectid(self, stream):
        """Test ObjectId to string conversion."""
        oid = ObjectId("507f1f77bcf86cd799439011")
        result = stream._convert_value(oid)
        
        assert result == "507f1f77bcf86cd799439011"
        assert isinstance(result, str)
    
    def test_convert_datetime(self, stream):
        """Test datetime to ISO string conversion."""
        dt = datetime(2024, 1, 15, 10, 30, 45)
        result = stream._convert_value(dt)
        
        assert result == "2024-01-15T10:30:45"
        assert isinstance(result, str)
    
    def test_convert_datetime_with_microseconds(self, stream):
        """Test datetime with microseconds."""
        dt = datetime(2024, 1, 15, 10, 30, 45, 123456)
        result = stream._convert_value(dt)
        
        assert result == "2024-01-15T10:30:45.123456"
        assert isinstance(result, str)

    def test_json_default_objectid(self, stream):
        """Test JSON default conversion for ObjectId."""
        oid = ObjectId("507f1f77bcf86cd799439011")
        assert stream._json_default(oid) == "507f1f77bcf86cd799439011"

    def test_json_default_datetime(self, stream):
        """Test JSON default conversion for datetime."""
        dt = datetime(2024, 1, 15, 10, 30, 45)
        assert stream._json_default(dt) == "2024-01-15T10:30:45"

    def test_json_default_bson_type(self, stream):
        """Test JSON default conversion for BSON types."""
        value = Decimal128("10.5")
        assert stream._json_default(value) == "10.5"

    def test_json_default_unsupported_type(self, stream):
        """Test unsupported JSON default conversion raises TypeError."""
        class Unsupported:
            pass

        with pytest.raises(TypeError, match="not JSON serializable"):
            stream._json_default(Unsupported())
    
    def test_convert_simple_dict(self, stream):
        """Test simple dictionary conversion."""
        data = {
            "name": "John",
            "age": 30,
            "active": True
        }
        result = stream._convert_value(data)
        
        assert isinstance(result, str)
        assert json.loads(result) == data
    
    def test_convert_nested_dict_with_objectid(self, stream):
        """Test nested dictionary with ObjectId."""
        data = {
            "user": {
                "_id": ObjectId("507f1f77bcf86cd799439011"),
                "name": "John"
            }
        }
        result = stream._convert_value(data)
        
        parsed = json.loads(result)
        assert parsed["user"]["_id"] == "507f1f77bcf86cd799439011"
        assert parsed["user"]["name"] == "John"
    
    def test_convert_nested_dict_with_datetime(self, stream):
        """Test nested dictionary with datetime."""
        data = {
            "event": {
                "timestamp": datetime(2024, 1, 1, 12, 0, 0),
                "type": "login"
            }
        }
        result = stream._convert_value(data)
        
        parsed = json.loads(result)
        assert parsed["event"]["timestamp"] == "2024-01-01T12:00:00"
        assert parsed["event"]["type"] == "login"
    
    def test_convert_list_of_primitives(self, stream):
        """Test list of primitive values."""
        data = [1, 2, 3, "four", 5.0, True]
        result = stream._convert_value(data)
        
        assert isinstance(result, str)
        assert json.loads(result) == data
    
    def test_convert_list_of_objectids(self, stream):
        """Test list of ObjectIds."""
        data = [
            ObjectId("507f1f77bcf86cd799439011"),
            ObjectId("507f1f77bcf86cd799439012"),
        ]
        result = stream._convert_value(data)
        
        parsed = json.loads(result)
        assert parsed[0] == "507f1f77bcf86cd799439011"
        assert parsed[1] == "507f1f77bcf86cd799439012"
    
    def test_convert_list_of_dicts(self, stream):
        """Test list of dictionaries."""
        data = [
            {"_id": ObjectId("507f1f77bcf86cd799439011"), "name": "John"},
            {"_id": ObjectId("507f1f77bcf86cd799439012"), "name": "Jane"},
        ]
        result = stream._convert_value(data)
        
        parsed = json.loads(result)
        assert parsed[0]["_id"] == "507f1f77bcf86cd799439011"
        assert parsed[1]["_id"] == "507f1f77bcf86cd799439012"
    
    def test_convert_deeply_nested_structure(self, stream):
        """Test deeply nested structure."""
        data = {
            "level1": {
                "level2": {
                    "level3": {
                        "_id": ObjectId("507f1f77bcf86cd799439011"),
                        "timestamp": datetime(2024, 1, 1),
                        "items": [
                            {"id": ObjectId("507f1f77bcf86cd799439012")},
                            {"id": ObjectId("507f1f77bcf86cd799439013")},
                        ]
                    }
                }
            }
        }
        result = stream._convert_value(data)
        
        parsed = json.loads(result)
        level3 = parsed["level1"]["level2"]["level3"]
        assert level3["_id"] == "507f1f77bcf86cd799439011"
        assert level3["timestamp"] == "2024-01-01T00:00:00"
        assert level3["items"][0]["id"] == "507f1f77bcf86cd799439012"
    
    def test_convert_none_value(self, stream):
        """Test None value conversion."""
        result = stream._convert_value(None)
        assert result is None
    
    def test_convert_empty_dict(self, stream):
        """Test empty dictionary."""
        result = stream._convert_value({})
        assert result == "{}"
    
    def test_convert_empty_list(self, stream):
        """Test empty list."""
        result = stream._convert_value([])
        assert result == "[]"
    
    def test_convert_mixed_types_in_dict(self, stream):
        """Test dictionary with mixed types."""
        data = {
            "_id": ObjectId("507f1f77bcf86cd799439011"),
            "name": "John",
            "age": 30,
            "balance": 1000.50,
            "active": True,
            "created_at": datetime(2024, 1, 1),
            "tags": ["tag1", "tag2"],
            "metadata": {"key": "value"},
            "nullable": None,
        }
        result = stream._convert_value(data)
        
        parsed = json.loads(result)
        assert parsed["_id"] == "507f1f77bcf86cd799439011"
        assert parsed["name"] == "John"
        assert parsed["age"] == 30
        assert parsed["balance"] == 1000.50
        assert parsed["active"] is True
        assert parsed["created_at"] == "2024-01-01T00:00:00"
        assert parsed["tags"] == ["tag1", "tag2"]
        assert parsed["metadata"] == {"key": "value"}
        assert parsed["nullable"] is None
    
    def test_convert_complete_document(self, stream):
        """Test conversion of a complete MongoDB document."""
        doc = {
            "_id": ObjectId("507f1f77bcf86cd799439011"),
            "user": {
                "name": "John Doe",
                "email": "john@example.com",
                "profile": {
                    "avatar_id": ObjectId("507f1f77bcf86cd799439012"),
                    "created_at": datetime(2024, 1, 1, 10, 0, 0),
                }
            },
            "orders": [
                {
                    "order_id": ObjectId("507f1f77bcf86cd799439013"),
                    "date": datetime(2024, 1, 15),
                    "total": 99.99,
                },
                {
                    "order_id": ObjectId("507f1f77bcf86cd799439014"),
                    "date": datetime(2024, 1, 20),
                    "total": 149.99,
                }
            ],
            "metadata": {
                "created_at": datetime(2024, 1, 1),
                "updated_at": datetime(2024, 1, 20),
                "version": 1,
            }
        }
        
        result = stream._convert_value(doc)
        
        parsed = json.loads(result)
        # Verify top level
        assert parsed["_id"] == "507f1f77bcf86cd799439011"
        
        # Verify nested user
        assert parsed["user"]["name"] == "John Doe"
        assert parsed["user"]["profile"]["avatar_id"] == "507f1f77bcf86cd799439012"
        assert parsed["user"]["profile"]["created_at"] == "2024-01-01T10:00:00"
        
        # Verify orders array
        assert len(parsed["orders"]) == 2
        assert parsed["orders"][0]["order_id"] == "507f1f77bcf86cd799439013"
        assert parsed["orders"][0]["date"] == "2024-01-15T00:00:00"
        assert parsed["orders"][1]["order_id"] == "507f1f77bcf86cd799439014"
        
        # Verify metadata
        assert parsed["metadata"]["created_at"] == "2024-01-01T00:00:00"
        assert parsed["metadata"]["updated_at"] == "2024-01-20T00:00:00"
        assert parsed["metadata"]["version"] == 1
