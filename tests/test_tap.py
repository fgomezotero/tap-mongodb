import pytest
from unittest.mock import Mock, MagicMock, patch
from tap_mongodb.tap import TapMongoDB
from pymongo.errors import ConnectionFailure


class TestTapMongoDB:
    """Test cases for TapMongoDB class."""
    
    @pytest.fixture
    def mock_config(self):
        return {
            "host": "localhost",
            "port": 27017,
            "database": "test_db",
            "username": "test_user",
            "password": "test_pass",
            "auth_source": "admin",
        }
    
    @patch('tap_mongodb.tap.TapMongoDB.discover_streams')
    def test_tap_initialization(self, mock_discover, mock_config):
        """Test tap initialization."""
        mock_discover.return_value = []
        tap = TapMongoDB(config=mock_config, parse_env_config=False, validate_config=False)
        assert tap.config["database"] == "test_db"
        assert tap._client is None
    
    def test_config_validation_missing_database(self):
        """Test config validation fails without database."""
        config = {"host": "localhost"}
        with pytest.raises(Exception):
            TapMongoDB(config=config, parse_env_config=False)
    
    @patch('pymongo.MongoClient')
    def test_get_client_with_host_port(self, mock_mongo_client, mock_config):
        """Test client creation with host/port."""
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance
        
        mock_config["directConnection"] = True
        
        with patch('tap_mongodb.tap.TapMongoDB.discover_streams', return_value=[]):
            tap = TapMongoDB(config=mock_config, parse_env_config=False, validate_config=False)
            client = tap._get_client()
            
            assert client == mock_client_instance
    
    @patch('pymongo.MongoClient')
    def test_get_client_with_connection_string(self, mock_mongo_client):
        """Test client creation with connection string."""
        config = {
            "connection_string": "mongodb://localhost:27017",
            "database": "test_db",
        }
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance
        
        with patch('tap_mongodb.tap.TapMongoDB.discover_streams', return_value=[]):
            tap = TapMongoDB(config=config, parse_env_config=False, validate_config=False)
            client = tap._get_client()
            
            assert client == mock_client_instance
            args, kwargs = mock_mongo_client.call_args
            assert args[0] == "mongodb://localhost:27017"
    
    @patch('pymongo.MongoClient')
    def test_get_client_caching(self, mock_mongo_client, mock_config):
        """Test client is cached after first creation."""
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance
        
        mock_config["directConnection"] = True
        
        with patch('tap_mongodb.tap.TapMongoDB.discover_streams', return_value=[]):
            tap = TapMongoDB(config=mock_config, parse_env_config=False, validate_config=False)
            client1 = tap._get_client()
            client2 = tap._get_client()
            
            assert client1 == client2
    
    @patch('pymongo.MongoClient')
    def test_detect_deployment_type_standalone(self, mock_mongo_client, mock_config):
        """Test detection of standalone deployment."""
        mock_client_instance = MagicMock()
        mock_client_instance.admin.command.return_value = {"ismaster": True}
        mock_mongo_client.return_value = mock_client_instance
        
        with patch('tap_mongodb.tap.TapMongoDB.discover_streams', return_value=[]):
            tap = TapMongoDB(config=mock_config, parse_env_config=False, validate_config=False)
            deployment_type = tap._detect_deployment_type(mock_client_instance)
            
            assert deployment_type == "standalone"
    
    @patch('pymongo.MongoClient')
    def test_detect_deployment_type_replicaset(self, mock_mongo_client, mock_config):
        """Test detection of replica set deployment."""
        mock_client_instance = MagicMock()
        mock_client_instance.admin.command.return_value = {
            "ismaster": True,
            "setName": "rs0"
        }
        mock_mongo_client.return_value = mock_client_instance
        
        with patch('tap_mongodb.tap.TapMongoDB.discover_streams', return_value=[]):
            tap = TapMongoDB(config=mock_config, parse_env_config=False, validate_config=False)
            deployment_type = tap._detect_deployment_type(mock_client_instance)
            
            assert deployment_type == "replicaset"
    
    @patch('pymongo.MongoClient')
    @patch('tap_mongodb.streams.MongoDBStream.validate_replication_key')
    def test_discover_streams(self, mock_validate, mock_mongo_client, mock_config):
        """Test stream discovery."""
        mock_client_instance = MagicMock()
        mock_db = MagicMock()
        mock_db.list_collection_names.return_value = ["collection1", "collection2"]
        mock_client_instance.__getitem__.return_value = mock_db
        mock_mongo_client.return_value = mock_client_instance
        
        mock_config["directConnection"] = True
        
        tap = TapMongoDB(config=mock_config, parse_env_config=False, validate_config=False)
        streams = tap.discover_streams()
        assert len(streams) == 2
    
    @patch('pymongo.MongoClient')
    @patch('tap_mongodb.streams.MongoDBStream.validate_replication_key')
    def test_discover_streams_auto_discovery(self, mock_validate, mock_mongo_client, mock_config):
        """Test auto-discovery when collections not specified."""
        mock_client_instance = MagicMock()
        mock_db = MagicMock()
        mock_db.list_collection_names.return_value = ["col1", "col2", "col3"]
        mock_client_instance.__getitem__.return_value = mock_db
        mock_mongo_client.return_value = mock_client_instance
        
        mock_config["directConnection"] = True
        
        tap = TapMongoDB(config=mock_config, parse_env_config=False, validate_config=False)
        streams = tap.discover_streams()
        assert len(streams) == 3
    
    @patch('pymongo.MongoClient')
    def test_cleanup(self, mock_mongo_client, mock_config):
        """Test cleanup closes connection."""
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance
        
        mock_config["directConnection"] = True
        
        with patch('tap_mongodb.tap.TapMongoDB.discover_streams', return_value=[]):
            tap = TapMongoDB(config=mock_config, parse_env_config=False, validate_config=False)
            tap._get_client()
            
            mock_client_instance.close.reset_mock()
            tap.cleanup()
            
            mock_client_instance.close.assert_called_once()
            assert tap._client is None
    
    @patch('pymongo.MongoClient')
    def test_connection_failure(self, mock_mongo_client, mock_config):
        """Test handling of connection failures."""
        mock_client_instance = MagicMock()
        mock_client_instance.admin.command.side_effect = ConnectionFailure("Connection failed")
        mock_mongo_client.return_value = mock_client_instance
        
        with patch('tap_mongodb.tap.TapMongoDB.discover_streams', return_value=[]):
            tap = TapMongoDB(config=mock_config, parse_env_config=False, validate_config=False)
            
            with pytest.raises(ConnectionFailure):
                tap._get_client()
    
    @patch('tap_mongodb.tap.TapMongoDB.discover_streams')
    def test_stream_config_merge(self, mock_discover, mock_config):
        """Test stream-specific config merges with global config."""
        config = mock_config.copy()
        config["replication_method"] = "FULL_TABLE"
        config["stream_configs"] = {
            "collection1": {
                "replication_method": "INCREMENTAL",
                "replication_key": "updated_at",
            }
        }
        
        mock_discover.return_value = []
        tap = TapMongoDB(config=config, parse_env_config=False, validate_config=False)
        assert tap.config["replication_method"] == "FULL_TABLE"
        assert tap.config["stream_configs"]["collection1"]["replication_method"] == "INCREMENTAL"
