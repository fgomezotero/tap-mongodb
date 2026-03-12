"""Shared pytest fixtures."""
import pytest
from unittest.mock import Mock


@pytest.fixture
def base_config():
    """Base configuration for all tests."""
    return {
        "host": "localhost",
        "port": 27017,
        "database": "test_db",
        "strategy": "flexible",
        "infer_schema_max_docs": 100,
        "batch_size": 1000,
        "max_retries": 3,
        "retry_delay": 1,
        "retry_backoff": 2,
    }


@pytest.fixture
def mock_logger():
    """Mock logger."""
    logger = Mock()
    logger.info = Mock()
    logger.warning = Mock()
    logger.error = Mock()
    logger.debug = Mock()
    return logger
