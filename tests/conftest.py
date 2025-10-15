"""
Pytest configuration and shared fixtures
"""
import pytest
import os
import tempfile
from unittest.mock import Mock, MagicMock
from datetime import datetime, timedelta
from faker import Faker

# Add project root to Python path
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import DatabaseConfig, MigrationConfig, LoggingConfig
from database import DatabaseManager


@pytest.fixture
def fake():
    """Faker instance for generating test data"""
    return Faker()


@pytest.fixture
def mock_db_config():
    """Mock database configuration"""
    config = Mock(spec=DatabaseConfig)
    config.host = "localhost"
    config.port = 5432
    config.database = "test_db"
    config.user = "test_user"
    config.password = "test_password"
    config.connection_string = "postgresql://test_user:test_password@localhost:5432/test_db"
    return config


@pytest.fixture
def mock_migration_config():
    """Mock migration configuration"""
    config = Mock(spec=MigrationConfig)
    config.batch_size = 1000
    config.sample_minutes = 5
    config.migration_timeout = 1800
    config.chunk_size = 100
    config.VALUE_FORMAT_MAPPING = {
        'Decimal': 'double_v',
        'Integer': 'long_v',
        'String': 'str_v',
        'Boolean': 'bool_v'
    }
    return config


@pytest.fixture
def mock_logging_config():
    """Mock logging configuration"""
    config = Mock(spec=LoggingConfig)
    config.level = "DEBUG"
    config.log_file = "test.log"
    config.max_file_size = "1MB"
    config.backup_count = 3
    return config


@pytest.fixture
def mock_database_manager():
    """Mock database manager"""
    manager = Mock(spec=DatabaseManager)
    manager.connection_string = "postgresql://test_user:test_password@localhost:5432/test_db"
    manager._connection = None
    
    # Mock common database operations
    manager.execute_query.return_value = []
    manager.execute_update.return_value = 1
    manager.execute_batch.return_value = 1
    manager.check_table_exists.return_value = False
    manager.get_table_info.return_value = []
    manager.get_distinct_ship_ids.return_value = ["IMO9976903", "IMO9976915"]
    
    return manager


@pytest.fixture
def sample_narrow_data(fake):
    """Sample narrow table data for testing"""
    return [
        {
            'id': 1,
            'ship_id': 'IMO9976903',
            'data_channel_id': 'engine_rpm',
            'created_time': datetime.now() - timedelta(minutes=5),
            'server_created_time': datetime.now() - timedelta(minutes=5),
            'bool_v': None,
            'str_v': None,
            'long_v': 1500,
            'double_v': None,
            'value_format': 'Integer'
        },
        {
            'id': 2,
            'ship_id': 'IMO9976903',
            'data_channel_id': 'fuel_level',
            'created_time': datetime.now() - timedelta(minutes=5),
            'server_created_time': datetime.now() - timedelta(minutes=5),
            'bool_v': None,
            'str_v': None,
            'long_v': None,
            'double_v': 75.5,
            'value_format': 'Decimal'
        },
        {
            'id': 3,
            'ship_id': 'IMO9976903',
            'data_channel_id': 'engine_status',
            'created_time': datetime.now() - timedelta(minutes=5),
            'server_created_time': datetime.now() - timedelta(minutes=5),
            'bool_v': True,
            'str_v': None,
            'long_v': None,
            'double_v': None,
            'value_format': 'Boolean'
        },
        {
            'id': 4,
            'ship_id': 'IMO9976903',
            'data_channel_id': 'location',
            'created_time': datetime.now() - timedelta(minutes=5),
            'server_created_time': datetime.now() - timedelta(minutes=5),
            'bool_v': None,
            'str_v': '37.7749,-122.4194',
            'long_v': None,
            'double_v': None,
            'value_format': 'String'
        }
    ]


@pytest.fixture
def sample_wide_schema():
    """Sample wide table schema for testing"""
    return {
        'ship_id': 'IMO9976903',
        'table_name': 'tbl_data_timeseries_IMO9976903',
        'columns': [
            {
                'name': 'created_time',
                'type': 'timestamp',
                'nullable': False,
                'description': 'Primary key - timestamp of data collection'
            },
            {
                'name': 'engine_rpm',
                'type': 'text',
                'nullable': True,
                'description': 'Data channel: engine_rpm',
                'primary_format': 'Integer',
                'value_formats': ['Integer']
            },
            {
                'name': 'fuel_level',
                'type': 'text',
                'nullable': True,
                'description': 'Data channel: fuel_level',
                'primary_format': 'Decimal',
                'value_formats': ['Decimal']
            },
            {
                'name': 'engine_status',
                'type': 'text',
                'nullable': True,
                'description': 'Data channel: engine_status',
                'primary_format': 'Boolean',
                'value_formats': ['Boolean']
            },
            {
                'name': 'location',
                'type': 'text',
                'nullable': True,
                'description': 'Data channel: location',
                'primary_format': 'String',
                'value_formats': ['String']
            }
        ],
        'primary_key': 'created_time',
        'indexes': ['created_time'],
        'sample_count': 4,
        'data_channels': ['engine_rpm', 'fuel_level', 'engine_status', 'location']
    }


@pytest.fixture
def temp_log_file():
    """Temporary log file for testing"""
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.log') as f:
        temp_file = f.name
    yield temp_file
    # Cleanup
    if os.path.exists(temp_file):
        os.unlink(temp_file)


@pytest.fixture
def mock_progress_callback():
    """Mock progress callback function"""
    return Mock()


@pytest.fixture
def sample_ship_ids():
    """Sample ship IDs for testing"""
    return ["IMO9976903", "IMO9976915", "IMO9976927", "IMO9976939"]


@pytest.fixture
def sample_data_channels():
    """Sample data channel IDs for testing"""
    return ["engine_rpm", "fuel_level", "engine_status", "location", "speed", "heading"]


@pytest.fixture
def sample_value_formats():
    """Sample value formats for testing"""
    return ["Decimal", "Integer", "String", "Boolean"]


@pytest.fixture(autouse=True)
def setup_test_environment(monkeypatch, mock_db_config, mock_migration_config, mock_logging_config):
    """Setup test environment by patching global configs"""
    monkeypatch.setattr('config.db_config', mock_db_config)
    monkeypatch.setattr('config.migration_config', mock_migration_config)
    monkeypatch.setattr('config.logging_config', mock_logging_config)
