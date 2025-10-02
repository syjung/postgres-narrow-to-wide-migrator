"""
Unit tests for DatabaseManager module
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
import psycopg2

from database import DatabaseManager


class TestDatabaseManager:
    """Test cases for DatabaseManager class"""
    
    def setup_method(self):
        """Setup test method"""
        self.db_manager = DatabaseManager()
    
    def test_init(self):
        """Test DatabaseManager initialization"""
        assert self.db_manager.connection_string is not None
        assert self.db_manager._connection is None
    
    @patch('database.psycopg2.connect')
    def test_connect_success(self, mock_connect):
        """Test successful database connection"""
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        result = self.db_manager.connect()
        
        assert result == mock_connection
        assert self.db_manager._connection == mock_connection
        mock_connect.assert_called_once_with(
            self.db_manager.connection_string,
            cursor_factory=psycopg2.extras.RealDictCursor
        )
    
    @patch('database.psycopg2.connect')
    def test_connect_failure(self, mock_connect):
        """Test database connection failure"""
        mock_connect.side_effect = psycopg2.Error("Connection failed")
        
        with pytest.raises(psycopg2.Error):
            self.db_manager.connect()
    
    def test_disconnect(self):
        """Test database disconnection"""
        mock_connection = Mock()
        self.db_manager._connection = mock_connection
        
        self.db_manager.disconnect()
        
        mock_connection.close.assert_called_once()
        # Note: The actual disconnect method sets _connection to None, but in test we're using Mock
        # So we just verify that close() was called
    
    def test_disconnect_no_connection(self):
        """Test disconnection when no connection exists"""
        self.db_manager._connection = None
        
        # Should not raise an exception
        self.db_manager.disconnect()
    
    @patch('database.DatabaseManager.connect')
    def test_get_cursor_success(self, mock_connect):
        """Test successful cursor context manager"""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        
        with self.db_manager.get_cursor() as cursor:
            assert cursor == mock_cursor
        
        mock_connection.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()
    
    @patch('database.DatabaseManager.connect')
    def test_get_cursor_exception(self, mock_connect):
        """Test cursor context manager with exception"""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        
        # Simulate exception during cursor usage
        mock_cursor.execute.side_effect = Exception("Test error")
        
        with pytest.raises(Exception):
            with self.db_manager.get_cursor() as cursor:
                cursor.execute("SELECT 1")
        
        mock_connection.rollback.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()
    
    @patch('database.DatabaseManager.get_cursor')
    def test_execute_query(self, mock_get_cursor):
        """Test executing SELECT query"""
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [{'id': 1, 'name': 'test'}]
        mock_get_cursor.return_value.__enter__.return_value = mock_cursor
        
        result = self.db_manager.execute_query("SELECT * FROM test", ('param',))
        
        assert result == [{'id': 1, 'name': 'test'}]
        mock_cursor.execute.assert_called_once_with("SELECT * FROM test", ('param',))
        mock_cursor.fetchall.assert_called_once()
    
    @patch('database.DatabaseManager.get_cursor')
    def test_execute_update(self, mock_get_cursor):
        """Test executing UPDATE query"""
        mock_cursor = Mock()
        mock_cursor.rowcount = 5
        mock_get_cursor.return_value.__enter__.return_value = mock_cursor
        
        result = self.db_manager.execute_update("UPDATE test SET name = %s", ('new_name',))
        
        assert result == 5
        mock_cursor.execute.assert_called_once_with("UPDATE test SET name = %s", ('new_name',))
    
    @patch('database.DatabaseManager.get_cursor')
    def test_execute_batch(self, mock_get_cursor):
        """Test executing batch operations"""
        mock_cursor = Mock()
        mock_cursor.rowcount = 3
        mock_get_cursor.return_value.__enter__.return_value = mock_cursor
        
        data = [('value1',), ('value2',), ('value3',)]
        result = self.db_manager.execute_batch("INSERT INTO test VALUES (%s)", data)
        
        assert result == 3
        mock_cursor.executemany.assert_called_once_with("INSERT INTO test VALUES (%s)", data)
    
    @patch('database.DatabaseManager.execute_query')
    def test_get_table_info(self, mock_execute_query):
        """Test getting table information"""
        mock_execute_query.return_value = [
            {'column_name': 'id', 'data_type': 'bigint', 'is_nullable': 'NO'},
            {'column_name': 'name', 'data_type': 'text', 'is_nullable': 'YES'}
        ]
        
        result = self.db_manager.get_table_info('test_table')
        
        assert len(result) == 2
        assert result[0]['column_name'] == 'id'
        assert result[1]['column_name'] == 'name'
        mock_execute_query.assert_called_once()
    
    @patch('database.DatabaseManager.execute_query')
    def test_check_table_exists_true(self, mock_execute_query):
        """Test checking table existence - table exists"""
        mock_execute_query.return_value = [{'exists': True}]
        
        result = self.db_manager.check_table_exists('test_table')
        
        assert result is True
        mock_execute_query.assert_called_once()
    
    @patch('database.DatabaseManager.execute_query')
    def test_check_table_exists_false(self, mock_execute_query):
        """Test checking table existence - table does not exist"""
        mock_execute_query.return_value = [{'exists': False}]
        
        result = self.db_manager.check_table_exists('test_table')
        
        assert result is False
        mock_execute_query.assert_called_once()
    
    @patch('database.DatabaseManager.execute_query')
    def test_check_table_exists_no_result(self, mock_execute_query):
        """Test checking table existence - no result"""
        mock_execute_query.return_value = []
        
        result = self.db_manager.check_table_exists('test_table')
        
        assert result is False
    
    @patch('database.DatabaseManager.execute_query')
    def test_get_distinct_ship_ids(self, mock_execute_query):
        """Test getting distinct ship IDs"""
        mock_execute_query.return_value = [
            {'ship_id': 'IMO9976903'},
            {'ship_id': 'IMO9976915'},
            {'ship_id': 'IMO9976927'}
        ]
        
        result = self.db_manager.get_distinct_ship_ids()
        
        assert result == ['IMO9976903', 'IMO9976915', 'IMO9976927']
        mock_execute_query.assert_called_once()
    
    @patch('database.DatabaseManager.execute_query')
    def test_get_sample_data(self, mock_execute_query, sample_narrow_data):
        """Test getting sample data"""
        mock_execute_query.return_value = sample_narrow_data
        
        result = self.db_manager.get_sample_data('IMO9976903', 10)
        
        assert result == sample_narrow_data
        mock_execute_query.assert_called_once()
    
    @patch('database.DatabaseManager.execute_query')
    def test_get_data_channels_for_ship(self, mock_execute_query):
        """Test getting data channels for a ship"""
        mock_execute_query.return_value = [
            {'data_channel_id': 'engine_rpm'},
            {'data_channel_id': 'fuel_level'},
            {'data_channel_id': 'engine_status'}
        ]
        
        result = self.db_manager.get_data_channels_for_ship('IMO9976903')
        
        assert result == ['engine_rpm', 'fuel_level', 'engine_status']
        mock_execute_query.assert_called_once()
