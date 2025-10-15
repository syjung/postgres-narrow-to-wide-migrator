"""
Integration tests for the complete migration flow
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta

from schema_analyzer import schema_analyzer
from table_generator import table_generator
from data_migrator import data_migrator
from realtime_processor import realtime_processor


class TestMigrationFlow:
    """Integration tests for the complete migration flow"""
    
    def setup_method(self):
        """Setup test method"""
        self.sample_ship_id = 'IMO9976903'
        self.sample_narrow_data = [
            {
                'id': 1,
                'ship_id': self.sample_ship_id,
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
                'ship_id': self.sample_ship_id,
                'data_channel_id': 'fuel_level',
                'created_time': datetime.now() - timedelta(minutes=5),
                'server_created_time': datetime.now() - timedelta(minutes=5),
                'bool_v': None,
                'str_v': None,
                'long_v': None,
                'double_v': 75.5,
                'value_format': 'Decimal'
            }
        ]
    
    @patch('schema_analyzer.db_manager')
    @patch('table_generator.db_manager')
    def test_complete_migration_flow(self, mock_table_db, mock_schema_db):
        """Test complete migration flow from schema analysis to data migration"""
        # Mock schema analysis
        mock_schema_db.get_sample_data.return_value = self.sample_narrow_data
        mock_schema_db.get_distinct_ship_ids.return_value = [self.sample_ship_id]
        
        # Mock table generation
        mock_table_db.check_table_exists.return_value = False
        mock_table_db.execute_update.return_value = 1
        mock_table_db.get_table_info.return_value = [
            {'column_name': 'created_time'},
            {'column_name': 'engine_rpm'},
            {'column_name': 'fuel_level'}
        ]
        
        # Step 1: Analyze schema
        schema = schema_analyzer.analyze_ship_data(self.sample_ship_id)
        
        assert schema['ship_id'] == self.sample_ship_id
        assert schema['table_name'] == f'tbl_data_timeseries_{self.sample_ship_id}'
        assert len(schema['columns']) >= 3  # created_time + data channels
        assert 'engine_rpm' in schema['data_channels']
        assert 'fuel_level' in schema['data_channels']
        
        # Step 2: Generate table
        table_result = table_generator.generate_table(schema)
        
        assert table_result is True
        
        # Step 3: Validate table structure
        issues = table_generator.validate_table_structure(schema['table_name'], schema)
        
        assert len(issues) == 0
    
    @patch('data_migrator.db_manager')
    @patch('data_migrator.table_generator')
    def test_data_migration_flow(self, mock_table_gen, mock_db):
        """Test data migration flow"""
        # Mock database operations
        mock_db.get_migration_count.return_value = 2
        mock_db.get_data_batches.return_value = [self.sample_narrow_data]
        mock_db.execute_batch.return_value = 1
        mock_db.execute_query.return_value = [{'count': 1}]
        
        # Mock table generator
        mock_table_gen.get_table_columns.return_value = ['created_time', 'engine_rpm', 'fuel_level']
        
        # Mock data integrity check
        mock_db.execute_query.side_effect = [
            [{'count': 1}],  # Count query
            [{'created_time': datetime.now() - timedelta(minutes=5), 'channel_count': 2}]  # Sample check
        ]
        
        # Execute migration
        result = data_migrator.migrate_ship_data(self.sample_ship_id)
        
        assert result['ship_id'] == self.sample_ship_id
        assert result['status'] == 'success'
        assert result['migrated_count'] > 0
        assert result['error_count'] == 0
    
    @patch('realtime_processor.db_manager')
    @patch('realtime_processor.table_generator')
    @patch('realtime_processor.schema_analyzer')
    def test_realtime_processing_flow(self, mock_schema_analyzer, mock_table_gen, mock_db):
        """Test real-time processing flow"""
        # Mock database operations
        mock_db.get_distinct_ship_ids.return_value = [self.sample_ship_id]
        mock_db.check_table_exists.return_value = True
        mock_db.execute_query.return_value = self.sample_narrow_data
        mock_db.execute_batch.return_value = 1
        
        # Mock table generator
        mock_table_gen.get_table_columns.return_value = ['created_time', 'engine_rpm', 'fuel_level']
        mock_table_gen.add_column_to_table.return_value = True
        
        # Mock schema analyzer
        mock_schema_analyzer.analyze_ship_data.return_value = {
            'ship_id': self.sample_ship_id,
            'table_name': f'tbl_data_timeseries_{self.sample_ship_id}',
            'columns': [
                {'name': 'created_time', 'type': 'timestamp', 'nullable': False},
                {'name': 'engine_rpm', 'type': 'text', 'nullable': True},
                {'name': 'fuel_level', 'type': 'text', 'nullable': True}
            ]
        }
        
        # Test processing single record
        record = self.sample_narrow_data[0]
        result = realtime_processor.process_single_record(record)
        
        assert result is True
    
    @patch('schema_analyzer.db_manager')
    @patch('table_generator.db_manager')
    @patch('data_migrator.db_manager')
    def test_error_handling_flow(self, mock_data_db, mock_table_db, mock_schema_db):
        """Test error handling throughout the migration flow"""
        # Mock schema analysis failure
        mock_schema_db.get_sample_data.side_effect = Exception("Database connection failed")
        
        # Test schema analysis with error
        schema = schema_analyzer.analyze_ship_data(self.sample_ship_id)
        
        # Should return empty schema when error occurs
        assert schema['ship_id'] == self.sample_ship_id
        assert len(schema['columns']) == 1  # Only created_time
        assert schema['sample_count'] == 0
        
        # Mock table generation failure
        mock_table_db.check_table_exists.return_value = False
        mock_table_db.execute_update.side_effect = Exception("Table creation failed")
        
        # Test table generation with error
        table_result = table_generator.generate_table(schema)
        
        assert table_result is False
        
        # Mock data migration failure
        mock_data_db.get_migration_count.side_effect = Exception("Migration count failed")
        
        # Test data migration with error
        migration_result = data_migrator.migrate_ship_data(self.sample_ship_id)
        
        assert migration_result['status'] == 'failed'
        assert migration_result['migrated_count'] == 0
        assert 'Migration failed' in migration_result['message']
    
    @patch('schema_analyzer.db_manager')
    def test_schema_validation_flow(self, mock_db):
        """Test schema validation flow"""
        mock_db.get_sample_data.return_value = self.sample_narrow_data
        
        # Analyze schema
        schema = schema_analyzer.analyze_ship_data(self.sample_ship_id)
        
        # Validate schema
        issues = schema_analyzer.validate_schema(schema)
        
        assert len(issues) == 0  # Should be valid
        
        # Test with invalid schema
        invalid_schema = {
            'table_name': 'invalid_table_name',
            'columns': [
                {'name': 'created_time', 'type': 'text', 'nullable': False}
            ]
        }
        
        issues = schema_analyzer.validate_schema(invalid_schema)
        
        assert len(issues) > 0
        assert any('Invalid table name' in issue for issue in issues)
    
    @patch('data_migrator.db_manager')
    @patch('data_migrator.table_generator')
    def test_data_consistency_flow(self, mock_table_gen, mock_db):
        """Test data consistency validation flow"""
        # Mock successful migration
        mock_db.get_migration_count.return_value = 2
        mock_db.get_data_batches.return_value = [self.sample_narrow_data]
        mock_db.execute_batch.return_value = 1
        mock_db.execute_query.return_value = [{'count': 1}]
        
        mock_table_gen.get_table_columns.return_value = ['created_time', 'engine_rpm', 'fuel_level']
        
        # Mock validation queries
        mock_db.execute_query.side_effect = [
            [{'count': 1}],  # Count query
            [{'created_time': datetime.now() - timedelta(minutes=5), 'channel_count': 2}]  # Sample check
        ]
        
        # Execute migration
        result = data_migrator.migrate_ship_data(self.sample_ship_id)
        
        # Check validation results
        assert result['validation']['count_match'] is True
        assert result['validation']['integrity_check']['passed'] is True
        assert result['validation']['status'] == 'passed'
    
    def test_value_format_mapping_consistency(self):
        """Test value format mapping consistency across modules"""
        # Test that all modules use the same value format mapping
        from config import migration_config
        
        expected_mapping = {
            'Decimal': 'double_v',
            'Integer': 'long_v',
            'String': 'str_v',
            'Boolean': 'bool_v'
        }
        
        assert schema_analyzer.value_format_mapping == expected_mapping
        assert data_migrator.value_format_mapping == expected_mapping
        assert realtime_processor.value_format_mapping == expected_mapping
        assert migration_config.VALUE_FORMAT_MAPPING == expected_mapping
    
    @patch('realtime_processor.db_manager')
    @patch('realtime_processor.table_generator')
    def test_dynamic_schema_update_flow(self, mock_table_gen, mock_db):
        """Test dynamic schema update during real-time processing"""
        # Mock existing table with limited columns
        mock_db.check_table_exists.return_value = True
        mock_table_gen.get_table_columns.return_value = ['created_time', 'engine_rpm']
        mock_table_gen.add_column_to_table.return_value = True
        mock_db.execute_query.return_value = [
            {
                'ship_id': self.sample_ship_id,
                'data_channel_id': 'fuel_level',  # New channel not in existing table
                'created_time': datetime.now(),
                'bool_v': None,
                'str_v': None,
                'long_v': None,
                'double_v': 75.5,
                'value_format': 'Decimal'
            }
        ]
        mock_db.execute_batch.return_value = 1
        
        # Process data with new channel
        realtime_processor._process_ship_data(self.sample_ship_id)
        
        # Verify that new column was added
        mock_table_gen.add_column_to_table.assert_called_with(
            f'tbl_data_timeseries_{self.sample_ship_id}',
            'fuel_level',
            'text'
        )
