"""
Unit tests for SchemaAnalyzer module
"""
import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

from schema_analyzer import SchemaAnalyzer


class TestSchemaAnalyzer:
    """Test cases for SchemaAnalyzer class"""
    
    def setup_method(self):
        """Setup test method"""
        self.analyzer = SchemaAnalyzer()
    
    def test_init(self):
        """Test SchemaAnalyzer initialization"""
        assert self.analyzer.value_format_mapping is not None
        assert 'Decimal' in self.analyzer.value_format_mapping
        assert 'Integer' in self.analyzer.value_format_mapping
        assert 'String' in self.analyzer.value_format_mapping
        assert 'Boolean' in self.analyzer.value_format_mapping
    
    def test_get_value_by_format_decimal(self):
        """Test getting value by format - Decimal"""
        row_data = {
            'double_v': 75.5,
            'long_v': 1500,
            'str_v': 'test',
            'bool_v': True
        }
        
        value = self.analyzer._get_sample_value(row_data, 'Decimal')
        assert value == 75.5
    
    def test_get_value_by_format_integer(self):
        """Test getting value by format - Integer"""
        row_data = {
            'double_v': 75.5,
            'long_v': 1500,
            'str_v': 'test',
            'bool_v': True
        }
        
        value = self.analyzer._get_sample_value(row_data, 'Integer')
        assert value == 1500
    
    def test_get_value_by_format_string(self):
        """Test getting value by format - String"""
        row_data = {
            'double_v': 75.5,
            'long_v': 1500,
            'str_v': 'test',
            'bool_v': True
        }
        
        value = self.analyzer._get_sample_value(row_data, 'String')
        assert value == 'test'
    
    def test_get_value_by_format_boolean(self):
        """Test getting value by format - Boolean"""
        row_data = {
            'double_v': 75.5,
            'long_v': 1500,
            'str_v': 'test',
            'bool_v': True
        }
        
        value = self.analyzer._get_sample_value(row_data, 'Boolean')
        assert value is True
    
    def test_get_value_by_format_invalid(self):
        """Test getting value by format - Invalid format"""
        row_data = {
            'double_v': 75.5,
            'long_v': 1500,
            'str_v': 'test',
            'bool_v': True
        }
        
        value = self.analyzer._get_sample_value(row_data, 'Invalid')
        assert value is None
    
    def test_determine_primary_format_decimal_priority(self):
        """Test determining primary format - Decimal has priority"""
        formats = ['Decimal', 'Integer', 'String', 'Boolean']
        primary = self.analyzer._determine_primary_format(formats)
        assert primary == 'Decimal'
    
    def test_determine_primary_format_integer_priority(self):
        """Test determining primary format - Integer has priority over String/Boolean"""
        formats = ['Integer', 'String', 'Boolean']
        primary = self.analyzer._determine_primary_format(formats)
        assert primary == 'Integer'
    
    def test_determine_primary_format_string_priority(self):
        """Test determining primary format - String has priority over Boolean"""
        formats = ['String', 'Boolean']
        primary = self.analyzer._determine_primary_format(formats)
        assert primary == 'String'
    
    def test_determine_primary_format_boolean_only(self):
        """Test determining primary format - Boolean only"""
        formats = ['Boolean']
        primary = self.analyzer._determine_primary_format(formats)
        assert primary == 'Boolean'
    
    def test_determine_primary_format_empty(self):
        """Test determining primary format - Empty list"""
        formats = []
        primary = self.analyzer._determine_primary_format(formats)
        assert primary == 'String'  # Default fallback
    
    def test_determine_primary_format_unknown(self):
        """Test determining primary format - Unknown format"""
        formats = ['UnknownFormat']
        primary = self.analyzer._determine_primary_format(formats)
        assert primary == 'UnknownFormat'  # Fallback to first format
    
    def test_analyze_data_channels(self, sample_narrow_data):
        """Test analyzing data channels from sample data"""
        channel_analysis = self.analyzer._analyze_data_channels(sample_narrow_data)
        
        # Check that all channels are analyzed
        assert 'engine_rpm' in channel_analysis
        assert 'fuel_level' in channel_analysis
        assert 'engine_status' in channel_analysis
        assert 'location' in channel_analysis
        
        # Check engine_rpm analysis
        engine_rpm = channel_analysis['engine_rpm']
        assert 'Integer' in engine_rpm['value_formats']
        assert engine_rpm['primary_format'] == 'Integer'
        assert len(engine_rpm['sample_values']) > 0
        
        # Check fuel_level analysis
        fuel_level = channel_analysis['fuel_level']
        assert 'Decimal' in fuel_level['value_formats']
        assert fuel_level['primary_format'] == 'Decimal'
        assert len(fuel_level['sample_values']) > 0
    
    def test_generate_column_definitions(self, sample_narrow_data):
        """Test generating column definitions"""
        channel_analysis = self.analyzer._analyze_data_channels(sample_narrow_data)
        columns = self.analyzer._generate_column_definitions(channel_analysis)
        
        # Check created_time column
        created_time_col = next(col for col in columns if col['name'] == 'created_time')
        assert created_time_col['type'] == 'timestamp'
        assert created_time_col['nullable'] is False
        
        # Check data channel columns
        engine_rpm_col = next(col for col in columns if col['name'] == 'engine_rpm')
        assert engine_rpm_col['type'] == 'text'
        assert engine_rpm_col['nullable'] is True
        assert engine_rpm_col['primary_format'] == 'Integer'
    
    def test_create_empty_schema(self):
        """Test creating empty schema when no sample data"""
        ship_id = 'IMO9976903'
        schema = self.analyzer._create_empty_schema(ship_id)
        
        assert schema['ship_id'] == ship_id
        assert schema['table_name'] == f'tbl_data_timeseries_{ship_id}'
        assert len(schema['columns']) == 1  # Only created_time
        assert schema['columns'][0]['name'] == 'created_time'
        assert schema['primary_key'] == 'created_time'
        assert schema['sample_count'] == 0
        assert len(schema['data_channels']) == 0
    
    @patch('schema_analyzer.db_manager')
    def test_analyze_ship_data_with_data(self, mock_db_manager, sample_narrow_data):
        """Test analyzing ship data with sample data"""
        mock_db_manager.get_sample_data.return_value = sample_narrow_data
        
        ship_id = 'IMO9976903'
        schema = self.analyzer.analyze_ship_data(ship_id, sample_minutes=5)
        
        assert schema['ship_id'] == ship_id
        assert schema['table_name'] == f'tbl_data_timeseries_{ship_id}'
        assert len(schema['columns']) > 1  # More than just created_time
        assert schema['sample_count'] == len(sample_narrow_data)
        assert len(schema['data_channels']) > 0
        
        mock_db_manager.get_sample_data.assert_called_once_with(ship_id, 5)
    
    @patch('schema_analyzer.db_manager')
    def test_analyze_ship_data_no_data(self, mock_db_manager):
        """Test analyzing ship data with no sample data"""
        mock_db_manager.get_sample_data.return_value = []
        
        ship_id = 'IMO9976903'
        schema = self.analyzer.analyze_ship_data(ship_id, sample_minutes=5)
        
        assert schema['ship_id'] == ship_id
        assert schema['table_name'] == f'tbl_data_timeseries_{ship_id}'
        assert len(schema['columns']) == 1  # Only created_time
        assert schema['sample_count'] == 0
        assert len(schema['data_channels']) == 0
    
    @patch('schema_analyzer.db_manager')
    def test_analyze_all_ships(self, mock_db_manager, sample_narrow_data):
        """Test analyzing all ships"""
        mock_db_manager.get_distinct_ship_ids.return_value = ['IMO9976903', 'IMO9976915']
        mock_db_manager.get_sample_data.return_value = sample_narrow_data
        
        schemas = self.analyzer.analyze_all_ships(sample_minutes=5)
        
        assert len(schemas) == 2
        assert 'IMO9976903' in schemas
        assert 'IMO9976915' in schemas
        
        for ship_id, schema in schemas.items():
            assert schema['ship_id'] == ship_id
            assert schema['table_name'] == f'tbl_data_timeseries_{ship_id}'
    
    def test_validate_schema_valid(self, sample_wide_schema):
        """Test validating a valid schema"""
        issues = self.analyzer.validate_schema(sample_wide_schema)
        assert len(issues) == 0
    
    def test_validate_schema_invalid_table_name(self):
        """Test validating schema with invalid table name"""
        schema = {
            'table_name': 'invalid_table_name',
            'columns': [
                {'name': 'created_time', 'type': 'timestamp', 'nullable': False}
            ]
        }
        
        issues = self.analyzer.validate_schema(schema)
        assert len(issues) > 0
        assert any('Invalid table name' in issue for issue in issues)
    
    def test_validate_schema_missing_created_time(self):
        """Test validating schema missing created_time column"""
        schema = {
            'table_name': 'tbl_data_timeseries_IMO9976903',
            'columns': [
                {'name': 'other_column', 'type': 'text', 'nullable': True}
            ]
        }
        
        issues = self.analyzer.validate_schema(schema)
        assert len(issues) > 0
        assert any('Missing created_time column' in issue for issue in issues)
    
    def test_validate_schema_wrong_created_time_type(self):
        """Test validating schema with wrong created_time type"""
        schema = {
            'table_name': 'tbl_data_timeseries_IMO9976903',
            'columns': [
                {'name': 'created_time', 'type': 'text', 'nullable': False}
            ]
        }
        
        issues = self.analyzer.validate_schema(schema)
        assert len(issues) > 0
        assert any('created_time column must be timestamp type' in issue for issue in issues)
    
    def test_validate_schema_duplicate_columns(self):
        """Test validating schema with duplicate column names"""
        schema = {
            'table_name': 'tbl_data_timeseries_IMO9976903',
            'columns': [
                {'name': 'created_time', 'type': 'timestamp', 'nullable': False},
                {'name': 'created_time', 'type': 'timestamp', 'nullable': False}
            ]
        }
        
        issues = self.analyzer.validate_schema(schema)
        assert len(issues) > 0
        assert any('Duplicate column names found' in issue for issue in issues)
