"""
Unit tests for TableGenerator module
"""
import pytest
from unittest.mock import Mock, patch, MagicMock

from table_generator import TableGenerator


class TestTableGenerator:
    """Test cases for TableGenerator class"""
    
    def setup_method(self):
        """Setup test method"""
        self.generator = TableGenerator()
    
    def test_init(self):
        """Test TableGenerator initialization"""
        assert self.generator.schema_analyzer is not None
    
    def test_format_column_definition_not_null(self):
        """Test formatting column definition with NOT NULL"""
        column = {
            'name': 'created_time',
            'type': 'timestamp',
            'nullable': False
        }
        
        result = self.generator._format_column_definition(column)
        
        assert result == "    created_time timestamp NOT NULL"
    
    def test_format_column_definition_nullable(self):
        """Test formatting column definition with nullable"""
        column = {
            'name': 'engine_rpm',
            'type': 'text',
            'nullable': True
        }
        
        result = self.generator._format_column_definition(column)
        
        assert result == "    engine_rpm text"
    
    def test_generate_create_table_sql(self, sample_wide_schema):
        """Test generating CREATE TABLE SQL"""
        sql = self.generator._generate_create_table_sql(sample_wide_schema)
        
        assert "CREATE TABLE tenant.tbl_data_timeseries_IMO9976903" in sql
        assert "created_time timestamp NOT NULL" in sql
        assert "engine_rpm text" in sql
        assert "fuel_level text" in sql
        assert "engine_status text" in sql
        assert "location text" in sql
        assert "CONSTRAINT tbl_data_timeseries_IMO9976903_pk PRIMARY KEY (created_time)" in sql
        assert sql.endswith(");")
    
    @patch('table_generator.db_manager')
    def test_create_indexes(self, mock_db_manager, sample_wide_schema):
        """Test creating indexes"""
        mock_db_manager.execute_update.return_value = 1
        
        self.generator._create_indexes(sample_wide_schema)
        
        # Should create index for created_time
        mock_db_manager.execute_update.assert_called_once()
        call_args = mock_db_manager.execute_update.call_args[0][0]
        assert "CREATE INDEX" in call_args
        assert "created_time" in call_args
        assert "DESC" in call_args
    
    @patch('table_generator.db_manager')
    def test_create_indexes_exception(self, mock_db_manager, sample_wide_schema):
        """Test creating indexes with exception"""
        mock_db_manager.execute_update.side_effect = Exception("Index creation failed")
        
        # Should not raise exception, just log error
        self.generator._create_indexes(sample_wide_schema)
        
        mock_db_manager.execute_update.assert_called_once()
    
    @patch('table_generator.db_manager')
    def test_drop_table(self, mock_db_manager):
        """Test dropping table"""
        mock_db_manager.execute_update.return_value = 1
        
        self.generator._drop_table('test_table')
        
        mock_db_manager.execute_update.assert_called_once_with(
            "DROP TABLE IF EXISTS tenant.test_table CASCADE;"
        )
    
    @patch('table_generator.db_manager')
    def test_generate_table_success(self, mock_db_manager, sample_wide_schema):
        """Test successful table generation"""
        mock_db_manager.check_table_exists.return_value = False
        mock_db_manager.execute_update.return_value = 1
        
        result = self.generator.generate_table(sample_wide_schema)
        
        assert result is True
        mock_db_manager.check_table_exists.assert_called_once()
        mock_db_manager.execute_update.assert_called()
    
    @patch('table_generator.db_manager')
    def test_generate_table_already_exists_no_drop(self, mock_db_manager, sample_wide_schema):
        """Test table generation when table already exists and drop_if_exists=False"""
        mock_db_manager.check_table_exists.return_value = True
        
        result = self.generator.generate_table(sample_wide_schema, drop_if_exists=False)
        
        assert result is True
        mock_db_manager.check_table_exists.assert_called_once()
        # Should not call execute_update for table creation
        mock_db_manager.execute_update.assert_not_called()
    
    @patch('table_generator.db_manager')
    def test_generate_table_already_exists_with_drop(self, mock_db_manager, sample_wide_schema):
        """Test table generation when table already exists and drop_if_exists=True"""
        mock_db_manager.check_table_exists.return_value = True
        mock_db_manager.execute_update.return_value = 1
        
        result = self.generator.generate_table(sample_wide_schema, drop_if_exists=True)
        
        assert result is True
        mock_db_manager.check_table_exists.assert_called_once()
        # Should call execute_update for both drop and create
        assert mock_db_manager.execute_update.call_count >= 2
    
    @patch('table_generator.db_manager')
    def test_generate_table_exception(self, mock_db_manager, sample_wide_schema):
        """Test table generation with exception"""
        mock_db_manager.check_table_exists.return_value = False
        mock_db_manager.execute_update.side_effect = Exception("Table creation failed")
        
        result = self.generator.generate_table(sample_wide_schema)
        
        assert result is False
    
    @patch('table_generator.db_manager')
    def test_generate_all_tables(self, mock_db_manager, sample_wide_schema):
        """Test generating all tables"""
        schemas = {
            'IMO9976903': sample_wide_schema,
            'IMO9976915': {**sample_wide_schema, 'ship_id': 'IMO9976915', 'table_name': 'tbl_data_timeseries_IMO9976915'}
        }
        
        mock_db_manager.check_table_exists.return_value = False
        mock_db_manager.execute_update.return_value = 1
        
        results = self.generator.generate_all_tables(schemas)
        
        assert len(results) == 2
        assert results['IMO9976903'] is True
        assert results['IMO9976915'] is True
    
    @patch('table_generator.db_manager')
    def test_generate_all_tables_partial_failure(self, mock_db_manager, sample_wide_schema):
        """Test generating all tables with partial failure"""
        schemas = {
            'IMO9976903': sample_wide_schema,
            'IMO9976915': {**sample_wide_schema, 'ship_id': 'IMO9976915', 'table_name': 'tbl_data_timeseries_IMO9976915'}
        }
        
        def side_effect(table_name):
            if 'IMO9976903' in table_name:
                return False
            return True
        
        mock_db_manager.check_table_exists.side_effect = side_effect
        mock_db_manager.execute_update.return_value = 1
        
        results = self.generator.generate_all_tables(schemas)
        
        assert len(results) == 2
        assert results['IMO9976903'] is True
        assert results['IMO9976915'] is True
    
    @patch('table_generator.db_manager')
    def test_add_column_to_table_success(self, mock_db_manager):
        """Test successfully adding column to table"""
        mock_db_manager.execute_update.return_value = 1
        
        result = self.generator.add_column_to_table('test_table', 'new_column', 'text')
        
        assert result is True
        mock_db_manager.execute_update.assert_called_once_with(
            "ALTER TABLE tenant.test_table ADD COLUMN new_column text;"
        )
    
    @patch('table_generator.db_manager')
    def test_add_column_to_table_failure(self, mock_db_manager):
        """Test adding column to table with failure"""
        mock_db_manager.execute_update.side_effect = Exception("Column addition failed")
        
        result = self.generator.add_column_to_table('test_table', 'new_column', 'text')
        
        assert result is False
    
    @patch('table_generator.db_manager')
    def test_get_table_columns(self, mock_db_manager):
        """Test getting table columns"""
        mock_db_manager.get_table_info.return_value = [
            {'column_name': 'created_time'},
            {'column_name': 'engine_rpm'},
            {'column_name': 'fuel_level'}
        ]
        
        result = self.generator.get_table_columns('test_table')
        
        assert result == ['created_time', 'engine_rpm', 'fuel_level']
        mock_db_manager.get_table_info.assert_called_once_with('test_table')
    
    @patch('table_generator.db_manager')
    def test_validate_table_structure_valid(self, mock_db_manager, sample_wide_schema):
        """Test validating valid table structure"""
        mock_db_manager.get_table_info.return_value = [
            {'column_name': 'created_time'},
            {'column_name': 'engine_rpm'},
            {'column_name': 'fuel_level'},
            {'column_name': 'engine_status'},
            {'column_name': 'location'}
        ]
        
        issues = self.generator.validate_table_structure('test_table', sample_wide_schema)
        
        assert len(issues) == 0
    
    @patch('table_generator.db_manager')
    def test_validate_table_structure_missing_columns(self, mock_db_manager, sample_wide_schema):
        """Test validating table structure with missing columns"""
        mock_db_manager.get_table_info.return_value = [
            {'column_name': 'created_time'},
            {'column_name': 'engine_rpm'}
        ]
        
        issues = self.generator.validate_table_structure('test_table', sample_wide_schema)
        
        assert len(issues) > 0
        assert any('Missing columns' in issue for issue in issues)
    
    @patch('table_generator.db_manager')
    def test_validate_table_structure_extra_columns(self, mock_db_manager, sample_wide_schema):
        """Test validating table structure with extra columns"""
        mock_db_manager.get_table_info.return_value = [
            {'column_name': 'created_time'},
            {'column_name': 'engine_rpm'},
            {'column_name': 'fuel_level'},
            {'column_name': 'engine_status'},
            {'column_name': 'location'},
            {'column_name': 'extra_column'}
        ]
        
        issues = self.generator.validate_table_structure('test_table', sample_wide_schema)
        
        assert len(issues) > 0
        assert any('Extra columns' in issue for issue in issues)
    
    @patch('table_generator.db_manager')
    def test_validate_table_structure_missing_created_time(self, mock_db_manager, sample_wide_schema):
        """Test validating table structure missing created_time"""
        mock_db_manager.get_table_info.return_value = [
            {'column_name': 'engine_rpm'},
            {'column_name': 'fuel_level'}
        ]
        
        issues = self.generator.validate_table_structure('test_table', sample_wide_schema)
        
        assert len(issues) > 0
        assert any('Missing created_time column' in issue for issue in issues)
    
    @patch('table_generator.db_manager')
    def test_validate_table_structure_exception(self, mock_db_manager, sample_wide_schema):
        """Test validating table structure with exception"""
        mock_db_manager.get_table_info.side_effect = Exception("Database error")
        
        issues = self.generator.validate_table_structure('test_table', sample_wide_schema)
        
        assert len(issues) > 0
        assert any('Failed to validate table structure' in issue for issue in issues)
