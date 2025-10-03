"""
Wide table generator module
"""
from typing import Dict, List, Any, Optional
from loguru import logger
from database import db_manager
from schema_analyzer import schema_analyzer


class TableGenerator:
    """Generates wide type tables based on analyzed schemas"""
    
    def __init__(self):
        self.schema_analyzer = schema_analyzer
    
    def generate_table(self, ship_id: str, schema: Dict[str, Any], drop_if_exists: bool = False) -> bool:
        """
        Generate a wide table based on schema
        
        Args:
            ship_id: Ship ID for the table
            schema: Schema information from SchemaAnalyzer
            drop_if_exists: Whether to drop table if it already exists
            
        Returns:
            True if successful, False otherwise
        """
        table_name = schema['table_name'].lower()  # Ensure lowercase for PostgreSQL compatibility
        
        logger.info(f"Generating table: {table_name} for ship_id: {ship_id}")
        
        try:
            # Check if table already exists
            if db_manager.check_table_exists(table_name):
                if drop_if_exists:
                    logger.info(f"Dropping existing table: {table_name}")
                    self._drop_table(table_name)
                else:
                    logger.warning(f"Table {table_name} already exists, skipping creation")
                    return True
            
            # Generate CREATE TABLE statement
            create_sql = self._generate_create_table_sql(schema)
            
            # Execute table creation
            db_manager.execute_update(create_sql)
            
            # Create indexes
            self._create_indexes(schema)
            
            logger.info(f"Successfully created table: {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create table {table_name}: {e}")
            return False
    
    def _generate_create_table_sql(self, schema: Dict[str, Any]) -> str:
        """Generate CREATE TABLE SQL statement"""
        table_name = schema['table_name']
        columns = schema['columns']
        
        # Start with table name
        sql_parts = [f"CREATE TABLE tenant.{table_name} ("]
        
        # Add columns
        column_definitions = []
        for col in columns:
            col_def = self._format_column_definition(col)
            column_definitions.append(col_def)
        
        sql_parts.append(",\n".join(column_definitions))
        
        # Add primary key constraint
        primary_key = schema.get('primary_key', 'created_time')
        sql_parts.append(f",\nCONSTRAINT {table_name}_pk PRIMARY KEY ({primary_key})")
        
        # Close table definition
        sql_parts.append(");")
        
        return "\n".join(sql_parts)
    
    def _format_column_definition(self, column: Dict[str, Any]) -> str:
        """Format individual column definition"""
        name = column['name']
        data_type = column['type']
        nullable = column.get('nullable', True)
        
        # Quote column name if it contains special characters
        if any(char in name for char in ['/', '-', ' ', '.', '(', ')', '[', ']', '{', '}', '@', '#', '$', '%', '^', '&', '*', '+', '=', '|', '\\', ':', ';', '"', "'", '<', '>', ',', '?', '!', '~', '`']):
            quoted_name = f'"{name}"'
        else:
            quoted_name = name
        
        # For data channel columns, use VARCHAR with limited length to reduce row size
        if data_type == 'text' and name != 'created_time':
            data_type = 'VARCHAR(255)'  # Limit to 255 characters to reduce row size
        
        # Format column definition
        col_def = f"    {quoted_name} {data_type}"
        
        if not nullable:
            col_def += " NOT NULL"
        
        return col_def
    
    def _create_indexes(self, schema: Dict[str, Any]) -> None:
        """Create indexes for the table"""
        table_name = schema['table_name']
        indexes = schema.get('indexes', ['created_time'])
        
        for index_col in indexes:
            index_name = f"{table_name}_{index_col}_idx"
            index_sql = f"""
            CREATE INDEX {index_name} 
            ON tenant.{table_name} 
            USING btree ({index_col} DESC);
            """
            
            try:
                db_manager.execute_update(index_sql)
                logger.info(f"Created index: {index_name}")
            except Exception as e:
                logger.error(f"Failed to create index {index_name}: {e}")
    
    def _drop_table(self, table_name: str) -> None:
        """Drop table if it exists"""
        drop_sql = f"DROP TABLE IF EXISTS tenant.{table_name} CASCADE;"
        db_manager.execute_update(drop_sql)
    
    def generate_all_tables(self, schemas: Dict[str, Dict[str, Any]], drop_if_exists: bool = False) -> Dict[str, bool]:
        """
        Generate all wide tables based on schemas
        
        Args:
            schemas: Dictionary of schemas from SchemaAnalyzer
            drop_if_exists: Whether to drop tables if they already exist
            
        Returns:
            Dictionary mapping ship_id to success status
        """
        logger.info(f"Generating {len(schemas)} wide tables")
        
        results = {}
        for ship_id, schema in schemas.items():
            try:
                success = self.generate_table(schema, drop_if_exists)
                results[ship_id] = success
            except Exception as e:
                logger.error(f"Failed to generate table for ship_id {ship_id}: {e}")
                results[ship_id] = False
        
        successful_count = sum(1 for success in results.values() if success)
        logger.info(f"Successfully generated {successful_count}/{len(schemas)} tables")
        
        return results
    
    def add_column_to_table(self, table_name: str, column_name: str, data_type: str = 'text') -> bool:
        """
        Add a new column to existing wide table
        
        Args:
            table_name: Name of the table
            column_name: Name of the new column
            data_type: Data type of the new column
            
        Returns:
            True if successful, False otherwise
        """
        try:
            alter_sql = f"ALTER TABLE tenant.{table_name} ADD COLUMN {column_name} {data_type};"
            db_manager.execute_update(alter_sql)
            logger.info(f"Added column {column_name} to table {table_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to add column {column_name} to table {table_name}: {e}")
            return False
    
    def get_table_columns(self, table_name: str) -> List[str]:
        """Get list of column names for a table"""
        table_info = db_manager.get_table_info(table_name)
        return [col['column_name'] for col in table_info]
    
    def validate_table_structure(self, table_name: str, expected_schema: Dict[str, Any]) -> List[str]:
        """
        Validate that table structure matches expected schema
        
        Args:
            table_name: Name of the table to validate
            expected_schema: Expected schema from SchemaAnalyzer
            
        Returns:
            List of validation issues
        """
        issues = []
        
        try:
            # Get actual table structure
            actual_columns = self.get_table_columns(table_name)
            expected_columns = [col['name'] for col in expected_schema['columns']]
            
            # Check for missing columns
            missing_columns = set(expected_columns) - set(actual_columns)
            if missing_columns:
                issues.append(f"Missing columns: {list(missing_columns)}")
            
            # Check for extra columns
            extra_columns = set(actual_columns) - set(expected_columns)
            if extra_columns:
                issues.append(f"Extra columns: {list(extra_columns)}")
            
            # Check primary key
            if 'created_time' not in actual_columns:
                issues.append("Missing created_time column")
            
        except Exception as e:
            issues.append(f"Failed to validate table structure: {e}")
        
        return issues


# Global table generator instance
table_generator = TableGenerator()

