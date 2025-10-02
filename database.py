"""
Database connection and utility functions
"""
import psycopg2
import psycopg2.extras
from contextlib import contextmanager
from typing import List, Dict, Any, Optional, Generator
from loguru import logger
from config import db_config


class DatabaseManager:
    """Database connection and query management"""
    
    def __init__(self):
        self.connection_string = db_config.connection_string
        self._connection = None
    
    def connect(self) -> psycopg2.extensions.connection:
        """Establish database connection"""
        try:
            self._connection = psycopg2.connect(
                self.connection_string,
                cursor_factory=psycopg2.extras.RealDictCursor
            )
            logger.info("Database connection established")
            return self._connection
        except psycopg2.Error as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def disconnect(self):
        """Close database connection"""
        if self._connection:
            self._connection.close()
            logger.info("Database connection closed")
    
    @contextmanager
    def get_cursor(self):
        """Context manager for database cursor"""
        conn = None
        cursor = None
        try:
            conn = self.connect()
            cursor = conn.cursor()
            yield cursor
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database operation failed: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """Execute SELECT query and return results"""
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()
    
    def execute_update(self, query: str, params: Optional[tuple] = None) -> int:
        """Execute UPDATE/INSERT/DELETE query and return affected rows"""
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            return cursor.rowcount
    
    def execute_batch(self, query: str, data: List[tuple]) -> int:
        """Execute batch insert/update"""
        with self.get_cursor() as cursor:
            cursor.executemany(query, data)
            return cursor.rowcount
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get table information including columns and constraints"""
        query = """
        SELECT 
            column_name, 
            data_type, 
            is_nullable,
            column_default
        FROM information_schema.columns 
        WHERE table_name = %s AND table_schema = 'tenant'
        ORDER BY ordinal_position;
        """
        return self.execute_query(query, (table_name,))
    
    def check_table_exists(self, table_name: str) -> bool:
        """Check if table exists"""
        query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'tenant' 
            AND table_name = %s
        );
        """
        result = self.execute_query(query, (table_name,))
        return result[0]['exists'] if result else False
    
    def get_distinct_ship_ids(self) -> List[str]:
        """Get target ship IDs from configuration"""
        from config import migration_config
        return migration_config.target_ship_ids
    
    def get_sample_data(self, ship_id: str, minutes: int = 10) -> List[Dict[str, Any]]:
        """Get sample data for schema analysis"""
        query = """
        SELECT 
            ship_id,
            data_channel_id,
            created_time,
            bool_v,
            str_v,
            long_v,
            double_v,
            value_format
        FROM tenant.tbl_data_timeseries 
        WHERE ship_id = %s 
        AND created_time >= NOW() - INTERVAL '%s minutes'
        ORDER BY created_time DESC
        LIMIT 1000;
        """
        return self.execute_query(query, (ship_id, minutes))
    
    def get_data_channels_for_ship(self, ship_id: str) -> List[str]:
        """Get all data_channel_ids for a specific ship"""
        query = """
        SELECT DISTINCT data_channel_id 
        FROM tenant.tbl_data_timeseries 
        WHERE ship_id = %s
        ORDER BY data_channel_id;
        """
        result = self.execute_query(query, (ship_id,))
        return [row['data_channel_id'] for row in result]


# Global database manager instance
db_manager = DatabaseManager()

