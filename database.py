"""
Database connection and utility functions
"""
import psycopg2
import psycopg2.extras
import psycopg2.pool
from contextlib import contextmanager
from typing import List, Dict, Any, Optional, Generator
from loguru import logger
from config import db_config


class DatabaseManager:
    """Database connection and query management with connection pooling"""
    
    def __init__(self):
        self.connection_string = db_config.connection_string
        
        # Connection pool configuration
        self.pool_config = {
            'minconn': 2,      # Minimum connections in pool
            'maxconn': 10,     # Maximum connections in pool
            'cursor_factory': psycopg2.extras.RealDictCursor
        }
        
        self._pool = None
        self._initialize_pool()
    
    def _initialize_pool(self):
        """Initialize connection pool"""
        try:
            self._pool = psycopg2.pool.ThreadedConnectionPool(
                self.pool_config['minconn'],
                self.pool_config['maxconn'],
                self.connection_string,
                cursor_factory=self.pool_config['cursor_factory']
            )
            logger.info(f"✅ Database connection pool initialized: {self.pool_config['minconn']}-{self.pool_config['maxconn']} connections")
        except psycopg2.Error as e:
            logger.error(f"❌ Failed to initialize connection pool: {e}")
            raise
    
    def get_connection(self):
        """Get connection from pool"""
        try:
            if not self._pool:
                self._initialize_pool()
            connection = self._pool.getconn()
            logger.debug("🔗 Connection acquired from pool")
            return connection
        except psycopg2.Error as e:
            logger.error(f"❌ Failed to get connection from pool: {e}")
            raise
    
    def return_connection(self, connection):
        """Return connection to pool"""
        try:
            if connection and self._pool:
                self._pool.putconn(connection)
                logger.debug("🔄 Connection returned to pool")
        except psycopg2.Error as e:
            logger.error(f"❌ Failed to return connection to pool: {e}")
    
    def close_pool(self):
        """Close all connections in pool"""
        try:
            if self._pool:
                self._pool.closeall()
                logger.info("🔒 Connection pool closed")
        except psycopg2.Error as e:
            logger.error(f"❌ Failed to close connection pool: {e}")
    
    def get_pool_status(self):
        """Get connection pool status"""
        if not self._pool:
            return {'status': 'not_initialized'}
        
        return {
            'status': 'active',
            'min_connections': self.pool_config['minconn'],
            'max_connections': self.pool_config['maxconn'],
            'pool_closed': self._pool.closed
        }
    
    @contextmanager
    def get_cursor(self):
        """Context manager for database cursor using connection pool"""
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
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
                self.return_connection(conn)
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """Execute SELECT query and return results (optimized with connection pool)"""
        import time
        
        # Log query start for large table queries
        if 'tbl_data_timeseries' in query:
            logger.info(f"🚀 Starting large table query execution...")
            start_time = time.time()
        
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            result = cursor.fetchall()
            
            # Log query completion for large table queries
            if 'tbl_data_timeseries' in query:
                end_time = time.time()
                execution_time = end_time - start_time
                logger.info(f"✅ Large table query completed: {len(result)} rows in {execution_time:.2f}s")
                
                if execution_time > 5.0:
                    logger.warning(f"⚠️ Slow query detected: {execution_time:.2f}s execution time")
            else:
                logger.debug(f"📊 Query executed: {len(result)} rows returned")
            
            return result
    
    def execute_update(self, query: str, params: Optional[tuple] = None) -> int:
        """Execute UPDATE/INSERT/DELETE query and return affected rows (optimized with connection pool)"""
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            affected_rows = cursor.rowcount
            logger.debug(f"📝 Update executed: {affected_rows} rows affected")
            return affected_rows
    
    def execute_batch(self, query: str, data: List[tuple]) -> int:
        """Execute batch insert/update (optimized with connection pool)"""
        with self.get_cursor() as cursor:
            cursor.executemany(query, data)
            affected_rows = cursor.rowcount
            logger.debug(f"📦 Batch executed: {affected_rows} rows affected")
            return affected_rows
    
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
        """Check if table exists (optimized with connection pool)"""
        query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'tenant' 
            AND table_name = %s
        );
        """
        result = self.execute_query(query, (table_name,))
        exists = result[0]['exists'] if result else False
        logger.debug(f"🔍 Table check: {table_name} exists={exists}")
        return exists
    
    def get_distinct_ship_ids(self) -> List[str]:
        """Get target ship IDs from configuration"""
        from config import migration_config
        return migration_config.target_ship_ids
    
    def get_sample_data(self, ship_id: str, minutes: int = 10) -> List[Dict[str, Any]]:
        """Get sample data for schema analysis (only allowed columns)"""
        # Load allowed columns
        try:
            with open('column_list.txt', 'r', encoding='utf-8') as f:
                allowed_columns = {line.strip() for line in f if line.strip()}
        except FileNotFoundError:
            logger.warning("⚠️ column_list.txt not found, returning empty sample data")
            return []
        except Exception as e:
            logger.error(f"❌ Failed to load column_list.txt: {e}")
            return []
        
        if not allowed_columns:
            logger.warning("⚠️ No allowed columns found, returning empty sample data")
            return []
        
        # Create placeholders for IN clause
        placeholders = ','.join(['%s'] * len(allowed_columns))
        
        query = f"""
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
        AND data_channel_id IN ({placeholders})
        ORDER BY created_time DESC
        LIMIT 1000;
        """
        
        # Prepare parameters: ship_id, minutes, then all allowed columns
        params = [ship_id, minutes] + list(allowed_columns)
        return self.execute_query(query, tuple(params))
    
    def get_data_channels_for_ship(self, ship_id: str) -> List[str]:
        """Get data channels for a ship (simplified - returns empty list)"""
        # This method is not used in actual migration logic
        # Return empty list to avoid slow queries
        logger.info(f"get_data_channels_for_ship called for {ship_id} - returning empty list (not used in migration)")
        return []


# Global database manager instance
db_manager = DatabaseManager()


def get_connection_pool_status():
    """Get connection pool status for monitoring"""
    return db_manager.get_pool_status()


def close_connection_pool():
    """Close connection pool (for cleanup)"""
    db_manager.close_pool()


# Connection pool monitoring utilities
def log_pool_status():
    """Log current connection pool status"""
    status = get_connection_pool_status()
    logger.info(f"🔗 Connection Pool Status: {status}")


def optimize_pool_for_migration():
    """Optimize connection pool settings for migration workload"""
    # Increase pool size for migration workload
    if db_manager._pool:
        logger.info("🚀 Optimizing connection pool for migration workload")
        # Note: psycopg2.pool doesn't support dynamic resizing
        # This would require recreating the pool with new settings
        logger.info("📊 Current pool config: min=2, max=10 connections")

