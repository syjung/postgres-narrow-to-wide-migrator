"""
Database connection and utility functions
"""
import psycopg2
import psycopg2.extras
import psycopg2.pool
import threading
from contextlib import contextmanager
from typing import List, Dict, Any, Optional, Generator
from loguru import logger
from config import db_config, migration_config


class DatabaseManager:
    """Database connection and query management with connection pooling"""
    
    def __init__(self):
        self.connection_string = db_config.connection_string
        
        # Dynamic connection pool configuration based on ship count
        self.pool_config = migration_config.get_optimal_pool_config()
        
        self._pool = None
        self._initialize_pool()
        
        # Log dynamic configuration
        ship_count = len(migration_config.target_ship_ids)
        thread_count = migration_config.get_optimal_thread_count()
        logger.info(f"🧠 Dynamic configuration applied:")
        logger.info(f"   📊 Ships: {ship_count}")
        logger.info(f"   📊 Threads: {thread_count}")
        logger.info(f"   📊 DB Pool: {self.pool_config['minconn']}-{self.pool_config['maxconn']}")
    
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
        """Get connection pool status with detailed monitoring"""
        if not self._pool:
            return {'status': 'not_initialized'}
        
        try:
            return {
                'status': 'active',
                'min_connections': self.pool_config['minconn'],
                'max_connections': self.pool_config['maxconn'],
                'current_connections': len(self._pool._used) + len(self._pool._pool),
                'used_connections': len(self._pool._used),
                'available_connections': len(self._pool._pool),
                'pool_closed': self._pool.closed,
                'utilization_percent': round((len(self._pool._used) / self.pool_config['maxconn']) * 100, 2)
            }
        except Exception as e:
            logger.warning(f"Failed to get detailed pool status: {e}")
            return {
                'status': 'active',
                'min_connections': self.pool_config['minconn'],
                'max_connections': self.pool_config['maxconn'],
                'pool_closed': self._pool.closed
            }
    
    @contextmanager
    def get_cursor(self):
        """Context manager for database cursor using connection pool (thread-isolated)"""
        conn = None
        cursor = None
        thread_id = threading.current_thread().ident
        try:
            conn = self.get_connection()
            # Set connection to autocommit mode for better thread safety
            conn.autocommit = True
            # Ensure each thread gets a fresh cursor
            cursor = conn.cursor()
            logger.debug(f"🔗 Thread {thread_id}: Fresh cursor created")
            yield cursor
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database operation failed in thread {thread_id}: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
                logger.debug(f"🔗 Thread {thread_id}: Cursor closed")
            if conn:
                self.return_connection(conn)
                logger.debug(f"🔗 Thread {thread_id}: Connection returned")
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """Execute SELECT query and return results (thread-safe with default cursor)"""
        import time
        
        # Log query start for large table queries (simplified)
        if 'tbl_data_timeseries' in query:
            start_time = time.time()
        
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            
            # Fetch all results and convert to dictionary format
            rows = cursor.fetchall()
            result = []
            
            # Check for metadata corruption (column names as values)
            if rows and len(rows) > 0:
                first_row = rows[0]
                if hasattr(first_row, '__iter__') and not isinstance(first_row, (str, bytes)):
                    # Check if the first row contains column names as values
                    if len(first_row) == len(columns):
                        is_metadata = True
                        for i, value in enumerate(first_row):
                            if i < len(columns) and str(value) != columns[i]:
                                is_metadata = False
                                break
                        
                        if is_metadata:
                            logger.error(f"🔍 ERROR: Getting column metadata instead of actual data!")
                            logger.error(f"🔍 This usually means the query didn't execute properly")
                            logger.error(f"🔍 Query: {query}")
                            logger.error(f"🔍 Params: {params}")
                            raise Exception("Database query returned metadata instead of actual data")
            
            # Convert rows to dictionaries
            for row in rows:
                row_dict = {}
                for i, value in enumerate(row):
                    if i < len(columns):
                        row_dict[columns[i]] = value
                result.append(row_dict)
            
            # Log query completion for large table queries (simplified)
            if 'tbl_data_timeseries' in query:
                end_time = time.time()
                execution_time = end_time - start_time
                if execution_time > 5.0:
                    logger.warning(f"⚠️ Slow query detected: {execution_time:.2f}s execution time")
                logger.info(f"✅ Large table query completed: {len(result)} rows in {execution_time:.2f}s")
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
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            # ❗ executemany를 위해 autocommit을 False로 설정 (명시적 트랜잭션)
            conn.autocommit = False
            cursor = conn.cursor()
            
            cursor.executemany(query, data)
            affected_rows = cursor.rowcount
            
            # ✅ 명시적 커밋
            conn.commit()
            logger.debug(f"📦 Batch executed and committed: {affected_rows} rows affected")
            return affected_rows
            
        except Exception as e:
            if conn and not conn.autocommit:
                conn.rollback()
                logger.error(f"❌ Batch execution failed, rolled back: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.return_connection(conn)
    
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
        """Check if table exists using simple approach"""
        # Convert table name to lowercase for PostgreSQL compatibility
        table_name_lower = table_name.lower()
        
        # Use the simplest possible approach - try to describe the table
        try:
            # Get connection directly to avoid error logging in get_cursor
            conn = self.get_connection()
            try:
                conn.autocommit = True
                cursor = conn.cursor()
                try:
                    cursor.execute("SELECT 1 FROM tenant.{} LIMIT 0".format(table_name_lower))
                    logger.debug(f"🔍 Table check: {table_name} (lowercase: {table_name_lower}) exists=True")
                    return True
                finally:
                    cursor.close()
            finally:
                self.return_connection(conn)
        except Exception as e:
            logger.debug(f"🔍 Table check: {table_name} (lowercase: {table_name_lower}) exists=False - {e}")
            return False
    
    def get_distinct_ship_ids(self) -> List[str]:
        """Get target ship IDs from configuration"""
        from config import migration_config
        return migration_config.target_ship_ids
    
    def get_wide_table_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all wide tables"""
        stats = {}
        ship_ids = self.get_distinct_ship_ids()
        
        for ship_id in ship_ids:
            table_name = f'tbl_data_timeseries_{ship_id.upper()}'
            table_name_lower = table_name.lower()
            
            try:
                # Check if table exists
                if not self.check_table_exists(table_name):
                    stats[ship_id] = {
                        'exists': False,
                        'record_count': 0,
                        'latest_time': None,
                        'earliest_time': None
                    }
                    continue
                
                # Get record count and time range
                query = f"""
                SELECT 
                    COUNT(*) as record_count,
                    MIN(created_time) as earliest_time,
                    MAX(created_time) as latest_time
                FROM tenant.{table_name_lower}
                """
                
                result = self.execute_query(query)
                if result:
                    row = result[0]
                    stats[ship_id] = {
                        'exists': True,
                        'record_count': row['record_count'],
                        'latest_time': row['latest_time'],
                        'earliest_time': row['earliest_time']
                    }
                else:
                    stats[ship_id] = {
                        'exists': True,
                        'record_count': 0,
                        'latest_time': None,
                        'earliest_time': None
                    }
                    
            except Exception as e:
                logger.debug(f"Error getting stats for {ship_id}: {e}")
                stats[ship_id] = {
                    'exists': False,
                    'record_count': 0,
                    'latest_time': None,
                    'earliest_time': None,
                    'error': str(e)
                }
        
        return stats
    
    def parse_status_logs(self, log_file_path: str) -> Dict[str, Dict[str, Any]]:
        """Parse status logs to extract work information"""
        import re
        from datetime import datetime
        
        work_stats = {}
        
        try:
            with open(log_file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            # Parse STATUS logs
            status_pattern = r'STATUS:(REALTIME|BATCH):([^:]+):(\d+):(\d+):([^:]+):(\d+)'
            
            for line in lines:
                match = re.search(status_pattern, line)
                if match:
                    process_type = match.group(1)  # REALTIME or BATCH
                    ship_id = match.group(2)
                    records = int(match.group(3))
                    columns = int(match.group(4))
                    time_range = match.group(5)
                    affected_rows = int(match.group(6))
                    
                    if ship_id not in work_stats:
                        work_stats[ship_id] = {
                            'realtime': {'total_records': 0, 'total_columns': 0, 'last_time_range': None, 'operations': 0},
                            'batch': {'total_records': 0, 'total_columns': 0, 'last_time_range': None, 'operations': 0}
                        }
                    
                    work_stats[ship_id][process_type.lower()]['total_records'] += records
                    work_stats[ship_id][process_type.lower()]['total_columns'] = columns  # Latest column count
                    work_stats[ship_id][process_type.lower()]['last_time_range'] = time_range
                    work_stats[ship_id][process_type.lower()]['operations'] += 1
            
            return work_stats
            
        except Exception as e:
            logger.debug(f"Error parsing status logs: {e}")
            return {}
    
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
        AND created_time >= NOW() - INTERVAL %s
        AND data_channel_id IN ({placeholders})
        ORDER BY created_time DESC
        LIMIT 1000;
        """
        
        # Prepare parameters: ship_id, minutes, then all allowed columns
        params = [ship_id, f"{minutes} minutes"] + list(allowed_columns)
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
    """Log detailed connection pool status"""
    status = get_connection_pool_status()
    if status.get('status') == 'active':
        logger.info(f"🔗 Connection Pool Status:")
        logger.info(f"   📊 Utilization: {status.get('utilization_percent', 0)}%")
        logger.info(f"   📊 Used/Available: {status.get('used_connections', 0)}/{status.get('available_connections', 0)}")
        logger.info(f"   📊 Total: {status.get('current_connections', 0)}/{status.get('max_connections', 0)}")
    else:
        logger.info(f"🔗 Connection Pool Status: {status}")


def optimize_pool_for_migration():
    """Optimize connection pool settings for migration workload"""
    # Increase pool size for migration workload
    if db_manager._pool:
        logger.info("🚀 Optimizing connection pool for migration workload")
        # Note: psycopg2.pool doesn't support dynamic resizing
        # This would require recreating the pool with new settings
        logger.info("📊 Current pool config: min=2, max=10 connections")

