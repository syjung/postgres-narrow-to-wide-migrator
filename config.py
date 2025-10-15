"""
Configuration module for PostgreSQL Narrow-to-Wide Table Migration
"""
import os
from typing import Optional, List
from pydantic_settings import BaseSettings
from typing import ClassVar


class DatabaseConfig(BaseSettings):
    """Database connection configuration"""
    # 시흥육상서버
    # host: str = "222.99.122.73"
    # port: int = 25432
    # database: str = "tenant_builder"
    # user: str = "tapp"
    # password: str = "tapp.123"

    """Database connection configuration"""
    # AZURE서버
    host: str = "20.249.68.82"
    port: int = 5432
    database: str = "tenant_builder"
    user: str = "tapp"
    password: str = "tapp.123"

    class Config:
        env_prefix = "DB_"
        case_sensitive = False
    
    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


class MigrationConfig(BaseSettings):
    """Migration process configuration"""
    batch_size: int = 50000  # Increased from 10,000 for better performance
    sample_minutes: int = 60  # Increased from 10 for better schema analysis
    migration_timeout: int = 3600
    chunk_size: int = 1000
    
    # Target ships configuration
    # 시흥육상서버
    #target_ship_ids: List[str] = ["IMO9999993", "IMO9999994"]

    # AZURE서버
    target_ship_ids: List[str] = ["IMO9976903", "IMO9976915","IMO9976927","IMO9976939","IMO9986051","IMO9986063","IMO9986087","IMO9986104"]
    
    # Multi-Table configuration
    use_multi_table: bool = True  # Enable multi-table mode
    channel_files: ClassVar[dict] = {
        '1': 'column_list_auxiliary_systems.txt',
        '2': 'column_list_engine_generator.txt',
        '3': 'column_list_navigation_ship.txt'
    }
    table_name_patterns: ClassVar[dict] = {
        '1': 'tbl_data_timeseries_{ship_id}_1',
        '2': 'tbl_data_timeseries_{ship_id}_2',
        '3': 'tbl_data_timeseries_{ship_id}_3'
    }
    
    # Performance optimization settings
    max_parallel_workers: int = 16  # Maximum thread limit for scalability
    parallel_workers: int = 8  # Default thread count (will be calculated dynamically)
    postgresql_optimization: bool = True
    dual_write_mode: bool = False
    rollback_enabled: bool = True
    
    def get_optimal_thread_count(self) -> int:
        """Calculate optimal thread count based on ship count and system limits"""
        if not self.target_ship_ids:
            return 0
            
        ship_count = len(self.target_ship_ids)
        
        # Intelligent thread calculation strategy
        if ship_count <= 4:
            # Small fleet: 1:1 mapping for maximum parallelism
            return ship_count
        elif ship_count <= 8:
            # Medium fleet: 1:1 mapping up to 8 ships
            return ship_count
        elif ship_count <= 12:
            # Large fleet: 75% ratio to avoid resource contention
            return max(8, int(ship_count * 0.75))
        else:
            # Very large fleet: Cap at max_parallel_workers
            return min(ship_count, self.max_parallel_workers)
    
    def get_optimal_pool_config(self) -> dict:
        """Calculate optimal connection pool configuration"""
        thread_count = self.get_optimal_thread_count()
        
        if thread_count == 0:
            return {
                'minconn': 1,  # Minimum 1 connection
                'maxconn': 2,  # Minimum 2 connections
                'cursor_factory': None
            }
        
        # Multi-table mode: 각 thread가 3개 테이블에 INSERT하므로 여유 필요
        if self.use_multi_table:
            # 3개 테이블 동시 처리를 위한 여유
            multiplier = 3
            return {
                'minconn': thread_count,  # 1:1 with thread count
                'maxconn': thread_count * multiplier,  # 3x for multi-table operations
                'cursor_factory': None
            }
        else:
            return {
                'minconn': thread_count,  # 1:1 with thread count
                'maxconn': thread_count * 2,  # 2x for burst capacity
                'cursor_factory': None
            }
    
    def get_optimal_postgresql_settings(self) -> dict:
        """Calculate optimal PostgreSQL settings based on thread count"""
        thread_count = self.get_optimal_thread_count()
        
        # Dynamic PostgreSQL settings based on thread count
        if thread_count <= 4:
            # Small fleet: Conservative settings
            return {
                'work_mem': '32MB',
                'maintenance_work_mem': '128MB',
                'max_parallel_workers_per_gather': 2,
                'max_parallel_workers': thread_count,
                'max_parallel_maintenance_workers': 2
            }
        elif thread_count <= 8:
            # Medium fleet: Balanced settings
            return {
                'work_mem': '64MB',
                'maintenance_work_mem': '256MB',
                'max_parallel_workers_per_gather': 4,
                'max_parallel_workers': thread_count,
                'max_parallel_maintenance_workers': 4
            }
        else:
            # Large fleet: High-performance settings
            return {
                'work_mem': '128MB',
                'maintenance_work_mem': '512MB',
                'max_parallel_workers_per_gather': 8,
                'max_parallel_workers': min(thread_count, 16),
                'max_parallel_maintenance_workers': 8
            }
    
    # Cutoff time persistence
    cutoff_time_file: str = "migration_cutoff_time.txt"
    
    # Chunked migration settings
    chunk_size_hours: int = 2  # Default chunk size (can be reduced for high-volume periods)
    max_records_per_chunk: int = 1000000  # Threshold for chunk size reduction
    adaptive_chunking: bool = True  # Enable dynamic chunk size adjustment
    batch_lookback_days: int = 365  # How many days back to process in batch migration (1 year default)
    
    # Value format mapping
    VALUE_FORMAT_MAPPING: ClassVar[dict] = {
        'Decimal': 'double_v',
        'Integer': 'long_v', 
        'String': 'str_v',
        'Boolean': 'bool_v'
    }


# PostgreSQLConfig removed - using PGOPTIONS in run_migration.sh instead
# This avoids confusion and ensures consistent runtime parameter settings


class LoggingConfig(BaseSettings):
    """Logging configuration"""
    level: str = "INFO"
    log_file: str = "logs/migration.log"
    rotation: str = "1 day"  # Daily rotation
    retention: str = "5 days"  # Keep logs for 30 days


# Global configuration instances
db_config = DatabaseConfig()
migration_config = MigrationConfig()
logging_config = LoggingConfig()

