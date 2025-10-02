"""
Configuration module for PostgreSQL Narrow-to-Wide Table Migration
"""
import os
from typing import Optional, List
from pydantic_settings import BaseSettings
from typing import ClassVar


class DatabaseConfig(BaseSettings):
    """Database connection configuration"""
    host: str = "222.99.122.73"
    port: int = 25432
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
    target_ship_ids: List[str] = ["IMO9999993", "IMO9999994"]
    
    # Performance optimization settings
    parallel_workers: int = 4
    postgresql_optimization: bool = True
    dual_write_mode: bool = False
    rollback_enabled: bool = True
    
    # Cutoff time persistence
    cutoff_time_file: str = "migration_cutoff_time.txt"
    
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
    max_file_size: str = "10MB"
    backup_count: int = 5


# Global configuration instances
db_config = DatabaseConfig()
migration_config = MigrationConfig()
logging_config = LoggingConfig()

