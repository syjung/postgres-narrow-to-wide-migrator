"""
Monitoring and logging utilities
"""
import os
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from loguru import logger
from database import db_manager
from config import logging_config


class MigrationMonitor:
    """Monitors migration progress and system health"""
    
    def __init__(self):
        self.log_file = logging_config.log_file
        self._ensure_log_directory()
        self._setup_logging()
    
    def _ensure_log_directory(self):
        """Ensure log directory exists"""
        log_dir = os.path.dirname(self.log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
    
    def _setup_logging(self):
        """Setup logging configuration"""
        logger.remove()  # Remove default handler
        
        # Add console handler
        logger.add(
            sink=lambda msg: print(msg, end=""),
            level=logging_config.level,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
        )
        
        # Add file handler
        logger.add(
            sink=self.log_file,
            level=logging_config.level,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
            rotation=logging_config.max_file_size,
            retention=logging_config.backup_count,
            compression="zip"
        )
    
    def log_migration_start(self, ship_id: str, total_records: int):
        """Log migration start"""
        logger.info(f"Migration started for ship_id: {ship_id}, total records: {total_records}")
    
    def log_migration_progress(self, ship_id: str, progress: float, current: int, total: int):
        """Log migration progress"""
        logger.info(f"Migration progress for {ship_id}: {progress:.1f}% ({current}/{total})")
    
    def log_migration_complete(self, ship_id: str, result: Dict[str, Any]):
        """Log migration completion"""
        logger.info(f"Migration completed for ship_id: {ship_id}, result: {result}")
    
    def log_error(self, error_type: str, message: str, details: Optional[Dict[str, Any]] = None):
        """Log error with details"""
        error_data = {
            'type': error_type,
            'message': message,
            'timestamp': datetime.now().isoformat(),
            'details': details or {}
        }
        logger.error(f"Error [{error_type}]: {message}")
        if details:
            logger.error(f"Error details: {json.dumps(details, indent=2)}")
    
    def get_system_health(self) -> Dict[str, Any]:
        """Get system health status"""
        try:
            # Database connection test
            db_status = self._test_database_connection()
            
            # Disk space check
            disk_status = self._check_disk_space()
            
            # Memory usage
            memory_status = self._check_memory_usage()
            
            return {
                'timestamp': datetime.now().isoformat(),
                'database': db_status,
                'disk': disk_status,
                'memory': memory_status,
                'overall_status': 'healthy' if all([
                    db_status['connected'],
                    disk_status['status'] == 'ok',
                    memory_status['status'] == 'ok'
                ]) else 'warning'
            }
            
        except Exception as e:
            logger.error(f"Failed to get system health: {e}")
            return {
                'timestamp': datetime.now().isoformat(),
                'overall_status': 'error',
                'error': str(e)
            }
    
    def _test_database_connection(self) -> Dict[str, Any]:
        """Test database connection"""
        try:
            result = db_manager.execute_query("SELECT 1 as test")
            return {
                'connected': True,
                'response_time': '< 1s',
                'status': 'ok'
            }
        except Exception as e:
            return {
                'connected': False,
                'error': str(e),
                'status': 'error'
            }
    
    def _check_disk_space(self) -> Dict[str, Any]:
        """Check disk space"""
        try:
            import shutil
            total, used, free = shutil.disk_usage("/")
            
            free_gb = free // (1024**3)
            total_gb = total // (1024**3)
            usage_percent = (used / total) * 100
            
            status = 'ok'
            if usage_percent > 90:
                status = 'critical'
            elif usage_percent > 80:
                status = 'warning'
            
            return {
                'status': status,
                'free_gb': free_gb,
                'total_gb': total_gb,
                'usage_percent': round(usage_percent, 2)
            }
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e)
            }
    
    def _check_memory_usage(self) -> Dict[str, Any]:
        """Check memory usage"""
        try:
            import psutil
            memory = psutil.virtual_memory()
            
            status = 'ok'
            if memory.percent > 90:
                status = 'critical'
            elif memory.percent > 80:
                status = 'warning'
            
            return {
                'status': status,
                'percent_used': memory.percent,
                'available_gb': round(memory.available / (1024**3), 2),
                'total_gb': round(memory.total / (1024**3), 2)
            }
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e)
            }
    
    def get_migration_stats(self) -> Dict[str, Any]:
        """Get migration statistics"""
        try:
            # Get table counts
            ship_ids = db_manager.get_distinct_ship_ids()
            table_stats = {}
            
            for ship_id in ship_ids:
                table_name = f'tbl_data_timeseries_{ship_id}'
                if db_manager.check_table_exists(table_name):
                    count_query = f"SELECT COUNT(*) as count FROM tenant.{table_name}"
                    result = db_manager.execute_query(count_query)
                    table_stats[ship_id] = {
                        'table_name': table_name,
                        'record_count': result[0]['count'] if result else 0,
                        'exists': True
                    }
                else:
                    table_stats[ship_id] = {
                        'table_name': table_name,
                        'record_count': 0,
                        'exists': False
                    }
            
            # Calculate totals
            total_tables = len(table_stats)
            existing_tables = sum(1 for stats in table_stats.values() if stats['exists'])
            total_records = sum(stats['record_count'] for stats in table_stats.values())
            
            return {
                'timestamp': datetime.now().isoformat(),
                'total_ships': len(ship_ids),
                'total_tables': total_tables,
                'existing_tables': existing_tables,
                'total_records': total_records,
                'table_stats': table_stats
            }
            
        except Exception as e:
            logger.error(f"Failed to get migration stats: {e}")
            return {
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            }
    
    def get_performance_metrics(self, time_window_hours: int = 24) -> Dict[str, Any]:
        """Get performance metrics for the specified time window"""
        try:
            cutoff_time = datetime.now() - timedelta(hours=time_window_hours)
            
            # Get data insertion rate
            insertion_query = """
            SELECT 
                DATE_TRUNC('hour', created_time) as hour,
                COUNT(*) as records_per_hour
            FROM tenant.tbl_data_timeseries 
            WHERE created_time >= %s
            GROUP BY DATE_TRUNC('hour', created_time)
            ORDER BY hour
            """
            
            insertion_data = db_manager.execute_query(insertion_query, (cutoff_time,))
            
            # Calculate metrics
            total_records = sum(row['records_per_hour'] for row in insertion_data)
            avg_per_hour = total_records / max(len(insertion_data), 1)
            
            return {
                'timestamp': datetime.now().isoformat(),
                'time_window_hours': time_window_hours,
                'total_records': total_records,
                'avg_records_per_hour': round(avg_per_hour, 2),
                'hourly_data': insertion_data
            }
            
        except Exception as e:
            logger.error(f"Failed to get performance metrics: {e}")
            return {
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            }
    
    def generate_report(self) -> str:
        """Generate a comprehensive migration report"""
        try:
            health = self.get_system_health()
            stats = self.get_migration_stats()
            performance = self.get_performance_metrics()
            
            report = f"""
# Migration Report
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## System Health
- Overall Status: {health['overall_status']}
- Database: {'Connected' if health['database']['connected'] else 'Disconnected'}
- Disk Usage: {health['disk'].get('usage_percent', 'N/A')}%
- Memory Usage: {health['memory'].get('percent_used', 'N/A')}%

## Migration Statistics
- Total Ships: {stats.get('total_ships', 0)}
- Tables Created: {stats.get('existing_tables', 0)}/{stats.get('total_tables', 0)}
- Total Records Migrated: {stats.get('total_records', 0)}

## Performance Metrics (Last 24 Hours)
- Total Records Processed: {performance.get('total_records', 0)}
- Average Records/Hour: {performance.get('avg_records_per_hour', 0)}

## Table Status
"""
            
            for ship_id, table_stat in stats.get('table_stats', {}).items():
                status = "✓" if table_stat['exists'] else "✗"
                report += f"- {ship_id}: {status} ({table_stat['record_count']} records)\n"
            
            return report
            
        except Exception as e:
            logger.error(f"Failed to generate report: {e}")
            return f"Error generating report: {e}"


# Global monitoring instance
monitor = MigrationMonitor()

