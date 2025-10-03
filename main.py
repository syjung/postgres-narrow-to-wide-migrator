"""
Main execution script for PostgreSQL Narrow-to-Wide Table Migration
"""
import argparse
import sys
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from loguru import logger

from config import migration_config
from database import db_manager
from schema_analyzer import schema_analyzer
from table_generator import table_generator
from data_migrator import data_migrator
from realtime_processor import realtime_processor
from monitoring import monitor
from cutoff_time_manager import cutoff_time_manager
from concurrent_migration_strategy import concurrent_strategy, hybrid_strategy, streaming_strategy
from parallel_batch_migrator import parallel_batch_migrator


class MigrationManager:
    """Main migration manager class"""
    
    def __init__(self):
        self.db_manager = db_manager
        self.schema_analyzer = schema_analyzer
        self.table_generator = table_generator
        self.data_migrator = data_migrator
        self.realtime_processor = realtime_processor
        self.monitor = monitor
    
    def run_full_migration(self, cutoff_time: Optional[datetime] = None, 
                          drop_tables: bool = False) -> Dict[str, Any]:
        """
        Run complete migration process
        
        Args:
            cutoff_time: Migrate data before this time
            drop_tables: Whether to drop existing tables
            
        Returns:
            Migration results summary
        """
        logger.info("Starting full migration process")
        
        try:
            # Step 1: Analyze schemas
            logger.info("Step 1: Analyzing schemas...")
            schemas = self.schema_analyzer.analyze_all_ships(migration_config.sample_minutes)
            logger.info(f"Analyzed schemas for {len(schemas)} ships")
            
            # Step 2: Generate tables
            logger.info("Step 2: Generating wide tables...")
            table_results = self.table_generator.generate_all_tables(schemas, drop_tables)
            successful_tables = sum(1 for success in table_results.values() if success)
            logger.info(f"Generated {successful_tables}/{len(table_results)} tables")
            
            # Step 3: Migrate data
            logger.info("Step 3: Migrating data...")
            migration_results = self.data_migrator.migrate_all_ships(
                cutoff_time, 
                self._progress_callback
            )
            
            # Step 4: Generate summary
            summary = self._generate_migration_summary(schemas, table_results, migration_results)
            
            logger.info("Full migration process completed")
            return summary
            
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            return {
                'status': 'failed',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def run_schema_analysis_only(self) -> Dict[str, Any]:
        """Run schema analysis only"""
        logger.info("Running schema analysis only")
        
        try:
            schemas = self.schema_analyzer.analyze_all_ships(migration_config.sample_minutes)
            
            # Validate schemas
            validation_results = {}
            for ship_id, schema in schemas.items():
                issues = self.schema_analyzer.validate_schema(schema)
                validation_results[ship_id] = {
                    'valid': len(issues) == 0,
                    'issues': issues,
                    'column_count': len(schema['columns']),
                    'data_channels': len(schema['data_channels'])
                }
            
            return {
                'status': 'success',
                'schemas': schemas,
                'validation': validation_results,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Schema analysis failed: {e}")
            return {
                'status': 'failed',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def run_table_generation_only(self, drop_tables: bool = False) -> Dict[str, Any]:
        """Run table generation only"""
        logger.info("Running table generation only")
        
        try:
            # First analyze schemas
            schemas = self.schema_analyzer.analyze_all_ships(migration_config.sample_minutes)
            
            # Generate tables
            table_results = self.table_generator.generate_all_tables(schemas, drop_tables)
            
            return {
                'status': 'success',
                'table_results': table_results,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Table generation failed: {e}")
            return {
                'status': 'failed',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def run_data_migration_only(self, cutoff_time: Optional[datetime] = None) -> Dict[str, Any]:
        """Run data migration only"""
        logger.info("Running data migration only")
        
        try:
            migration_results = self.data_migrator.migrate_all_ships(
                cutoff_time, 
                self._progress_callback
            )
            
            return {
                'status': 'success',
                'migration_results': migration_results,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Data migration failed: {e}")
            return {
                'status': 'failed',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def start_realtime_processing(self, interval_minutes: int = 1):
        """Start real-time data processing"""
        logger.info(f"Starting real-time processing with {interval_minutes} minute intervals")
        
        try:
            self.realtime_processor.start_processing(interval_minutes)
        except KeyboardInterrupt:
            logger.info("Real-time processing stopped by user")
        except Exception as e:
            logger.error(f"Real-time processing failed: {e}")
    
    def start_dual_write_processing(self, interval_minutes: int = 1):
        """Start dual-write processing (both narrow and wide tables)"""
        logger.info(f"Starting dual-write processing with {interval_minutes} minute intervals")
        
        try:
            # Set cutoff time to current DB server time (UTC)
            logger.info("Getting current DB server time (UTC)...")
            db_time_result = self.db_manager.execute_query("SELECT NOW() as current_time")
            if db_time_result and len(db_time_result) > 0:
                db_time = db_time_result[0]['current_time']
                # Convert timezone-aware datetime to naive (UTC)
                if db_time.tzinfo is not None:
                    cutoff_time = db_time.replace(tzinfo=None)
                else:
                    cutoff_time = db_time
                logger.info(f"DB server time (UTC): {cutoff_time}")
            else:
                # Fallback to local time if DB query fails
                from datetime import datetime
                cutoff_time = datetime.now()
                logger.warning(f"Failed to get DB time, using local time: {cutoff_time}")
            
            self.realtime_processor.set_cutoff_time(cutoff_time)
            
            # Start dual-write mode
            self.realtime_processor.start_dual_write_mode(interval_minutes)
        except KeyboardInterrupt:
            logger.info("Dual-write processing stopped by user")
        except Exception as e:
            logger.error(f"Dual-write processing failed: {e}")
    
    def start_concurrent_migration(self, interval_minutes: int = 1) -> Dict[str, Any]:
        """Start concurrent migration: real-time + background backfill"""
        logger.info(f"Starting concurrent migration with {interval_minutes} minute intervals")
        
        try:
            result = concurrent_strategy.start_concurrent_migration(interval_minutes)
            return result
        except Exception as e:
            logger.error(f"Concurrent migration failed: {e}")
            return {
                'status': 'failed',
                'error': str(e),
                'message': 'Failed to start concurrent migration'
            }
    
    def start_hybrid_migration(self, interval_minutes: int = 1) -> Dict[str, Any]:
        """Start hybrid migration strategy"""
        logger.info(f"Starting hybrid migration with {interval_minutes} minute intervals")
        
        try:
            result = hybrid_strategy.start_hybrid_migration(interval_minutes)
            return result
        except Exception as e:
            logger.error(f"Hybrid migration failed: {e}")
            return {
                'status': 'failed',
                'error': str(e),
                'message': 'Failed to start hybrid migration'
            }
    
    def start_parallel_batch_migration(self, cutoff_time: Optional[datetime] = None) -> Dict[str, Any]:
        """Start parallel batch migration for all ships"""
        logger.info("Starting parallel batch migration")
        
        try:
            # Use parallel batch migrator
            result = parallel_batch_migrator.migrate_all_ships_parallel(cutoff_time)
            
            logger.info("Parallel batch migration completed")
            return result
            
        except Exception as e:
            logger.error(f"Parallel batch migration failed: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    def start_streaming_migration(self, interval_minutes: int = 1) -> Dict[str, Any]:
        """Start streaming migration strategy"""
        logger.info(f"Starting streaming migration with {interval_minutes} minute intervals")
        
        try:
            result = streaming_strategy.start_streaming_migration(interval_minutes)
            return result
        except Exception as e:
            logger.error(f"Streaming migration failed: {e}")
            return {
                'status': 'failed',
                'error': str(e),
                'message': 'Failed to start streaming migration'
            }
    
    def get_status_report(self) -> str:
        """Get current status report"""
        report = self.monitor.generate_report()
        
        # Add cutoff time information
        cutoff_status = cutoff_time_manager.get_cutoff_time_status()
        cutoff_info = f"\n\nCutoff Time Status:\n"
        cutoff_info += f"  Has Cutoff Time: {cutoff_status['has_cutoff_time']}\n"
        if cutoff_status['cutoff_time']:
            cutoff_info += f"  Cutoff Time: {cutoff_status['cutoff_time']}\n"
        cutoff_info += f"  File Path: {cutoff_status['file_path']}\n"
        
        # Add connection pool information
        from database import get_connection_pool_status
        pool_status = get_connection_pool_status()
        pool_info = f"\n\nConnection Pool Status:\n"
        pool_info += f"  Status: {pool_status['status']}\n"
        if pool_status['status'] == 'active':
            pool_info += f"  Min Connections: {pool_status['min_connections']}\n"
            pool_info += f"  Max Connections: {pool_status['max_connections']}\n"
            pool_info += f"  Pool Closed: {pool_status['pool_closed']}\n"
        
        return report + cutoff_info + pool_info
    
    def _progress_callback(self, ship_id: str, progress: float, current: int, total: int):
        """Progress callback for migration"""
        self.monitor.log_migration_progress(ship_id, progress, current, total)
    
    def _generate_migration_summary(self, schemas: Dict, table_results: Dict, migration_results: Dict) -> Dict[str, Any]:
        """Generate migration summary"""
        total_ships = len(schemas)
        successful_tables = sum(1 for success in table_results.values() if success)
        successful_migrations = sum(1 for result in migration_results.values() if result['status'] == 'success')
        total_migrated_records = sum(result['migrated_count'] for result in migration_results.values())
        total_errors = sum(result['error_count'] for result in migration_results.values())
        
        return {
            'status': 'completed',
            'timestamp': datetime.now().isoformat(),
            'summary': {
                'total_ships': total_ships,
                'successful_tables': successful_tables,
                'successful_migrations': successful_migrations,
                'total_migrated_records': total_migrated_records,
                'total_errors': total_errors
            },
            'table_results': table_results,
            'migration_results': migration_results
        }


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='PostgreSQL Narrow-to-Wide Table Migration')
    
    parser.add_argument('--mode', choices=[
        'full', 'schema-only', 'tables-only', 'migration-only', 'realtime', 'dual-write', 
        'concurrent', 'hybrid', 'streaming', 'parallel-batch', 'status'
    ], default='full', help='Migration mode')
    
    parser.add_argument('--cutoff-time', type=str, 
                       help='Cutoff time for migration (YYYY-MM-DD HH:MM:SS)')
    
    parser.add_argument('--drop-tables', action='store_true',
                       help='Drop existing tables before creation')
    
    parser.add_argument('--interval', type=int, default=1,
                       help='Real-time processing interval in minutes')
    
    parser.add_argument('--ship-id', type=str,
                       help='Process specific ship ID only')
    
    args = parser.parse_args()
    
    # Parse cutoff time if provided
    cutoff_time = None
    if args.cutoff_time:
        try:
            cutoff_time = datetime.strptime(args.cutoff_time, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            logger.error("Invalid cutoff time format. Use YYYY-MM-DD HH:MM:SS")
            sys.exit(1)
    
    # Initialize migration manager
    migration_manager = MigrationManager()
    
    try:
        if args.mode == 'full':
            result = migration_manager.run_full_migration(cutoff_time, args.drop_tables)
            print(f"Migration completed: {result['status']}")
            
        elif args.mode == 'schema-only':
            result = migration_manager.run_schema_analysis_only()
            print(f"Schema analysis completed: {result['status']}")
            
        elif args.mode == 'tables-only':
            result = migration_manager.run_table_generation_only(args.drop_tables)
            print(f"Table generation completed: {result['status']}")
            
        elif args.mode == 'migration-only':
            result = migration_manager.run_data_migration_only(cutoff_time)
            print(f"Data migration completed: {result['status']}")
            
        elif args.mode == 'realtime':
            migration_manager.start_realtime_processing(args.interval)
            
        elif args.mode == 'dual-write':
            migration_manager.start_dual_write_processing(args.interval)
            
        elif args.mode == 'concurrent':
            result = migration_manager.start_concurrent_migration(args.interval)
            print(f"Concurrent migration started: {result['status']}")
            
        elif args.mode == 'hybrid':
            result = migration_manager.start_hybrid_migration(args.interval)
            print(f"Hybrid migration started: {result['strategy']}")
            
        elif args.mode == 'streaming':
            result = migration_manager.start_streaming_migration(args.interval)
            print(f"Streaming migration started: {result['status']}")
            
        elif args.mode == 'parallel-batch':
            result = migration_manager.start_parallel_batch_migration(cutoff_time)
            print(f"Parallel batch migration completed: {result['total_records_processed']} records processed")
            
        elif args.mode == 'status':
            report = migration_manager.get_status_report()
            print(report)
            
    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Operation failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

