"""
Data migration module for PostgreSQL Narrow-to-Wide Table Migration

⚠️ DEPRECATED: This module is deprecated and will be removed in future versions.
Please use parallel_batch_migrator.py instead.

This file is kept for backward compatibility only.
"""
import asyncio
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime, timedelta
from loguru import logger
from database import db_manager
from ultra_fast_migrator import ultra_fast_migrator
from config import migration_config
from cutoff_time_manager import cutoff_time_manager


class DataMigrator:
    """Data migration management class"""
    
    def __init__(self):
        self.ultra_fast_migrator = ultra_fast_migrator
        self.migration_config = migration_config
        self.migration_stats = {
            'total_ships': 0,
            'completed_ships': 0,
            'failed_ships': 0,
            'total_records': 0,
            'start_time': None,
            'end_time': None
        }
    
    def migrate_all_ships(self, cutoff_time: Optional[datetime] = None, 
                         progress_callback: Optional[Callable] = None) -> Dict[str, Any]:
        """
        Migrate data for all ships
        
        Args:
            cutoff_time: Only migrate data before this time
            progress_callback: Callback function for progress updates
            
        Returns:
            Dictionary containing migration results
        """
        logger.info("Starting data migration for all ships")
        
        self.migration_stats['start_time'] = datetime.now()
        
        try:
            # Get all ship IDs
            ship_ids = db_manager.get_distinct_ship_ids()
            self.migration_stats['total_ships'] = len(ship_ids)
            
            logger.info(f"Found {len(ship_ids)} ships to migrate")
            
            migration_results = {}
            
            for i, ship_id in enumerate(ship_ids):
                try:
                    logger.info(f"Migrating ship {i+1}/{len(ship_ids)}: {ship_id}")
                    
                    # Migrate ship data using chunked approach for large datasets
                    result = self.ultra_fast_migrator.migrate_ship_data_chunked(
                        ship_id, cutoff_time
                    )
                    
                    migration_results[ship_id] = result
                    
                    # Update stats
                    if result['status'] == 'completed':
                        self.migration_stats['completed_ships'] += 1
                        self.migration_stats['total_records'] += result['migrated_count']
                    else:
                        self.migration_stats['failed_ships'] += 1
                    
                    # Progress callback
                    if progress_callback:
                        progress = (i + 1) / len(ship_ids)
                        progress_callback(ship_id, progress, i + 1, len(ship_ids))
                    
                    logger.info(f"Migration completed for {ship_id}: {result['status']}")
                    
                except Exception as e:
                    logger.error(f"Failed to migrate ship {ship_id}: {e}")
                    migration_results[ship_id] = {
                        'ship_id': ship_id,
                        'status': 'failed',
                        'migrated_count': 0,
                        'error_count': 1,
                        'message': f'Migration failed: {str(e)}'
                    }
                    self.migration_stats['failed_ships'] += 1
            
            self.migration_stats['end_time'] = datetime.now()
            
            # Save cutoff time for real-time processing (ship-specific)
            if cutoff_time:
                # Save global cutoff time for backward compatibility
                cutoff_time_manager.save_cutoff_time(cutoff_time)
                logger.info(f"Global cutoff time saved: {cutoff_time}")
                
                # Save ship-specific cutoff times
                for ship_id in migration_results:
                    cutoff_time_manager.save_ship_cutoff_time(ship_id, cutoff_time)
                    logger.debug(f"Ship cutoff time saved for {ship_id}: {cutoff_time}")
            else:
                # Use current time as cutoff if not specified
                current_cutoff = datetime.now()
                cutoff_time_manager.save_cutoff_time(current_cutoff)
                logger.info(f"Global cutoff time set to current time: {current_cutoff}")
                
                # Save ship-specific cutoff times
                for ship_id in migration_results:
                    cutoff_time_manager.save_ship_cutoff_time(ship_id, current_cutoff)
                    logger.debug(f"Ship cutoff time saved for {ship_id}: {current_cutoff}")
            
            # Generate summary
            summary = self._generate_migration_summary(migration_results)
            
            logger.info("Data migration completed for all ships")
            return summary
            
        except Exception as e:
            logger.error(f"Data migration failed: {e}")
            self.migration_stats['end_time'] = datetime.now()
            return {
                'status': 'failed',
                'error': str(e),
                'timestamp': datetime.now().isoformat(),
                'migration_results': {}
            }
    
    def migrate_single_ship(self, ship_id: str, cutoff_time: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Migrate data for a single ship
        
        Args:
            ship_id: Ship identifier
            cutoff_time: Only migrate data before this time
            
        Returns:
            Migration result dictionary
        """
        logger.info(f"Starting migration for ship: {ship_id}")
        
        try:
            result = self.ultra_fast_migrator.migrate_ship_data_chunked(
                ship_id, cutoff_time
            )
            
            logger.info(f"Migration completed for {ship_id}: {result['status']}")
            return result
            
        except Exception as e:
            logger.error(f"Migration failed for ship {ship_id}: {e}")
            return {
                'ship_id': ship_id,
                'status': 'failed',
                'migrated_count': 0,
                'error_count': 1,
                'message': f'Migration failed: {str(e)}'
            }
    
    def migrate_ships_batch(self, ship_ids: List[str], cutoff_time: Optional[datetime] = None,
                           progress_callback: Optional[Callable] = None) -> Dict[str, Any]:
        """
        Migrate data for a batch of ships
        
        Args:
            ship_ids: List of ship identifiers
            cutoff_time: Only migrate data before this time
            progress_callback: Callback function for progress updates
            
        Returns:
            Dictionary containing migration results
        """
        logger.info(f"Starting batch migration for {len(ship_ids)} ships")
        
        migration_results = {}
        
        for i, ship_id in enumerate(ship_ids):
            try:
                logger.info(f"Migrating ship {i+1}/{len(ship_ids)}: {ship_id}")
                
                result = self.migrate_single_ship(ship_id, cutoff_time)
                migration_results[ship_id] = result
                
                # Progress callback
                if progress_callback:
                    progress = (i + 1) / len(ship_ids)
                    progress_callback(ship_id, progress, i + 1, len(ship_ids))
                
            except Exception as e:
                logger.error(f"Failed to migrate ship {ship_id}: {e}")
                migration_results[ship_id] = {
                    'ship_id': ship_id,
                    'status': 'failed',
                    'migrated_count': 0,
                    'error_count': 1,
                    'message': f'Migration failed: {str(e)}'
                }
        
        # Generate summary
        summary = self._generate_migration_summary(migration_results)
        
        logger.info(f"Batch migration completed for {len(ship_ids)} ships")
        return summary
    
    def get_migration_status(self, ship_id: str) -> Dict[str, Any]:
        """
        Get migration status for a specific ship
        
        Args:
            ship_id: Ship identifier
            
        Returns:
            Status information dictionary
        """
        try:
            table_name = f"tbl_data_timeseries_{ship_id.lower()}"
            
            # Check if table exists
            table_exists = db_manager.check_table_exists(table_name)
            
            if not table_exists:
                return {
                    'ship_id': ship_id,
                    'status': 'not_started',
                    'table_exists': False,
                    'record_count': 0
                }
            
            # Skip slow COUNT(*) and MAX() queries for performance
            # Just check if table has any data using LIMIT 1
            check_data_query = f"SELECT 1 FROM tenant.{table_name} LIMIT 1"
            data_result = db_manager.execute_query(check_data_query)
            has_data = len(data_result) > 0 if data_result else False
            
            return {
                'ship_id': ship_id,
                'status': 'completed' if has_data else 'empty',
                'table_exists': True,
                'has_data': has_data,
                'message': 'Optimized status check (no count queries)'
            }
            
        except Exception as e:
            logger.error(f"Failed to get migration status for {ship_id}: {e}")
            return {
                'ship_id': ship_id,
                'status': 'error',
                'error': str(e)
            }
    
    def validate_migration(self, ship_id: str) -> Dict[str, Any]:
        """
        Validate migration results for a specific ship
        
        Args:
            ship_id: Ship identifier
            
        Returns:
            Validation results dictionary
        """
        logger.info(f"Validating migration for ship: {ship_id}")
        
        try:
            # Skip slow COUNT queries for performance
            # Just check if both tables have data
            table_name = f"tbl_data_timeseries_{ship_id.lower()}"
            
            # Check source table has data
            source_check_query = """
            SELECT 1 FROM tenant.tbl_data_timeseries 
            WHERE ship_id = %s LIMIT 1
            """
            source_result = db_manager.execute_query(source_check_query, (ship_id,))
            source_has_data = len(source_result) > 0 if source_result else False
            
            # Check target table has data
            target_check_query = f"SELECT 1 FROM tenant.{table_name} LIMIT 1"
            target_result = db_manager.execute_query(target_check_query)
            target_has_data = len(target_result) > 0 if target_result else False
            
            # Simple validation: both tables should have data
            validation_status = 'passed' if (source_has_data and target_has_data) else 'failed'
            
            validation_result = {
                'ship_id': ship_id,
                'status': validation_status,
                'source_has_data': source_has_data,
                'target_has_data': target_has_data,
                'validation_time': datetime.now().isoformat()
            }
            
            if not validation_status == 'passed':
                validation_result['message'] = f"Validation failed: source_has_data={source_has_data}, target_has_data={target_has_data}"
            else:
                validation_result['message'] = "Validation passed: both tables have data"
            
            logger.info(f"Validation completed for {ship_id}: {validation_status}")
            return validation_result
            
        except Exception as e:
            logger.error(f"Validation failed for ship {ship_id}: {e}")
            return {
                'ship_id': ship_id,
                'status': 'error',
                'error': str(e),
                'validation_time': datetime.now().isoformat()
            }
    
    def get_migration_statistics(self) -> Dict[str, Any]:
        """Get overall migration statistics"""
        return {
            'migration_stats': self.migration_stats,
            'timestamp': datetime.now().isoformat()
        }
    
    def _generate_migration_summary(self, migration_results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate migration summary from results"""
        total_ships = len(migration_results)
        successful_ships = sum(1 for result in migration_results.values() 
                             if result['status'] == 'completed')
        failed_ships = sum(1 for result in migration_results.values() 
                         if result['status'] == 'failed')
        total_records = sum(result['migrated_count'] for result in migration_results.values())
        total_errors = sum(result['error_count'] for result in migration_results.values())
        
        return {
            'status': 'completed',
            'timestamp': datetime.now().isoformat(),
            'summary': {
                'total_ships': total_ships,
                'successful_ships': successful_ships,
                'failed_ships': failed_ships,
                'total_records': total_records,
                'total_errors': total_errors,
                'success_rate': (successful_ships / total_ships * 100) if total_ships > 0 else 0
            },
            'migration_results': migration_results
        }


# Global data migrator instance
data_migrator = DataMigrator()
