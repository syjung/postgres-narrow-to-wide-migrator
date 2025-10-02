"""
Concurrent migration strategy: Real-time processing + Background backfill
"""
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from loguru import logger
from database import db_manager
from realtime_processor import realtime_processor
from ultra_fast_migrator import ultra_fast_migrator
from cutoff_time_manager import cutoff_time_manager
from config import migration_config


class ConcurrentMigrationStrategy:
    """Concurrent migration strategy for real-time + backfill processing"""
    
    def __init__(self):
        self.realtime_processor = realtime_processor
        self.ultra_fast_migrator = ultra_fast_migrator
        self.backfill_thread = None
        self.backfill_running = False
        self.backfill_progress = {}
        self.migration_start_time = None
    
    def start_concurrent_migration(self, interval_minutes: int = 1) -> Dict[str, Any]:
        """
        Start concurrent migration: real-time processing + background backfill
        
        Args:
            interval_minutes: Real-time processing interval
            
        Returns:
            Migration status dictionary
        """
        logger.info("Starting concurrent migration strategy")
        
        try:
            # Step 1: Start real-time processing immediately
            logger.info("Step 1: Starting real-time processing...")
            self.migration_start_time = datetime.now()
            
            # Set cutoff time to migration start time
            self.realtime_processor.set_cutoff_time(self.migration_start_time)
            
            # Start real-time processing in background thread
            realtime_thread = threading.Thread(
                target=self._start_realtime_processing,
                args=(interval_minutes,),
                daemon=True
            )
            realtime_thread.start()
            
            # Step 2: Start background backfill
            logger.info("Step 2: Starting background backfill...")
            self.backfill_running = True
            self.backfill_thread = threading.Thread(
                target=self._run_background_backfill,
                daemon=True
            )
            self.backfill_thread.start()
            
            return {
                'status': 'started',
                'strategy': 'concurrent',
                'realtime_started': True,
                'backfill_started': True,
                'migration_start_time': self.migration_start_time.isoformat(),
                'message': 'Concurrent migration started successfully'
            }
            
        except Exception as e:
            logger.error(f"Failed to start concurrent migration: {e}")
            return {
                'status': 'failed',
                'error': str(e),
                'message': 'Failed to start concurrent migration'
            }
    
    def _start_realtime_processing(self, interval_minutes: int):
        """Start real-time processing in background thread"""
        try:
            logger.info("Real-time processing started in background")
            self.realtime_processor.start_processing(interval_minutes)
        except Exception as e:
            logger.error(f"Real-time processing failed: {e}")
    
    def _run_background_backfill(self):
        """Run background backfill for historical data"""
        logger.info("🔄 Background backfill started")
        
        try:
            ship_ids = migration_config.target_ship_ids
            total_ships = len(ship_ids)
            
            logger.info(f"📊 Starting backfill for {total_ships} ships")
            
            for index, ship_id in enumerate(ship_ids, 1):
                if not self.backfill_running:
                    logger.info("⏹️ Backfill stopped by user")
                    break
                
                logger.info(f"🚢 [{index}/{total_ships}] Starting backfill for ship: {ship_id}")
                self.backfill_progress[ship_id] = {
                    'status': 'running',
                    'start_time': datetime.now(),
                    'processed_chunks': 0,
                    'total_chunks': 0,
                    'records_processed': 0
                }
                
                try:
                    # Run chunked migration for historical data
                    logger.info(f"🔄 Running chunked migration for {ship_id}...")
                    result = self.ultra_fast_migrator.migrate_ship_data_chunked(
                        ship_id, self.migration_start_time
                    )
                    
                    self.backfill_progress[ship_id].update({
                        'status': 'completed',
                        'end_time': datetime.now(),
                        'records_processed': result.get('migrated_count', 0),
                        'result': result
                    })
                    
                    logger.info(f"✅ [{index}/{total_ships}] Backfill completed for ship {ship_id}: {result.get('migrated_count', 0)} records")
                    
                except Exception as e:
                    logger.error(f"❌ [{index}/{total_ships}] Backfill failed for ship {ship_id}: {e}")
                    self.backfill_progress[ship_id].update({
                        'status': 'failed',
                        'end_time': datetime.now(),
                        'error': str(e)
                    })
            
            logger.info("🎉 Background backfill completed for all ships")
            
        except Exception as e:
            logger.error(f"❌ Background backfill failed: {e}")
    
    def get_concurrent_status(self) -> Dict[str, Any]:
        """Get current status of concurrent migration"""
        return {
            'strategy': 'concurrent',
            'migration_start_time': self.migration_start_time.isoformat() if self.migration_start_time else None,
            'backfill_running': self.backfill_running,
            'realtime_running': self.realtime_processor.running,
            'backfill_progress': self.backfill_progress,
            'timestamp': datetime.now().isoformat()
        }
    
    def stop_concurrent_migration(self):
        """Stop concurrent migration"""
        logger.info("Stopping concurrent migration")
        
        # Stop backfill
        self.backfill_running = False
        if self.backfill_thread and self.backfill_thread.is_alive():
            self.backfill_thread.join(timeout=30)
        
        # Stop real-time processing
        self.realtime_processor.stop_processing()
        
        logger.info("Concurrent migration stopped")


class HybridMigrationStrategy:
    """Hybrid strategy: Start with real-time, backfill in chunks"""
    
    def __init__(self):
        self.concurrent_strategy = ConcurrentMigrationStrategy()
    
    def start_hybrid_migration(self, interval_minutes: int = 1) -> Dict[str, Any]:
        """
        Start hybrid migration strategy
        
        Args:
            interval_minutes: Real-time processing interval
            
        Returns:
            Migration status dictionary
        """
        logger.info("Starting hybrid migration strategy")
        
        # Start concurrent migration
        result = self.concurrent_strategy.start_concurrent_migration(interval_minutes)
        
        return {
            'strategy': 'hybrid',
            'concurrent_result': result,
            'message': 'Hybrid migration started: real-time + background backfill'
        }
    
    def get_hybrid_status(self) -> Dict[str, Any]:
        """Get hybrid migration status"""
        concurrent_status = self.concurrent_strategy.get_concurrent_status()
        
        return {
            'strategy': 'hybrid',
            'concurrent_status': concurrent_status,
            'timestamp': datetime.now().isoformat()
        }


class StreamingMigrationStrategy:
    """Streaming strategy: Process data as it arrives"""
    
    def __init__(self):
        self.realtime_processor = realtime_processor
        self.processed_timestamps = set()
    
    def start_streaming_migration(self, interval_minutes: int = 1) -> Dict[str, Any]:
        """
        Start streaming migration: process all data as it arrives
        
        Args:
            interval_minutes: Processing interval
            
        Returns:
            Migration status dictionary
        """
        logger.info("Starting streaming migration strategy")
        
        try:
            # Set cutoff time to a very early date to process all data
            early_cutoff = datetime(2020, 1, 1)  # Very early date
            self.realtime_processor.set_cutoff_time(early_cutoff)
            
            # Start processing
            self.realtime_processor.start_processing(interval_minutes)
            
            return {
                'status': 'started',
                'strategy': 'streaming',
                'cutoff_time': early_cutoff.isoformat(),
                'message': 'Streaming migration started: processing all data as it arrives'
            }
            
        except Exception as e:
            logger.error(f"Failed to start streaming migration: {e}")
            return {
                'status': 'failed',
                'error': str(e),
                'message': 'Failed to start streaming migration'
            }


# Global strategy instances
concurrent_strategy = ConcurrentMigrationStrategy()
hybrid_strategy = HybridMigrationStrategy()
streaming_strategy = StreamingMigrationStrategy()
