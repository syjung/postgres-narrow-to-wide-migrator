"""
Parallel Batch Migrator - 선박별 스레드 기반 배치 처리
실시간 처리에서 발견된 오류들을 반영한 안전한 배치 처리
"""
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Any, Optional, Set
from datetime import datetime, timedelta
from loguru import logger
from database import db_manager
from table_generator import table_generator
from schema_analyzer import schema_analyzer
from config import migration_config
from chunked_migration_strategy import chunked_migration_strategy


class ParallelBatchMigrator:
    """선박별 스레드 기반 배치 마이그레이션"""
    
    def __init__(self):
        self.batch_size = migration_config.batch_size
        self.value_format_mapping = migration_config.VALUE_FORMAT_MAPPING
        
        # Load allowed columns from column_list.txt
        self.allowed_columns = self._load_allowed_columns()
        
        # Cache for table columns to avoid repeated queries
        self.table_columns_cache: Dict[str, Set[str]] = {}
        
        # Thread pool for parallel ship processing
        ship_count = len(migration_config.target_ship_ids)
        self.max_workers = min(ship_count, migration_config.parallel_workers)
        self.thread_pool = ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="batch")
        
        logger.info(f"🚀 Initialized parallel batch migrator with {self.max_workers} threads for {ship_count} ships")
        logger.info(f"📊 Thread limit: {migration_config.parallel_workers}, Ships: {ship_count}")
        
        # Thread-safe locks for shared resources
        self.cache_lock = threading.Lock()
        self.progress_lock = threading.Lock()
        
        # Progress tracking
        self.completed_ships: Set[str] = set()
        self.failed_ships: Set[str] = set()
        self.total_records_processed = 0
    
    def _load_allowed_columns(self) -> Set[str]:
        """Load allowed columns from column_list.txt"""
        try:
            with open('column_list.txt', 'r', encoding='utf-8') as f:
                columns = {line.strip() for line in f if line.strip()}
            logger.info(f"✅ Loaded {len(columns)} allowed columns from column_list.txt")
            return columns
        except FileNotFoundError:
            logger.warning("⚠️ column_list.txt not found, using empty allowed columns set")
            return set()
        except Exception as e:
            logger.error(f"❌ Failed to load column_list.txt: {e}")
            return set()
    
    def migrate_all_ships_parallel(self, cutoff_time: Optional[datetime] = None) -> Dict[str, Any]:
        """
        모든 선박을 병렬로 마이그레이션
        
        Args:
            cutoff_time: 마이그레이션 cutoff 시간
            
        Returns:
            전체 마이그레이션 결과
        """
        logger.info("🚀 Starting parallel batch migration for all ships")
        logger.info(f"📊 Target ships: {migration_config.target_ship_ids}")
        logger.info(f"📊 Max workers: {self.max_workers}")
        
        start_time = time.time()
        
        try:
            # Submit all ship migration tasks to thread pool
            future_to_ship = {}
            for ship_id in migration_config.target_ship_ids:
                future = self.thread_pool.submit(self._migrate_ship_safe, ship_id, cutoff_time)
                future_to_ship[future] = ship_id
            
            # Process completed tasks
            completed_count = 0
            failed_count = 0
            ship_results = {}
            
            for future in as_completed(future_to_ship):
                ship_id = future_to_ship[future]
                completed_count += 1
                
                try:
                    result = future.result()
                    ship_results[ship_id] = result
                    
                    if result['success']:
                        logger.info(f"✅ [{completed_count}/{len(migration_config.target_ship_ids)}] Completed migration for ship: {ship_id}")
                        logger.info(f"   📊 Records processed: {result.get('records_processed', 0)}")
                        logger.info(f"   📊 Processing time: {result.get('processing_time', 0):.2f}s")
                        
                        with self.progress_lock:
                            self.completed_ships.add(ship_id)
                            self.total_records_processed += result.get('records_processed', 0)
                    else:
                        logger.error(f"❌ [{completed_count}/{len(migration_config.target_ship_ids)}] Failed migration for ship: {ship_id}")
                        logger.error(f"   📊 Error: {result.get('error', 'Unknown error')}")
                        
                        with self.progress_lock:
                            self.failed_ships.add(ship_id)
                        failed_count += 1
                        
                except Exception as e:
                    logger.error(f"❌ [{completed_count}/{len(migration_config.target_ship_ids)}] Exception migrating ship {ship_id}: {e}")
                    
                    with self.progress_lock:
                        self.failed_ships.add(ship_id)
                    failed_count += 1
                    
                    ship_results[ship_id] = {
                        'success': False,
                        'error': str(e),
                        'records_processed': 0
                    }
            
            end_time = time.time()
            total_time = end_time - start_time
            
            # Generate summary
            summary = {
                'total_ships': len(migration_config.target_ship_ids),
                'completed_ships': len(self.completed_ships),
                'failed_ships': len(self.failed_ships),
                'total_records_processed': self.total_records_processed,
                'total_processing_time': total_time,
                'average_time_per_ship': total_time / len(migration_config.target_ship_ids) if migration_config.target_ship_ids else 0,
                'ship_results': ship_results,
                'cutoff_time': cutoff_time.isoformat() if cutoff_time else None
            }
            
            logger.info(f"🎉 Parallel batch migration completed!")
            logger.info(f"   📊 Total ships: {summary['total_ships']}")
            logger.info(f"   📊 Completed: {summary['completed_ships']}")
            logger.info(f"   📊 Failed: {summary['failed_ships']}")
            logger.info(f"   📊 Total records: {summary['total_records_processed']}")
            logger.info(f"   📊 Total time: {summary['total_processing_time']:.2f}s")
            logger.info(f"   📊 Avg time per ship: {summary['average_time_per_ship']:.2f}s")
            
            return summary
            
        except Exception as e:
            logger.error(f"❌ Parallel batch migration failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'total_ships': len(migration_config.target_ship_ids),
                'completed_ships': len(self.completed_ships),
                'failed_ships': len(self.failed_ships)
            }
    
    def _migrate_ship_safe(self, ship_id: str, cutoff_time: Optional[datetime] = None) -> Dict[str, Any]:
        """Thread-safe wrapper for ship migration with thread info"""
        thread_id = threading.current_thread().ident
        thread_name = threading.current_thread().name
        
        logger.info(f"🚢 [Thread-{thread_id}] Starting batch migration for ship: {ship_id}")
        
        try:
            start_time = time.time()
            result = self._migrate_ship_data(ship_id, cutoff_time)
            end_time = time.time()
            
            processing_time = end_time - start_time
            result['processing_time'] = processing_time
            
            logger.info(f"✅ [Thread-{thread_id}] Completed batch migration for ship: {ship_id} in {processing_time:.2f}s")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ [Thread-{thread_id}] Error migrating ship {ship_id}: {e}")
            return {
                'success': False,
                'ship_id': ship_id,
                'error': str(e),
                'records_processed': 0
            }
    
    def _migrate_ship_data(self, ship_id: str, cutoff_time: Optional[datetime] = None) -> Dict[str, Any]:
        """Migrate data for a specific ship using chunked strategy"""
        thread_id = threading.current_thread().ident
        
        logger.info(f"🚀 [Thread-{thread_id}] Starting chunked migration for ship: {ship_id}")
        
        try:
            # Ensure table exists and is properly created
            table_name = f'tbl_data_timeseries_{ship_id.upper()}'  # Force uppercase for consistency
            logger.info(f"📋 [Thread-{thread_id}] Target table: {table_name}")
            
            if not db_manager.check_table_exists(table_name):
                logger.info(f"📋 [Thread-{thread_id}] Creating table: {table_name}")
                self._create_table_for_ship(ship_id)
            else:
                logger.info(f"📋 [Thread-{thread_id}] Table already exists: {table_name}")
            
            # Use chunked migration strategy
            total_records = 0
            
            # Get data chunks
            chunks = list(chunked_migration_strategy.get_data_chunks(ship_id, cutoff_time))
            logger.info(f"📊 [Thread-{thread_id}] Found {len(chunks)} chunks to process")
            
            for i, (start_time, end_time) in enumerate(chunks):
                logger.info(f"🔄 [Thread-{thread_id}] Processing chunk {i+1}/{len(chunks)}: {start_time} to {end_time}")
                
                try:
                    chunk_result = chunked_migration_strategy.migrate_chunk(ship_id, start_time, end_time, table_name)
                    
                    if chunk_result['status'] == 'completed':
                        records_processed = chunk_result.get('records_processed', 0)
                        total_records += records_processed
                        logger.info(f"✅ [Thread-{thread_id}] Chunk {i+1} completed: {records_processed} records")
                    else:
                        logger.info(f"⏭️ [Thread-{thread_id}] Chunk {i+1} skipped: {chunk_result.get('message', 'No data')}")
                        
                except Exception as e:
                    logger.error(f"❌ [Thread-{thread_id}] Chunk {i+1} failed: {e}")
                    # Continue with next chunk instead of failing entire ship
                    continue
            
            logger.info(f"✅ [Thread-{thread_id}] Ship migration completed: {total_records} total records")
            
            return {
                'success': True,
                'ship_id': ship_id,
                'records_processed': total_records,
                'table_name': table_name,
                'chunks_processed': len(chunks)
            }
            
        except Exception as e:
            logger.error(f"❌ [Thread-{thread_id}] Ship migration failed: {e}")
            return {
                'success': False,
                'ship_id': ship_id,
                'error': str(e),
                'records_processed': 0
            }
    
    def _create_table_for_ship(self, ship_id: str):
        """Create table for ship if it doesn't exist (thread-safe)"""
        thread_id = threading.current_thread().ident
        
        try:
            logger.info(f"📋 [Thread-{thread_id}] Creating table for ship: {ship_id}")
            
            # Analyze schema for this ship
            schema = schema_analyzer.analyze_ship_data(ship_id, sample_minutes=60)
            
            # Generate table - pass both ship_id and schema
            success = table_generator.generate_table(ship_id, schema, drop_if_exists=False)
            
            if success:
                logger.info(f"✅ [Thread-{thread_id}] Created table for ship_id: {ship_id}")
            else:
                logger.error(f"❌ [Thread-{thread_id}] Failed to create table for ship_id: {ship_id}")
                
        except Exception as e:
            logger.error(f"❌ [Thread-{thread_id}] Error creating table for ship_id {ship_id}: {e}")
    
    def get_migration_progress(self) -> Dict[str, Any]:
        """Get current migration progress"""
        with self.progress_lock:
            return {
                'completed_ships': list(self.completed_ships),
                'failed_ships': list(self.failed_ships),
                'total_records_processed': self.total_records_processed,
                'progress_percentage': (len(self.completed_ships) / len(migration_config.target_ship_ids)) * 100 if migration_config.target_ship_ids else 0
            }
    
    def shutdown(self):
        """Shutdown thread pool"""
        logger.info("🛑 Shutting down parallel batch migrator")
        self.thread_pool.shutdown(wait=True)
        logger.info("✅ Parallel batch migrator shutdown complete")


# Global parallel batch migrator instance
parallel_batch_migrator = ParallelBatchMigrator()
