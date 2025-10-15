"""
Parallel Batch Migrator - 선박별 스레드 기반 배치 처리 (Multi-Table 지원)
3개 테이블(auxiliary, engine, navigation)로 데이터 분산 저장
"""
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Any, Optional, Set
from datetime import datetime, timedelta
from loguru import logger
from database import db_manager
from config import migration_config
from thread_logger import get_ship_thread_logger

# Multi-table modules
from channel_router import channel_router
from multi_table_generator import multi_table_generator
from multi_table_chunked_strategy import multi_table_chunked_strategy

# Legacy module for backward compatibility
try:
    from chunked_migration_strategy import chunked_migration_strategy
    LEGACY_MODE_AVAILABLE = True
except:
    LEGACY_MODE_AVAILABLE = False


class ParallelBatchMigrator:
    """선박별 스레드 기반 배치 마이그레이션"""
    
    def __init__(self):
        self.batch_size = migration_config.batch_size
        self.value_format_mapping = migration_config.VALUE_FORMAT_MAPPING
        self.use_multi_table = migration_config.use_multi_table
        
        # Multi-table or legacy mode
        if self.use_multi_table:
            logger.info("🎯 Multi-Table Mode Enabled")
            self.channel_router = channel_router
            self.table_generator = multi_table_generator
            self.migration_strategy = multi_table_chunked_strategy
        else:
            logger.info("⚠️ Legacy Single-Table Mode (will be deprecated)")
            if not LEGACY_MODE_AVAILABLE:
                raise RuntimeError("Legacy mode not available but use_multi_table=False")
            self.migration_strategy = chunked_migration_strategy
        
        # Cache for table columns to avoid repeated queries
        self.table_columns_cache: Dict[str, Set[str]] = {}
        
        # Dynamic thread pool for parallel ship processing
        ship_count = len(migration_config.target_ship_ids)
        self.max_workers = migration_config.get_optimal_thread_count()
        self.thread_pool = ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="batch")
        
        logger.info(f"🚀 Initialized parallel batch migrator:")
        logger.info(f"   📊 Mode: {'Multi-Table (3 tables/ship)' if self.use_multi_table else 'Single-Table (legacy)'}")
        logger.info(f"   📊 Ships: {ship_count}")
        logger.info(f"   📊 Threads: {self.max_workers}")
        logger.info(f"   📊 Ratio: {ship_count/self.max_workers:.2f} ships per thread")
        logger.info(f"   📊 DB Pool: min={migration_config.get_optimal_pool_config()['minconn']}, max={migration_config.get_optimal_pool_config()['maxconn']}")
        
        if self.use_multi_table:
            stats = channel_router.get_channel_count_by_table()
            logger.info(f"   📊 Channel distribution:")
            for table_type, count in stats.items():
                logger.info(f"      - {table_type}: {count} channels")
        
        # Thread-safe locks for shared resources
        self.cache_lock = threading.Lock()
        self.progress_lock = threading.Lock()
        
        # Progress tracking
        self.completed_ships: Set[str] = set()
        self.failed_ships: Set[str] = set()
        self.total_records_processed = 0
    
    
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
            
            # Save global cutoff time (ship-specific times already updated per chunk)
            from cutoff_time_manager import cutoff_time_manager
            
            if cutoff_time:
                # Save global cutoff time for backward compatibility
                cutoff_time_manager.save_cutoff_time(cutoff_time)
                logger.info(f"📅 Global cutoff time saved: {cutoff_time}")
                logger.info(f"📅 Ship-specific cutoffs already updated incrementally during migration")
            else:
                # Use current time as cutoff if not specified
                current_cutoff = datetime.now()
                cutoff_time_manager.save_cutoff_time(current_cutoff)
                logger.info(f"📅 Global cutoff time set to current time: {current_cutoff}")
                logger.info(f"📅 Ship-specific cutoffs already updated incrementally during migration")
            
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
        thread_logger = get_ship_thread_logger(ship_id, mode="batch")
        thread_id = threading.current_thread().ident
        thread_name = threading.current_thread().name
        
        thread_logger.info(f"🚢 Starting batch migration for ship: {ship_id}")
        
        try:
            start_time = time.time()
            result = self._migrate_ship_data(ship_id, cutoff_time, thread_logger)
            end_time = time.time()
            
            processing_time = end_time - start_time
            result['processing_time'] = processing_time
            
            thread_logger.success(f"Completed batch migration for ship: {ship_id} in {processing_time:.2f}s")
            
            return result
            
        except Exception as e:
            thread_logger.error(f"Error migrating ship {ship_id}: {e}")
            return {
                'success': False,
                'ship_id': ship_id,
                'error': str(e),
                'records_processed': 0
            }
    
    def _migrate_ship_data(self, ship_id: str, cutoff_time: Optional[datetime] = None, thread_logger=None) -> Dict[str, Any]:
        """Migrate data for a specific ship using chunked strategy (Multi-Table support)"""
        if thread_logger is None:
            thread_logger = get_ship_thread_logger(ship_id)
        
        mode = "Multi-Table" if self.use_multi_table else "Single-Table"
        thread_logger.info(f"🚀 Starting chunked migration for ship: {ship_id} ({mode} mode)")
        
        try:
            # Ensure tables exist
            if self.use_multi_table:
                # Create 3 tables for this ship
                thread_logger.info(f"📋 Creating 3 tables for ship: {ship_id}")
                if not multi_table_generator.ensure_all_tables_exist(ship_id):
                    raise RuntimeError(f"Failed to create tables for {ship_id}")
                
                thread_logger.success(f"✅ All 3 tables ready for {ship_id}")
            else:
                # Legacy single table
                table_name = f'tbl_data_timeseries_{ship_id.upper()}'
                thread_logger.info(f"📋 Target table: {table_name}")
                
                if not db_manager.check_table_exists(table_name):
                    thread_logger.info(f"📋 Creating table: {table_name}")
                    self._create_table_for_ship_legacy(ship_id, thread_logger)
                else:
                    thread_logger.info(f"📋 Table already exists: {table_name}")
            
            # Use appropriate migration strategy
            total_records = 0
            total_narrow_records = 0
            start_migration_time = time.time()
            
            # Get data chunks
            chunks = list(self.migration_strategy.get_data_chunks(ship_id, cutoff_time, thread_logger))
            total_chunks = len(chunks)
            thread_logger.info(f"📊 Found {total_chunks} chunks to process")
            
            if total_chunks > 0:
                first_chunk_start = chunks[0][0]
                last_chunk_end = chunks[-1][1]
                thread_logger.info(f"📅 Time range: {first_chunk_start} to {last_chunk_end}")
            
            for i, (start_time, end_time) in enumerate(chunks, 1):
                chunk_start_time = time.time()
                progress_pct = (i / total_chunks) * 100
                
                thread_logger.info(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
                thread_logger.info(f"🔄 Chunk {i}/{total_chunks} ({progress_pct:.1f}%)")
                thread_logger.info(f"📅 Date range: {start_time} to {end_time}")
                
                try:
                    if self.use_multi_table:
                        # Multi-table migration
                        chunk_result = self.migration_strategy.migrate_chunk(
                            ship_id, start_time, end_time, thread_logger
                        )
                    else:
                        # Legacy single-table migration
                        chunk_result = self.migration_strategy.migrate_chunk(
                            ship_id, start_time, end_time, table_name, thread_logger
                        )
                    
                    chunk_duration = time.time() - chunk_start_time
                    
                    if chunk_result['status'] == 'completed':
                        records_processed = chunk_result.get('records_processed', 0)
                        narrow_records = chunk_result.get('narrow_records', 0)
                        total_records += records_processed
                        total_narrow_records += narrow_records
                        
                        thread_logger.success(f"✅ Chunk {i}/{total_chunks} completed in {chunk_duration:.2f}s")
                        
                        # ✅ Update BATCH cutoff time after each successful chunk
                        from cutoff_time_manager import cutoff_time_manager
                        cutoff_time_manager.save_batch_cutoff_time(ship_id, end_time)
                        thread_logger.debug(f"💾 Updated batch cutoff time for {ship_id}: {end_time}")
                        
                        # Calculate ETA
                        elapsed_time = time.time() - start_migration_time
                        avg_time_per_chunk = elapsed_time / i
                        remaining_chunks = total_chunks - i
                        eta_seconds = avg_time_per_chunk * remaining_chunks
                        eta_minutes = eta_seconds / 60
                        
                        thread_logger.info(f"📊 Progress: {i}/{total_chunks} chunks ({progress_pct:.1f}%)")
                        thread_logger.info(f"📊 Speed: {chunk_duration:.2f}s/chunk, Avg: {avg_time_per_chunk:.2f}s/chunk")
                        thread_logger.info(f"📊 ETA: {eta_minutes:.1f} minutes ({remaining_chunks} chunks remaining)")
                        thread_logger.info(f"📊 Total so far: {total_narrow_records:,} narrow → {total_records:,} wide records")
                        thread_logger.debug(f"📊 Cutoff time: {end_time}")
                        
                    else:
                        thread_logger.info(f"⏭️ Chunk {i}/{total_chunks} skipped in {chunk_duration:.2f}s: {chunk_result.get('message', 'No data')}")
                        
                        # ✅ Update BATCH cutoff time even for skipped chunks
                        from cutoff_time_manager import cutoff_time_manager
                        cutoff_time_manager.save_batch_cutoff_time(ship_id, end_time)
                        thread_logger.debug(f"💾 Updated batch cutoff time for skipped chunk: {end_time}")
                        
                except Exception as e:
                    chunk_duration = time.time() - chunk_start_time
                    thread_logger.error(f"❌ Chunk {i}/{total_chunks} failed after {chunk_duration:.2f}s: {e}")
                    # Continue with next chunk instead of failing entire ship
                    continue
            
            total_migration_time = time.time() - start_migration_time
            thread_logger.success(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            thread_logger.success(f"🎉 Ship migration completed: {ship_id}")
            thread_logger.success(f"📊 Total time: {total_migration_time:.2f}s ({total_migration_time/60:.1f} minutes)")
            thread_logger.success(f"📊 Chunks processed: {total_chunks}")
            thread_logger.success(f"📊 Records: {total_narrow_records:,} narrow → {total_records:,} wide")
            
            result = {
                'success': True,
                'ship_id': ship_id,
                'records_processed': total_records,
                'chunks_processed': len(chunks),
                'mode': mode
            }
            
            if self.use_multi_table:
                result['tables'] = [
                    f"tbl_data_timeseries_{ship_id.lower()}_1",
                    f"tbl_data_timeseries_{ship_id.lower()}_2",
                    f"tbl_data_timeseries_{ship_id.lower()}_3"
                ]
            else:
                result['table_name'] = f'tbl_data_timeseries_{ship_id.upper()}'
            
            return result
            
        except Exception as e:
            thread_logger.error(f"Ship migration failed: {e}")
            return {
                'success': False,
                'ship_id': ship_id,
                'error': str(e),
                'records_processed': 0
            }
    
    def _create_table_for_ship_legacy(self, ship_id: str, thread_logger=None):
        """Create legacy single table for ship (backward compatibility)"""
        if thread_logger is None:
            thread_logger = get_ship_thread_logger(ship_id)
        
        try:
            from table_generator import table_generator
            from schema_analyzer import schema_analyzer
            
            thread_logger.info(f"📋 Creating legacy table for ship: {ship_id}")
            
            # Analyze schema for this ship
            schema = schema_analyzer.analyze_ship_data(ship_id, sample_minutes=60)
            
            # Generate table - pass both ship_id and schema
            success = table_generator.generate_table(ship_id, schema, drop_if_exists=False)
            
            if success:
                thread_logger.success(f"Created table for ship_id: {ship_id}")
            else:
                thread_logger.error(f"Failed to create table for ship_id: {ship_id}")
                
        except Exception as e:
            thread_logger.error(f"Error creating table for ship_id {ship_id}: {e}")
    
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
