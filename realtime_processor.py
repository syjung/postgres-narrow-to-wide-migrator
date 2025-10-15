"""
Real-time data processing module (Multi-Table support)
"""
import asyncio
import schedule
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Any, Optional, Set
from datetime import datetime, timedelta
from loguru import logger
from database import db_manager
from config import migration_config
from cutoff_time_manager import cutoff_time_manager
from thread_logger import get_ship_thread_logger

# Multi-table support
from channel_router import channel_router
from multi_table_generator import multi_table_generator

# Legacy support
try:
    from table_generator import table_generator
    from schema_analyzer import schema_analyzer
    LEGACY_MODE_AVAILABLE = True
except:
    LEGACY_MODE_AVAILABLE = False


class RealTimeProcessor:
    """Handles real-time data processing for wide tables (Multi-Table support)"""
    
    def __init__(self):
        self.batch_size = migration_config.batch_size
        self.value_format_mapping = migration_config.VALUE_FORMAT_MAPPING
        self.processed_timestamps: Set[datetime] = set()
        self.running = False
        self.cutoff_time: Optional[datetime] = None  # 마이그레이션 완료 시점
        self.dual_write_mode = migration_config.dual_write_mode
        self.use_multi_table = migration_config.use_multi_table
        
        # Multi-table or legacy mode
        if self.use_multi_table:
            logger.info("🎯 Real-time: Multi-Table Mode Enabled")
            self.channel_router = channel_router
            self.table_generator = multi_table_generator
            # Multi-table mode doesn't use allowed_columns file
            self.allowed_columns = None
        else:
            logger.info("⚠️ Real-time: Legacy Single-Table Mode")
            # Load allowed columns from column_list.txt
            self.allowed_columns = self._load_allowed_columns()
        
        # Cache for table columns to avoid repeated queries
        self.table_columns_cache: Dict[str, Set[str]] = {}
        
        # Dynamic thread pool for parallel ship processing
        ship_count = len(migration_config.target_ship_ids)
        self.max_workers = migration_config.get_optimal_thread_count()
        self.thread_pool = ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="realtime")
        
        logger.info(f"🚀 Initialized real-time processor:")
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
        else:
            logger.info(f"   📊 Allowed columns: {len(self.allowed_columns) if self.allowed_columns else 0}")
        
        # Thread-safe locks for shared resources
        self.cache_lock = threading.Lock()
        self.cutoff_lock = threading.Lock()
    
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
    
    def start_processing(self, interval_minutes: int = 1):
        """
        Start real-time data processing with minute-based scheduling
        
        Args:
            interval_minutes: Processing interval in minutes (should be 1 for minute-based processing)
        """
        logger.info(f"Starting real-time processing with {interval_minutes} minute intervals")
        logger.info("🕐 Scheduling: Process previous minute's 4 batches (00, 15, 30, 45s) at each minute mark")
        
        # Load cutoff time from file if not set
        if not self.cutoff_time:
            self.load_cutoff_time()
        
        # Schedule processing job to run at the start of every minute
        schedule.every().minute.at(":00").do(self._process_new_data_at_minute_mark)
        
        self.running = True
        
        # Start processing loop
        while self.running:
            try:
                schedule.run_pending()
                time.sleep(1)  # Check every second for precise timing
            except KeyboardInterrupt:
                logger.info("Received interrupt signal, stopping real-time processing")
                self.stop_processing()
                break
            except Exception as e:
                logger.error(f"Error in real-time processing loop: {e}")
                time.sleep(30)  # Wait before retrying
    
    def stop_processing(self):
        """Stop real-time data processing"""
        logger.info("Stopping real-time data processing")
        self.running = False
        schedule.clear()
        
        # Shutdown thread pool gracefully
        logger.info("Shutting down thread pool...")
        self.thread_pool.shutdown(wait=True)
        logger.info("Thread pool shutdown complete")
    
    def set_cutoff_time(self, cutoff_time: datetime):
        """
        Set cutoff time for migration completion and save to file
        
        Args:
            cutoff_time: Time when migration was completed
        """
        self.cutoff_time = cutoff_time
        
        # Save to file for persistence
        success = cutoff_time_manager.save_cutoff_time(cutoff_time)
        
        if success:
            logger.info(f"Cutoff time set and saved: {cutoff_time}")
        else:
            logger.error(f"Failed to save cutoff time: {cutoff_time}")
    
    def load_cutoff_time(self) -> Optional[datetime]:
        """
        Load cutoff time from file
        
        Returns:
            Cutoff time if exists, None otherwise
        """
        cutoff_time = cutoff_time_manager.load_cutoff_time()
        if cutoff_time:
            self.cutoff_time = cutoff_time
            logger.info(f"Cutoff time loaded from file: {cutoff_time}")
        return cutoff_time
    
    def start_dual_write_mode(self, interval_minutes: int = 1):
        """
        Start dual-write mode (write to both narrow and wide tables)
        
        Args:
            interval_minutes: Processing interval in minutes
        """
        logger.info(f"Starting dual-write mode with {interval_minutes} minute intervals")
        self.dual_write_mode = True
        self.start_processing(interval_minutes)
    
    def _process_new_data_at_minute_mark(self):
        """Process new data at minute mark using optimized cutoff strategy"""
        from cutoff_time_strategy import cutoff_strategy
        
        logger.info("🕐 === MINUTE MARK PROCESSING STARTED ===")
        
        # Get current processing window
        window = cutoff_strategy.get_processing_window()
        target_minute = window['target_minute']
        
        logger.info(f"🕐 Processing minute: {target_minute}")
        logger.info(f"🕐 Window: {window['start_time']} to {window['end_time']}")
        
        # Check if this minute should be processed
        if not cutoff_strategy.should_process_minute(target_minute):
            logger.info(f"⏭️ Minute {target_minute} already processed, skipping")
            return
        
        logger.info(f"✅ Processing minute {target_minute} - 4 batches (00, 15, 30, 45s)")
        
        # Process the data
        self._process_new_data()
        
        # Mark minute as processed
        cutoff_strategy.mark_minute_processed(target_minute)
        
        logger.info(f"✅ === MINUTE MARK PROCESSING COMPLETED: {target_minute} ===")

    def _process_new_data(self):
        """Process new data for all target ships in parallel"""
        logger.info("🔄 Processing new data batch (parallel mode)")
        
        try:
            # Get target ship IDs from configuration
            ship_ids = db_manager.get_distinct_ship_ids()
            total_ships = len(ship_ids)
            
            logger.info(f"📊 Processing data for {total_ships} ships using {self.max_workers} threads")
            
            # Submit all ship processing tasks to thread pool
            future_to_ship = {}
            for ship_id in ship_ids:
                future = self.thread_pool.submit(self._process_ship_data_safe, ship_id)
                future_to_ship[future] = ship_id
            
            # Process completed tasks
            completed_count = 0
            failed_count = 0
            
            for future in as_completed(future_to_ship):
                ship_id = future_to_ship[future]
                completed_count += 1
                
                try:
                    result = future.result()
                    if result['success']:
                        logger.info(f"✅ [{completed_count}/{total_ships}] Completed processing for ship: {ship_id}")
                        logger.info(f"   📊 Records processed: {result.get('records_processed', 0)}")
                    else:
                        logger.error(f"❌ [{completed_count}/{total_ships}] Failed processing for ship: {ship_id}")
                        logger.error(f"   📊 Error: {result.get('error', 'Unknown error')}")
                        failed_count += 1
                        
                except Exception as e:
                    logger.error(f"❌ [{completed_count}/{total_ships}] Exception processing ship {ship_id}: {e}")
                    failed_count += 1
            
            logger.info(f"🎉 Completed processing new data batch")
            logger.info(f"   📊 Success: {completed_count - failed_count}/{total_ships}")
            logger.info(f"   📊 Failed: {failed_count}/{total_ships}")
            
        except Exception as e:
            logger.error(f"❌ Error processing new data: {e}")
    
    def _process_ship_data_safe(self, ship_id: str) -> Dict[str, Any]:
        """Thread-safe wrapper for ship data processing with thread info"""
        thread_logger = get_ship_thread_logger(ship_id, mode="realtime")
        
        thread_logger.info(f"🚢 Starting processing for ship: {ship_id}")
        
        try:
            start_time = time.time()
            self._process_ship_data(ship_id, thread_logger)
            end_time = time.time()
            
            processing_time = end_time - start_time
            thread_logger.success(f"Completed processing for ship: {ship_id} in {processing_time:.2f}s")
            
            return {
                'success': True,
                'ship_id': ship_id,
                'processing_time': processing_time,
                'records_processed': 0  # Could be enhanced to track actual count
            }
            
        except Exception as e:
            import traceback
            thread_logger.error(f"Error processing ship {ship_id}: {e}")
            thread_logger.error(f"Traceback: {traceback.format_exc()}")
            return {
                'success': False,
                'ship_id': ship_id,
                'error': str(e),
                'traceback': traceback.format_exc()
            }
    
    def _process_ship_data(self, ship_id: str, thread_logger=None):
        """Process new data for a specific ship (Multi-Table support)"""
        if thread_logger is None:
            thread_logger = get_ship_thread_logger(ship_id)
        
        # STEP 1: Ensure tables exist
        if self.use_multi_table:
            # Multi-Table mode: 3 tables
            thread_logger.debug(f"Ensuring 3 tables exist for {ship_id}")
            self._ensure_multi_tables_exist(ship_id, thread_logger)
        else:
            # Legacy mode: 1 table
            table_name = f'tbl_data_timeseries_{ship_id.upper()}'
            
            if not db_manager.check_table_exists(table_name):
                thread_logger.info(f"Table {table_name} does not exist, creating it")
                self._create_table_for_ship_legacy(ship_id, thread_logger)
                thread_logger.success(f"Table {table_name} created successfully")
            else:
                thread_logger.debug(f"Table {table_name} already exists")
        
        # STEP 2: Process data (only if data exists)
        # Get the last processed time for this ship (realtime-specific)
        last_processed_time = self._get_last_processed_time(ship_id)
        
        # Determine cutoff time
        if self.cutoff_time and (not last_processed_time or self.cutoff_time > last_processed_time):
            cutoff_time = self.cutoff_time
        elif last_processed_time:
            cutoff_time = last_processed_time
        else:
            # ✅ First time running realtime for this ship
            cutoff_time = datetime.now() - timedelta(minutes=5)
            thread_logger.warning(f"🚀 Realtime starting for first time, cutoff: {cutoff_time}")
            
            # ✅ Record realtime start time (this becomes the boundary for Batch)
            cutoff_time_manager.save_realtime_cutoff_time(ship_id, cutoff_time)
            thread_logger.info(f"📅 Recorded realtime start time for {ship_id}: {cutoff_time}")
            thread_logger.info(f"📊 Batch migration will stop at this time to avoid overlap")
        
        thread_logger.info(f"🔍 Processing data for {ship_id} since {cutoff_time}")
        
        # Get new data since cutoff time
        new_data = self._get_new_data(ship_id, cutoff_time, thread_logger)
        
        if not new_data:
            thread_logger.info(f"📊 No new data found for {ship_id} - table ready for future data")
            return
        
        thread_logger.info(f"Processing {len(new_data)} new records for ship_id: {ship_id}")
        
        # Process data in batches
        for batch in self._chunk_data(new_data, self.batch_size):
            if self.use_multi_table:
                # Multi-Table mode: process to 3 tables
                self._process_batch_multi_table(batch, ship_id, thread_logger)
            else:
                # Legacy mode: single table
                table_name = f'tbl_data_timeseries_{ship_id.upper()}'
                self._process_batch(batch, table_name, thread_logger)
        
        # Update last processed time to the latest record's timestamp
        if new_data:
            latest_timestamp = max(record['created_time'] for record in new_data)
            self._update_last_processed_time(ship_id, latest_timestamp)
            thread_logger.info(f"✅ Updated last processed time for {ship_id}: {latest_timestamp}")
    
    def _get_last_processed_time(self, ship_id: str) -> Optional[datetime]:
        """Get the last processed timestamp for a ship (Realtime-specific cutoff)"""
        # ✅ Use REALTIME-specific cutoff time file
        realtime_cutoff_time = cutoff_time_manager.load_realtime_cutoff_time(ship_id)
        if realtime_cutoff_time:
            logger.debug(f"Using realtime cutoff time for {ship_id}: {realtime_cutoff_time}")
            return realtime_cutoff_time
        
        # Legacy fallback (for backward compatibility)
        ship_cutoff_time = cutoff_time_manager.load_ship_cutoff_time(ship_id)
        if ship_cutoff_time:
            logger.debug(f"Using legacy ship cutoff time for {ship_id}: {ship_cutoff_time}")
            return ship_cutoff_time
        
        # Global fallback (for backward compatibility)
        global_cutoff_time = cutoff_time_manager.load_cutoff_time()
        if global_cutoff_time:
            logger.debug(f"Using global cutoff time for {ship_id}: {global_cutoff_time}")
            return global_cutoff_time
        
        # Try to get the latest timestamp from tables
        try:
            if self.use_multi_table:
                # Multi-Table: Check all 3 tables and get the latest
                table_names = [
                    f'tbl_data_timeseries_{ship_id.lower()}_1',
                    f'tbl_data_timeseries_{ship_id.lower()}_2',
                    f'tbl_data_timeseries_{ship_id.lower()}_3'
                ]
                
                latest_time = None
                for table_name in table_names:
                    if not db_manager.check_table_exists(table_name):
                        continue
                        
                    query = f"""
                    SELECT MAX(created_time) as latest_time
                    FROM tenant.{table_name}
                    """
                    result = db_manager.execute_query(query)
                    if result and result[0]['latest_time']:
                        table_time = result[0]['latest_time']
                        if latest_time is None or table_time > latest_time:
                            latest_time = table_time
                
                if latest_time:
                    logger.debug(f"Using latest timestamp from tables for {ship_id}: {latest_time}")
                    return latest_time
            else:
                # Legacy: Single table
                table_name = f'tbl_data_timeseries_{ship_id}'
                query = f"""
                SELECT MAX(created_time) as latest_time
                FROM tenant.{table_name}
                """
                result = db_manager.execute_query(query)
                if result and result[0]['latest_time']:
                    latest_time = result[0]['latest_time']
                    logger.debug(f"Using latest timestamp from table for {ship_id}: {latest_time}")
                    return latest_time
                    
        except Exception as e:
            logger.debug(f"Could not get last processed time for {ship_id}: {e}")
        
        return None
    
    def _update_last_processed_time(self, ship_id: str, timestamp: datetime):
        """Update the last processed timestamp for a ship (thread-safe)"""
        from cutoff_time_strategy import cutoff_strategy
        
        # Thread-safe cutoff time update - REALTIME-specific
        with self.cutoff_lock:
            # ✅ Save REALTIME-specific cutoff time only
            cutoff_time_manager.save_realtime_cutoff_time(ship_id, timestamp)
            
            # Mark the target minute as processed
            window = cutoff_strategy.get_processing_window()
            cutoff_strategy.mark_minute_processed(window['target_minute'])
            
            logger.debug(f"Updated REALTIME cutoff time to {timestamp} for ship {ship_id}")
            logger.debug(f"Marked minute {window['target_minute']} as processed")
    
    def _get_new_data(self, ship_id: str, cutoff_time: datetime, thread_logger=None) -> List[Dict[str, Any]]:
        """Get new data since cutoff time (optimized for minute-based batch processing)"""
        import time
        import threading
        from cutoff_time_strategy import cutoff_strategy
        
        if thread_logger is None:
            thread_logger = get_ship_thread_logger(ship_id)
        
        # Use migration cutoff time if not specified, otherwise use optimized strategy
        if cutoff_time:
            actual_cutoff = cutoff_time
        else:
            # Get optimal processing window
            window = cutoff_strategy.get_processing_window()
            actual_cutoff = cutoff_strategy.get_cutoff_time_for_query(window['target_minute'])
        
        thread_logger.info(f"🔍 Starting real-time data query for ship: {ship_id}")
        
        # Strategy: Use existing created_time DESC index by putting time condition first
        # No upper bound needed since data comes in batches every minute
        # This leverages the existing index more effectively
        
        query = """
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
        WHERE created_time >= %s
        AND ship_id = %s
        ORDER BY created_time ASC
        """
        
        start_time_query = time.time()
        thread_logger.info(f"🚀 Executing time-index-optimized query...")
        thread_logger.info(f"📊 Time range: created_time >= {actual_cutoff}")
        
        try:
            result = db_manager.execute_query(query, (actual_cutoff, ship_id))
            end_time_query = time.time()
            execution_time = end_time_query - start_time_query
            
            record_count = len(result) if result else 0
            thread_logger.success(f"Time-index-optimized query completed successfully!")
            thread_logger.info(f"📈 Execution time: {execution_time:.2f} seconds")
            thread_logger.info(f"📊 New records found: {record_count}")
            
            if execution_time > 3.0:
                thread_logger.warning(f"⚠️ Slow query detected: {execution_time:.2f}s execution time")
            
            # Volume information (no threshold limit)
            if self.use_multi_table and record_count > 0:
                thread_logger.info(f"📊 Volume: {record_count:,} records, distributed to 3 tables")
                thread_logger.debug(f"   Estimated per table: ~{record_count // 3:,} records")
            elif record_count > 0:
                thread_logger.info(f"📊 Volume: {record_count:,} records")
            
            return result
            
        except Exception as e:
            end_time_query = time.time()
            execution_time = end_time_query - start_time_query
            thread_logger.error(f"Time-index-optimized query failed after {execution_time:.2f} seconds: {e}")
            
            # Fallback to original query if optimized version fails
            thread_logger.info(f"🔄 Attempting fallback to original query...")
            try:
                fallback_query = """
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
                AND created_time >= %s
                ORDER BY created_time ASC
                LIMIT 1000
                """
                
                fallback_result = db_manager.execute_query(fallback_query, (ship_id, actual_cutoff))
                thread_logger.success(f"Fallback query succeeded: {len(fallback_result)} records")
                return fallback_result
                
            except Exception as fallback_error:
                thread_logger.error(f"Fallback query also failed: {fallback_error}")
                raise
    
    def _process_batch_multi_table(self, batch_data: List[Dict[str, Any]], ship_id: str, thread_logger=None):
        """Process a batch of data to 3 tables (Multi-Table mode)"""
        if not batch_data:
            thread_logger.warning("⚠️ _process_batch_multi_table: batch_data is empty")
            return
            
        if thread_logger is None:
            thread_logger = get_ship_thread_logger(ship_id)
        
        thread_logger.info(f"🔍 Processing {len(batch_data)} records → 3 tables")
        
        # Debug: Check how many channels are in channel_list
        unique_channels = set(row['data_channel_id'] for row in batch_data)
        all_allowed_channels = self.channel_router.get_all_channels()
        matched_channels = unique_channels & all_allowed_channels
        unmatched_channels = unique_channels - all_allowed_channels
        
        thread_logger.debug(f"   📊 Channels: {len(matched_channels)}/{len(unique_channels)} matched")
        
        if len(unmatched_channels) > 0:
            thread_logger.debug(f"      ⚠️ {len(unmatched_channels)} unmatched (will filter)")
            sample_unmatched = list(unmatched_channels)[:3]
            thread_logger.debug(f"      Sample: {sample_unmatched}")
        
        # Debug: Show sample raw data
        if len(batch_data) > 0:
            sample_raw = batch_data[0]
            thread_logger.debug(f"   🔍 Sample: {sample_raw}")
        
        # Group data by timestamp
        grouped_data = self._group_data_by_timestamp(batch_data)
        thread_logger.debug(f"   📊 Grouped into {len(grouped_data)} timestamps")
        
        # Prepare data for each table type
        table_data = {
            self.channel_router.TABLE_AUXILIARY: [],
            self.channel_router.TABLE_ENGINE: [],
            self.channel_router.TABLE_NAVIGATION: []
        }
        
        thread_logger.info(f"   🔍 Table types in use: {self.channel_router.get_all_table_types()}")
        
        # Log all timestamps before processing
        all_timestamps = sorted(grouped_data.keys())
        thread_logger.info(f"   📅 All timestamps in batch: {[ts.strftime('%H:%M:%S') for ts in all_timestamps]}")
        
        # Clean up old processed timestamps (keep only last 2 minutes)
        # This prevents memory buildup and allows reprocessing if needed
        cutoff_for_cleanup = datetime.now() - timedelta(minutes=2)
        old_count = len(self.processed_timestamps)
        self.processed_timestamps = {
            ts for ts in self.processed_timestamps 
            if ts > cutoff_for_cleanup
        }
        cleaned_count = old_count - len(self.processed_timestamps)
        if cleaned_count > 0:
            thread_logger.debug(f"   🧹 Cleaned {cleaned_count} old timestamps from cache (kept {len(self.processed_timestamps)})")
        
        for timestamp, channels in grouped_data.items():
            # Skip if already processed (within last 2 minutes)
            if timestamp in self.processed_timestamps:
                thread_logger.debug(f"   ⏭️ Cached timestamp: {timestamp}")
                
                # ✅ Verify it actually exists in DB (safety check)
                should_skip = True
                if self.use_multi_table:
                    sample_table = f"tbl_data_timeseries_{ship_id.lower()}_1"
                    check_query = f"SELECT COUNT(*) as cnt FROM tenant.{sample_table} WHERE created_time = %s"
                    check_result = db_manager.execute_query(check_query, (timestamp,))
                    if check_result and check_result[0]['cnt'] > 0:
                        thread_logger.debug(f"      ✅ In DB - safe to skip")
                    else:
                        thread_logger.warning(f"      ⚠️ NOT in DB - reprocessing {timestamp}")
                        self.processed_timestamps.remove(timestamp)
                        should_skip = False
                
                if should_skip:
                    continue
            
            thread_logger.debug(f"   🔄 Processing timestamp {timestamp}: {len(channels)} channels")
            
            # Debug: Show sample raw channels
            if len(channels) > 0:
                sample_raw = channels[0]
                thread_logger.debug(f"   🔍 Sample raw channel data: {sample_raw}")
            
            # Prepare row for each table type
            for table_type in self.channel_router.get_all_table_types():
                table_name = f"tbl_data_timeseries_{ship_id.lower()}_{table_type}"
                
                # Debug: Check channel routing for this table type (only in DEBUG mode)
                if len(channels) > 0:
                    for idx, ch in enumerate(channels[:3]):
                        ch_id = ch['data_channel_id']
                        detected_type = self.channel_router.get_table_type(ch_id)
                        match_status = "✅ MATCH" if detected_type == table_type else "❌ NO MATCH"
                        thread_logger.debug(f"      🔍 Channel[{idx}]: '{ch_id}' → {detected_type} | Expecting: {table_type} | {match_status}")
                
                # Filter channels for this table
                filtered_channels = [
                    ch for ch in channels 
                    if self.channel_router.get_table_type(ch['data_channel_id']) == table_type
                ]
                
                thread_logger.debug(f"      📊 Filtering: {len(channels)} total → {len(filtered_channels)} for table {table_type}")
                
                if not filtered_channels:
                    thread_logger.debug(f"      ⏭️ No channels matched table {table_type} at {timestamp}")
                    
                    # Additional debug: Show what types were detected
                    detected_types = {}
                    for ch in channels[:10]:
                        dt = self.channel_router.get_table_type(ch['data_channel_id'])
                        detected_types[dt] = detected_types.get(dt, 0) + 1
                    thread_logger.debug(f"         Types found in sample: {detected_types}")
                    continue
                
                thread_logger.debug(f"      ✅ {table_type}: {len(filtered_channels)} channels at {timestamp}")
                
                # Debug: Show sample channel IDs
                if len(filtered_channels) > 0:
                    sample_channels = [ch['data_channel_id'] for ch in filtered_channels[:3]]
                    thread_logger.debug(f"         Sample channels: {sample_channels}")
                
                # Prepare wide row
                row_data = self._prepare_wide_row_multi_table(
                    timestamp, filtered_channels, table_type, thread_logger
                )
                
                if row_data:
                    row_cols = len(row_data) - 1  # Exclude created_time
                    table_data[table_type].append(row_data)
                    thread_logger.debug(f"      ✅ Added row to {table_type} with {row_cols} data columns")
                else:
                    thread_logger.warning(f"      ⚠️ _prepare_wide_row_multi_table returned None for {table_type} ({len(filtered_channels)} channels)")
        
        # Summary before insertion (compact format)
        counts = {t: len(table_data[t]) for t in self.channel_router.get_all_table_types()}
        total_rows = sum(counts.values())
        thread_logger.info(f"   📊 Prepared {total_rows} rows: T1={counts['1']}, T2={counts['2']}, T3={counts['3']}")
        
        # Track successfully inserted timestamps
        successfully_inserted = False
        has_data_to_insert = False
        
        # Insert data into each table
        try:
            insert_summary = []
            for table_type in self.channel_router.get_all_table_types():
                if table_data[table_type]:
                    has_data_to_insert = True
                    table_name = f"tbl_data_timeseries_{ship_id.lower()}_{table_type}"
                    row_count = len(table_data[table_type])
                    self._insert_batch_data(table_data[table_type], table_name, thread_logger)
                    insert_summary.append(f"T{table_type}:{row_count}")
                else:
                    thread_logger.debug(f"   ⏭️ No data to insert into {table_type}")
            
            # Compact INSERT summary
            if insert_summary:
                thread_logger.info(f"   💾 Inserted: {', '.join(insert_summary)}")
            
            # ✅ All inserts successful (or no data to insert)
            successfully_inserted = True
            
        except Exception as e:
            thread_logger.error(f"❌ INSERT failed: {e}")
            thread_logger.error(f"   Timestamps will NOT be marked as processed (will retry next time)")
            raise
        
        finally:
            # Only add to processed_timestamps if there was data AND all inserts succeeded
            if successfully_inserted and has_data_to_insert:
                for timestamp in grouped_data.keys():
                    self.processed_timestamps.add(timestamp)
                thread_logger.debug(f"   ✅ Cached {len(grouped_data)} timestamps (cache size: {len(self.processed_timestamps)})")
            elif successfully_inserted and not has_data_to_insert:
                # No data to insert - don't mark as processed (will check again next time)
                thread_logger.debug(f"   ⏭️ No data inserted, timestamps NOT cached")
    
    def _process_batch(self, batch_data: List[Dict[str, Any]], table_name: str, thread_logger=None):
        """Process a batch of data (Legacy Single-Table mode)"""
        if not batch_data:
            return
            
        if thread_logger is None:
            # Extract ship_id from table_name for logging
            ship_id = table_name.replace('tbl_data_timeseries_', '').upper()
            thread_logger = get_ship_thread_logger(ship_id)
        
        # Debug: Log batch data structure
        thread_logger.debug(f"🔍 Processing batch with {len(batch_data)} records")
        thread_logger.debug(f"🔍 First record structure: {batch_data[0]}")
        thread_logger.debug(f"🔍 First record created_time: {batch_data[0]['created_time']} (type: {type(batch_data[0]['created_time'])})")
        
        # Group data by timestamp
        grouped_data = self._group_data_by_timestamp(batch_data)
        
        # Debug: Log grouped data
        thread_logger.debug(f"🔍 Grouped data keys: {list(grouped_data.keys())}")
        thread_logger.debug(f"🔍 First timestamp: {list(grouped_data.keys())[0]} (type: {type(list(grouped_data.keys())[0])})")
        
        # Prepare and insert data
        insert_data = []
        for timestamp, channels in grouped_data.items():
            # Skip if already processed
            if timestamp in self.processed_timestamps:
                continue
            
            thread_logger.debug(f"🔍 Preparing row for timestamp: {timestamp} (type: {type(timestamp)})")
            row_data = self._prepare_wide_row(timestamp, channels, table_name, thread_logger)
            if row_data:
                insert_data.append(row_data)
                self.processed_timestamps.add(timestamp)
        
        if not insert_data:
            thread_logger.warning("⚠️ No data to insert after processing")
            return
        
        thread_logger.debug(f"🔍 Final insert_data count: {len(insert_data)}")
        thread_logger.debug(f"🔍 First insert_data: {insert_data[0]}")
        
        # Execute batch insert with conflict resolution
        self._insert_batch_data(insert_data, table_name, thread_logger)
    
    def _group_data_by_timestamp(self, batch_data: List[Dict[str, Any]]) -> Dict[datetime, List[Dict[str, Any]]]:
        """Group data by created_time"""
        grouped = {}
        
        for row in batch_data:
            timestamp = row['created_time']
            if timestamp not in grouped:
                grouped[timestamp] = []
            grouped[timestamp].append(row)
        
        return grouped
    
    def _prepare_wide_row_multi_table(self, timestamp: datetime, channels: List[Dict[str, Any]], table_type: str, thread_logger=None) -> Optional[Dict[str, Any]]:
        """Prepare a single row for multi-table insertion"""
        if thread_logger is None:
            thread_logger = logger
        
        thread_logger.debug(f"🔍 Preparing wide row for {table_type} at {timestamp} with {len(channels)} channels")
        
        # Debug: Show sample channel
        if len(channels) > 0:
            sample = channels[0]
            thread_logger.debug(f"   Sample channel: {sample.get('data_channel_id', 'N/A')}, format: {sample.get('value_format', 'N/A')}")
        
        # Start with created_time
        row_data = {'created_time': timestamp}
        
        processed_channels = 0
        skipped_channels = 0
        
        # Add data for each channel
        for channel_data in channels:
            channel_id = channel_data.get('data_channel_id')
            if not channel_id:
                thread_logger.warning(f"   ⚠️ Channel data missing 'data_channel_id': {channel_data}")
                skipped_channels += 1
                continue
            
            value_format = channel_data.get('value_format')
            
            # Convert channel to column name
            col_name = self._channel_to_column_name(channel_id)
            
            # Get the appropriate value based on format
            value = self._get_value_by_format(channel_data, value_format)
            
            # Store value (convert to double)
            if value is not None:
                try:
                    row_data[col_name] = float(value)
                    processed_channels += 1
                except (ValueError, TypeError):
                    row_data[col_name] = None
                    thread_logger.debug(f"   ⚠️ Failed to convert value to float: {value}")
            else:
                row_data[col_name] = None
        
        thread_logger.debug(f"   📊 Processed: {processed_channels}, Skipped: {skipped_channels}, Total columns: {len(row_data)}")
        
        if processed_channels == 0:
            thread_logger.warning(f"   ⚠️ No data processed for {table_type}! All {len(channels)} channels resulted in None/errors")
        
        return row_data if processed_channels > 0 else None
    
    def _channel_to_column_name(self, channel: str) -> str:
        """Convert channel ID to column name (use as-is, no transformation)"""
        # 채널 ID를 그대로 컬럼명으로 사용 (변환 없음)
        return channel
    
    def _get_table_columns(self, table_name: str) -> Set[str]:
        """Get table columns with thread-safe caching"""
        # Convert table name to lowercase to match PostgreSQL behavior
        table_name_lower = table_name.lower()
        
        # Thread-safe cache access
        with self.cache_lock:
            if table_name_lower not in self.table_columns_cache:
                if self.use_multi_table:
                    # Multi-table mode: get columns from information_schema
                    query = """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'tenant'
                    AND table_name = %s
                    ORDER BY ordinal_position
                    """
                    result = db_manager.execute_query(query, (table_name_lower,))
                    existing_columns_list = [row['column_name'] for row in result] if result else []
                else:
                    # Legacy mode: use table_generator
                    existing_columns_list = table_generator.get_table_columns(table_name_lower)
                
                self.table_columns_cache[table_name_lower] = set(existing_columns_list)
                logger.debug(f"📋 Cached {len(existing_columns_list)} columns for {table_name_lower}")
            
            return self.table_columns_cache[table_name_lower].copy()  # Return a copy to avoid race conditions
    
    def _prepare_wide_row(self, timestamp: datetime, channels: List[Dict[str, Any]], table_name: str, thread_logger=None) -> Optional[Dict[str, Any]]:
        """Prepare a single row for wide table insertion"""
        if thread_logger is None:
            # Extract ship_id from table_name for logging
            ship_id = table_name.replace('tbl_data_timeseries_', '').upper()
            thread_logger = get_ship_thread_logger(ship_id)
        
        thread_logger.debug(f"🔍 Preparing wide row for {table_name} at {timestamp}")
        
        # Get existing columns for the table (with caching)
        existing_columns = self._get_table_columns(table_name)
        
        # Start with created_time
        row_data = {'created_time': timestamp}
        
        processed_channels = 0
        skipped_not_allowed = 0
        skipped_not_in_table = 0
        
        # Add data for each channel (only if column is in allowed list)
        for channel_data in channels:
            channel_id = channel_data['data_channel_id']
            value_format = channel_data['value_format']
            
            # Only process columns that are in the allowed list
            if channel_id not in self.allowed_columns:
                skipped_not_allowed += 1
                continue
            
            # Check if column exists in table, skip if not (don't add new columns)
            if channel_id not in existing_columns:
                thread_logger.debug(f"🔍 Skipping column {channel_id} - not in table {table_name}")
                skipped_not_in_table += 1
                continue
            
            # Get the appropriate value based on format
            value = self._get_value_by_format(channel_data, value_format)
            
            # Convert to text as per requirements, but handle None properly
            if value is not None:
                row_data[channel_id] = str(value)
            else:
                row_data[channel_id] = None  # Keep as None, not "None" string
            
            processed_channels += 1
        
        thread_logger.info(f"📊 Processing summary:")
        thread_logger.info(f"   📊 Processed channels: {processed_channels}")
        thread_logger.info(f"   📊 Skipped (not allowed): {skipped_not_allowed}")
        thread_logger.info(f"   📊 Skipped (not in table): {skipped_not_in_table}")
        thread_logger.debug(f"   📊 Final row columns: {len(row_data)}")
        
        # Debug: Log the prepared row data
        thread_logger.debug(f"🔍 Prepared row data: {row_data}")
        thread_logger.debug(f"🔍 Row data keys: {list(row_data.keys())}")
        thread_logger.debug(f"🔍 Row data values: {list(row_data.values())}")
        
        return row_data
    
    def _get_value_by_format(self, row_data: Dict[str, Any], value_format: str) -> Any:
        """Get value based on value_format"""
        if not value_format or value_format not in self.value_format_mapping:
            return None
        
        column_name = self.value_format_mapping[value_format]
        return row_data.get(column_name)
    
    def _insert_batch_data(self, insert_data: List[Dict[str, Any]], table_name: str, thread_logger=None):
        """Insert batch data with conflict resolution"""
        if not insert_data:
            return
            
        if thread_logger is None:
            # Extract ship_id from table_name for logging
            ship_id = table_name.replace('tbl_data_timeseries_', '').upper()
            thread_logger = get_ship_thread_logger(ship_id)
        
        # Debug: Log the first row structure
        thread_logger.debug(f"🔍 First row data structure: {insert_data[0]}")
        thread_logger.debug(f"🔍 First row keys: {list(insert_data[0].keys())}")
        thread_logger.debug(f"🔍 First row values: {list(insert_data[0].values())}")
        
        # Generate INSERT SQL with ON CONFLICT
        columns = list(insert_data[0].keys())
        
        # Ensure created_time is always first
        if 'created_time' in columns:
            columns.remove('created_time')
            columns.insert(0, 'created_time')
        
        # Quote column names if they contain special characters
        special_chars = ['/', '-', ' ', '.', '(', ')', '[', ']', '{', '}', '@', '#', '$', '%', '^', '&', '*', '+', '=', '|', '\\', ':', ';', '"', "'", '<', '>', ',', '?', '!', '~', '`']
        quoted_columns = [f'"{col}"' if any(c in col for c in special_chars) else col for col in columns]
        
        columns_str = ', '.join(quoted_columns)
        placeholders = ', '.join(['%s'] * len(columns))
        
        # Create update clause for non-primary key columns
        update_clauses = []
        for col in columns:
            if col != 'created_time':
                quoted_col = f'"{col}"' if any(c in col for c in special_chars) else col
                update_clauses.append(f'{quoted_col} = EXCLUDED.{quoted_col}')
        
        # Check if we have any columns to update
        if not update_clauses:
            thread_logger.warning(f"No columns to update for {table_name}, using simple INSERT")
            insert_sql = f"""
            INSERT INTO tenant.{table_name} ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT (created_time) DO NOTHING
            """
        else:
            update_clause = ', '.join(update_clauses)
            insert_sql = f"""
            INSERT INTO tenant.{table_name} ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT (created_time) DO UPDATE SET
            {update_clause}
            """
        
        try:
            # Debug: Log the actual values being inserted
            first_row_values = [insert_data[0].get(col) for col in columns]
            thread_logger.debug(f"🔍 First row values for insertion: {first_row_values}")
            thread_logger.debug(f"🔍 Columns order: {columns}")
            
            # Prepare data in correct column order
            values_list = []
            for row in insert_data:
                row_values = [row.get(col) for col in columns]
                values_list.append(tuple(row_values))
            
            affected_rows = db_manager.execute_batch(insert_sql, values_list)
            
            # 📊 상세한 로그 정보
            data_columns = len(columns) - 1  # created_time 제외
            time_range = f"{min(row['created_time'] for row in insert_data)} ~ {max(row['created_time'] for row in insert_data)}"
            
            thread_logger.success(f"REALTIME INSERT SUCCESS: {table_name}")
            thread_logger.info(f"   📊 Records: {len(insert_data)} rows inserted")
            thread_logger.info(f"   📊 Columns: {data_columns} data columns (total: {len(columns)})")
            thread_logger.info(f"   📊 Time Range: {time_range}")
            thread_logger.info(f"   📊 Affected Rows: {affected_rows}")
            
            # Status tracking log for check_status.sh
            # Extract ship_id from table_name (format: tbl_data_timeseries_SHIPID)
            ship_id = table_name.split('_')[-1].lower()
            thread_logger.info(f"STATUS:REALTIME:{ship_id}:{len(insert_data)}:{data_columns}:{time_range}:{affected_rows}")
            
        except Exception as e:
            thread_logger.error(f"REALTIME INSERT FAILED: {table_name}")
            thread_logger.error(f"   📊 Records: {len(insert_data)} rows failed")
            thread_logger.error(f"   📊 Columns: {len(columns)} columns")
            thread_logger.error(f"   📊 Error: {e}")
            thread_logger.error(f"SQL: {insert_sql}")
            raise
    
    def _ensure_multi_tables_exist(self, ship_id: str, thread_logger=None):
        """Ensure all 3 tables exist for ship (Multi-Table mode)"""
        if thread_logger is None:
            thread_logger = get_ship_thread_logger(ship_id)
        
        try:
            # Use multi_table_generator to create all tables (quietly if already exist)
            success = self.table_generator.ensure_all_tables_exist(ship_id)
            
            if not success:
                thread_logger.error(f"Failed to create tables for {ship_id}")
                raise RuntimeError(f"Table creation failed for {ship_id}")
            
            # Tables ready (log message handled by multi_table_generator)
                
        except Exception as e:
            thread_logger.error(f"Error ensuring multi-tables for {ship_id}: {e}")
            raise
    
    def _create_table_for_ship_legacy(self, ship_id: str, thread_logger=None):
        """Create table for ship if it doesn't exist or has wrong column count (Legacy mode)"""
        if thread_logger is None:
            thread_logger = get_ship_thread_logger(ship_id)
            
        table_name = f'tbl_data_timeseries_{ship_id.upper()}'
        
        try:
            # Check if table exists and has correct column count
            should_recreate = False
            
            if db_manager.check_table_exists(table_name):
                # Get current column count
                query = '''
                SELECT COUNT(*) as column_count
                FROM information_schema.columns 
                WHERE table_schema = 'tenant' 
                AND table_name = %s;
                '''
                
                result = db_manager.execute_query(query, (table_name.lower(),))
                current_columns = result[0]['column_count'] if result else 0
                expected_columns = len(self.allowed_columns) + 1  # +1 for created_time
                
                if current_columns != expected_columns:
                    thread_logger.warning(f"Table {table_name} has {current_columns} columns, expected {expected_columns}")
                    thread_logger.info(f"Recreating table with correct column count")
                    should_recreate = True
                else:
                    thread_logger.debug(f"Table {table_name} already exists with correct column count ({current_columns})")
            else:
                thread_logger.info(f"Table {table_name} does not exist, creating it")
                should_recreate = True
            
            if should_recreate:
                # Create schema with all allowed columns from column_list.txt
                schema = self._create_realtime_schema(ship_id)
                
                # Generate table - force recreation if needed
                success = table_generator.generate_table(ship_id, schema, drop_if_exists=should_recreate)
                
                if success:
                    thread_logger.success(f"Created/updated table for ship_id: {ship_id} with {len(schema['columns'])} columns")
                else:
                    thread_logger.error(f"Failed to create table for ship_id: {ship_id}")
            else:
                thread_logger.success(f"Table {table_name} is ready with correct column count")
                
        except Exception as e:
            thread_logger.error(f"Error creating table for ship_id {ship_id}: {e}")
            raise
    
    def _create_realtime_schema(self, ship_id: str) -> Dict[str, Any]:
        """Create schema for real-time processing with all allowed columns"""
        table_name = f'tbl_data_timeseries_{ship_id.upper()}'
        
        # Start with created_time column
        columns = [
            {
                'name': 'created_time',
                'type': 'timestamp',
                'nullable': False,
                'description': 'Primary key - timestamp of data collection'
            }
        ]
        
        # Add all allowed columns from column_list.txt
        for column_name in self.allowed_columns:
            columns.append({
                'name': column_name,
                'type': 'text',
                'nullable': True,
                'description': f'Data channel: {column_name}'
            })
        
        schema = {
            'ship_id': ship_id,
            'table_name': table_name,
            'columns': columns,
            'primary_key': 'created_time',
            'indexes': ['created_time'],
            'sample_count': 0,
            'data_channels': list(self.allowed_columns)
        }
        
        logger.info(f"Created real-time schema for {ship_id}: {len(columns)} columns (1 + {len(self.allowed_columns)} data channels)")
        return schema
    
    def _chunk_data(self, data: List[Any], chunk_size: int) -> List[List[Any]]:
        """Split data into chunks"""
        for i in range(0, len(data), chunk_size):
            yield data[i:i + chunk_size]
    
    def process_single_record(self, record: Dict[str, Any]) -> bool:
        """
        Process a single record immediately
        
        Args:
            record: Single data record
            
        Returns:
            True if successful, False otherwise
        """
        ship_id = record['ship_id']
        table_name = f'tbl_data_timeseries_{ship_id.upper()}'  # Force uppercase for consistency
        
        try:
            # Ensure table exists
            if not db_manager.check_table_exists(table_name):
                self._create_table_for_ship(ship_id)
            
            # Prepare row data
            row_data = self._prepare_wide_row(
                record['created_time'], 
                [record], 
                table_name
            )
            
            if not row_data:
                return False
            
            # Insert single record
            self._insert_batch_data([row_data], table_name)
            
            logger.info(f"Processed single record for ship_id: {ship_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to process single record for ship_id {ship_id}: {e}")
            return False
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """Get real-time processing statistics"""
        return {
            'running': self.running,
            'processed_timestamps': len(self.processed_timestamps),
            'last_processed': max(self.processed_timestamps) if self.processed_timestamps else None,
            'scheduled_jobs': len(schedule.jobs)
        }


    def get_data_channels_for_ship(self, ship_id: str) -> List[str]:
        """Get data channels for a specific ship"""
        return db_manager.get_data_channels_for_ship(ship_id)


# Global real-time processor instance
realtime_processor = RealTimeProcessor()

