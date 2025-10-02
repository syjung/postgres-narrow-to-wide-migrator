"""
Real-time data processing module
"""
import asyncio
import schedule
import time
from typing import Dict, List, Any, Optional, Set
from datetime import datetime, timedelta
from loguru import logger
from database import db_manager
from table_generator import table_generator
from schema_analyzer import schema_analyzer
from config import migration_config
from cutoff_time_manager import cutoff_time_manager


class RealTimeProcessor:
    """Handles real-time data processing for wide tables"""
    
    def __init__(self):
        self.batch_size = migration_config.batch_size
        self.value_format_mapping = migration_config.VALUE_FORMAT_MAPPING
        self.processed_timestamps: Set[datetime] = set()
        self.running = False
        self.cutoff_time: Optional[datetime] = None  # 마이그레이션 완료 시점
        self.dual_write_mode = migration_config.dual_write_mode
        
        # Load allowed columns from column_list.txt
        self.allowed_columns = self._load_allowed_columns()
    
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
        Start real-time data processing
        
        Args:
            interval_minutes: Processing interval in minutes
        """
        logger.info(f"Starting real-time processing with {interval_minutes} minute intervals")
        
        # Load cutoff time from file if not set
        if not self.cutoff_time:
            self.load_cutoff_time()
        
        # Schedule processing job
        schedule.every(interval_minutes).minutes.do(self._process_new_data)
        
        self.running = True
        
        # Start processing loop
        while self.running:
            try:
                schedule.run_pending()
                time.sleep(10)  # Check every 10 seconds
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
    
    def _process_new_data(self):
        """Process new data for all target ships"""
        logger.info("🔄 Processing new data batch")
        
        try:
            # Get target ship IDs from configuration
            ship_ids = db_manager.get_distinct_ship_ids()
            total_ships = len(ship_ids)
            
            logger.info(f"📊 Processing data for {total_ships} ships")
            
            for index, ship_id in enumerate(ship_ids, 1):
                try:
                    logger.info(f"🚢 [{index}/{total_ships}] Processing data for ship: {ship_id}")
                    self._process_ship_data(ship_id)
                    logger.info(f"✅ [{index}/{total_ships}] Completed processing for ship: {ship_id}")
                    
                except Exception as e:
                    logger.error(f"❌ [{index}/{total_ships}] Failed to process data for ship_id {ship_id}: {e}")
            
            logger.info("🎉 Completed processing new data batch")
            
        except Exception as e:
            logger.error(f"❌ Error processing new data: {e}")
    
    def _process_ship_data(self, ship_id: str):
        """Process new data for a specific ship"""
        table_name = f'tbl_data_timeseries_{ship_id}'
        
        # Check if table exists, create if not
        if not db_manager.check_table_exists(table_name):
            logger.warning(f"Table {table_name} does not exist, creating it")
            self._create_table_for_ship(ship_id)
        
        # Get new data (last 2 minutes to ensure we don't miss anything)
        cutoff_time = datetime.now() - timedelta(minutes=2)
        new_data = self._get_new_data(ship_id, cutoff_time)
        
        if not new_data:
            return
        
        logger.info(f"Processing {len(new_data)} new records for ship_id: {ship_id}")
        
        # Process data in batches
        for batch in self._chunk_data(new_data, self.batch_size):
            self._process_batch(batch, table_name)
    
    def _get_new_data(self, ship_id: str, cutoff_time: datetime) -> List[Dict[str, Any]]:
        """Get new data since cutoff time"""
        import time
        
        # Use migration cutoff time if not specified
        actual_cutoff = cutoff_time if cutoff_time else self.cutoff_time
        
        if not actual_cutoff:
            logger.warning("No cutoff time set, using default 2 minutes ago")
            actual_cutoff = datetime.now() - timedelta(minutes=2)
        
        logger.info(f"🔍 Starting real-time data query for ship: {ship_id}")
        logger.info(f"📊 Query: SELECT from tenant.tbl_data_timeseries WHERE ship_id={ship_id} AND created_time > {actual_cutoff}")
        
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
        WHERE ship_id = %s 
        AND created_time > %s
        ORDER BY created_time
        """
        
        start_time_query = time.time()
        logger.info(f"🚀 Executing real-time data query (this may take time)...")
        
        try:
            result = db_manager.execute_query(query, (ship_id, actual_cutoff))
            end_time_query = time.time()
            execution_time = end_time_query - start_time_query
            
            record_count = len(result) if result else 0
            logger.info(f"✅ Real-time query completed successfully!")
            logger.info(f"📈 Execution time: {execution_time:.2f} seconds")
            logger.info(f"📊 New records found: {record_count}")
            
            if execution_time > 3.0:
                logger.warning(f"⚠️ Slow real-time query detected: {execution_time:.2f}s execution time")
            
            return result
            
        except Exception as e:
            end_time_query = time.time()
            execution_time = end_time_query - start_time_query
            logger.error(f"❌ Real-time query failed after {execution_time:.2f} seconds: {e}")
            raise
    
    def _process_batch(self, batch_data: List[Dict[str, Any]], table_name: str):
        """Process a batch of data"""
        if not batch_data:
            return
        
        # Group data by timestamp
        grouped_data = self._group_data_by_timestamp(batch_data)
        
        # Prepare and insert data
        insert_data = []
        for timestamp, channels in grouped_data.items():
            # Skip if already processed
            if timestamp in self.processed_timestamps:
                continue
            
            row_data = self._prepare_wide_row(timestamp, channels, table_name)
            if row_data:
                insert_data.append(row_data)
                self.processed_timestamps.add(timestamp)
        
        if not insert_data:
            return
        
        # Execute batch insert with conflict resolution
        self._insert_batch_data(insert_data, table_name)
    
    def _group_data_by_timestamp(self, batch_data: List[Dict[str, Any]]) -> Dict[datetime, List[Dict[str, Any]]]:
        """Group data by created_time"""
        grouped = {}
        
        for row in batch_data:
            timestamp = row['created_time']
            if timestamp not in grouped:
                grouped[timestamp] = []
            grouped[timestamp].append(row)
        
        return grouped
    
    def _prepare_wide_row(self, timestamp: datetime, channels: List[Dict[str, Any]], table_name: str) -> Optional[Dict[str, Any]]:
        """Prepare a single row for wide table insertion"""
        # Get existing columns for the table
        existing_columns = table_generator.get_table_columns(table_name)
        
        # Start with created_time
        row_data = {'created_time': timestamp}
        
        # Add data for each channel (only if column is in allowed list)
        for channel_data in channels:
            channel_id = channel_data['data_channel_id']
            value_format = channel_data['value_format']
            
            # Only process columns that are in the allowed list
            if channel_id not in self.allowed_columns:
                logger.debug(f"⚠️ Skipping column {channel_id} - not in allowed columns list")
                continue
            
            # Check if column exists in table, skip if not (don't add new columns)
            if channel_id not in existing_columns:
                logger.warning(f"⚠️ Skipping column {channel_id} - not in table {table_name}")
                continue
            
            # Get the appropriate value based on format
            value = self._get_value_by_format(channel_data, value_format)
            
            # Convert to text as per requirements
            row_data[channel_id] = str(value) if value is not None else None
        
        return row_data
    
    def _get_value_by_format(self, row_data: Dict[str, Any], value_format: str) -> Any:
        """Get value based on value_format"""
        if not value_format or value_format not in self.value_format_mapping:
            return None
        
        column_name = self.value_format_mapping[value_format]
        return row_data.get(column_name)
    
    def _insert_batch_data(self, insert_data: List[Dict[str, Any]], table_name: str):
        """Insert batch data with conflict resolution"""
        if not insert_data:
            return
        
        # Generate INSERT SQL with ON CONFLICT
        columns = list(insert_data[0].keys())
        columns_str = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))
        
        # Create update clause for non-primary key columns
        update_clause = ', '.join([
            f'{col} = EXCLUDED.{col}' 
            for col in columns 
            if col != 'created_time'
        ])
        
        insert_sql = f"""
        INSERT INTO tenant.{table_name} ({columns_str})
        VALUES ({placeholders})
        ON CONFLICT (created_time) DO UPDATE SET
        {update_clause}
        """
        
        try:
            affected_rows = db_manager.execute_batch(
                insert_sql, 
                [tuple(row.values()) for row in insert_data]
            )
            logger.info(f"Inserted {affected_rows} records into {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to insert batch data into {table_name}: {e}")
            raise
    
    def _create_table_for_ship(self, ship_id: str):
        """Create table for ship if it doesn't exist"""
        try:
            # Analyze schema for this ship
            schema = schema_analyzer.analyze_ship_data(ship_id, sample_minutes=5)
            
            # Generate table
            success = table_generator.generate_table(schema, drop_if_exists=False)
            
            if success:
                logger.info(f"Created table for ship_id: {ship_id}")
            else:
                logger.error(f"Failed to create table for ship_id: {ship_id}")
                
        except Exception as e:
            logger.error(f"Error creating table for ship_id {ship_id}: {e}")
    
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
        table_name = f'tbl_data_timeseries_{ship_id}'
        
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

