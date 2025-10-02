"""
Chunked migration strategy for large-scale data processing
"""
import tempfile
import os
from typing import Dict, List, Any, Optional, Generator, Tuple
from datetime import datetime, timedelta
from loguru import logger
from database import db_manager
from config import migration_config


class ChunkedMigrationStrategy:
    """Chunked migration strategy for large-scale data"""
    
    def __init__(self):
        self.chunk_size_hours = 24  # 24시간 단위로 청킹
        self.max_chunk_records = 1000000  # 청크당 최대 레코드 수
        self.batch_size = migration_config.batch_size
    
    def get_data_chunks(self, ship_id: str, cutoff_time: Optional[datetime] = None) -> Generator[Tuple[datetime, datetime], None, None]:
        """
        Generate time-based chunks for migration
        
        Args:
            ship_id: Ship identifier
            cutoff_time: Only migrate data before this time
            
        Yields:
            Tuple of (start_time, end_time) for each chunk
        """
        logger.info(f"Generating data chunks for ship_id: {ship_id}")
        
        # Get data time range
        time_range = self._get_data_time_range(ship_id, cutoff_time)
        if not time_range:
            logger.warning(f"No data found for ship_id: {ship_id}")
            return
        
        start_time, end_time = time_range
        logger.info(f"Data time range: {start_time} to {end_time}")
        
        # Generate chunks
        current_start = start_time
        chunk_count = 0
        
        while current_start < end_time:
            current_end = min(
                current_start + timedelta(hours=self.chunk_size_hours),
                end_time
            )
            
            # Check if chunk has data
            if self._chunk_has_data(ship_id, current_start, current_end):
                chunk_count += 1
                logger.info(f"Chunk {chunk_count}: {current_start} to {current_end}")
                yield (current_start, current_end)
            
            current_start = current_end
        
        logger.info(f"Generated {chunk_count} chunks for ship_id: {ship_id}")
    
    def _get_data_time_range(self, ship_id: str, cutoff_time: Optional[datetime] = None) -> Optional[Tuple[datetime, datetime]]:
        """Get the time range of data for a ship"""
        query = """
        SELECT 
            MIN(created_time) as min_time,
            MAX(created_time) as max_time,
            COUNT(*) as total_count
        FROM tenant.tbl_data_timeseries 
        WHERE ship_id = %s
        """
        params = [ship_id]
        
        if cutoff_time:
            query += " AND created_time < %s"
            params.append(cutoff_time)
        
        result = db_manager.execute_query(query, tuple(params))
        
        if result and result[0]['total_count'] > 0:
            return (result[0]['min_time'], result[0]['max_time'])
        
        return None
    
    def _chunk_has_data(self, ship_id: str, start_time: datetime, end_time: datetime) -> bool:
        """Check if a time chunk has data"""
        query = """
        SELECT COUNT(*) as count
        FROM tenant.tbl_data_timeseries 
        WHERE ship_id = %s 
        AND created_time >= %s 
        AND created_time < %s
        """
        
        result = db_manager.execute_query(query, (ship_id, start_time, end_time))
        return result[0]['count'] > 0 if result else False
    
    def migrate_chunk(self, ship_id: str, start_time: datetime, end_time: datetime, 
                     table_name: str) -> Dict[str, Any]:
        """
        Migrate a single chunk of data
        
        Args:
            ship_id: Ship identifier
            start_time: Chunk start time
            end_time: Chunk end time
            table_name: Target table name
            
        Returns:
            Migration result dictionary
        """
        logger.info(f"🚀 Starting chunk migration: {ship_id} [{start_time} to {end_time}]")
        
        try:
            # Step 1: Extract chunk data
            logger.info(f"📊 Extracting data for chunk: {start_time} to {end_time}")
            chunk_data = self._extract_chunk_data(ship_id, start_time, end_time)
            
            if not chunk_data:
                logger.warning(f"⚠️ No data found in chunk: {start_time} to {end_time}")
                return {
                    'status': 'skipped',
                    'records_processed': 0,
                    'message': 'No data in chunk'
                }
            
            logger.info(f"📈 Extracted {len(chunk_data)} records from chunk")
            
            # Step 2: Transform to wide format
            logger.info(f"🔄 Transforming data to wide format...")
            wide_data = self._transform_chunk_to_wide(chunk_data)
            
            logger.info(f"✅ Transformed to {len(wide_data)} wide records")
            
            # Step 3: Insert to target table
            logger.info(f"💾 Inserting data in batches of {self.batch_size}...")
            inserted_count = self._insert_chunk_data(table_name, wide_data)
            
            logger.info(f"🎉 Chunk migration completed: {inserted_count} records inserted")
            
            return {
                'status': 'completed',
                'records_processed': inserted_count,
                'chunk_start': start_time.isoformat(),
                'chunk_end': end_time.isoformat(),
                'message': f'Chunk migrated successfully: {inserted_count} records'
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to migrate chunk {start_time} to {end_time}: {e}")
            return {
                'status': 'failed',
                'records_processed': 0,
                'chunk_start': start_time.isoformat(),
                'chunk_end': end_time.isoformat(),
                'error': str(e)
            }
    
    def _extract_chunk_data(self, ship_id: str, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        """Extract data for a specific chunk"""
        query = """
        SELECT 
            created_time,
            data_channel_id,
            COALESCE(
                CASE WHEN value_format = 'Decimal' THEN double_v::text END,
                CASE WHEN value_format = 'Integer' THEN long_v::text END,
                CASE WHEN value_format = 'String' THEN str_v END,
                CASE WHEN value_format = 'Boolean' THEN bool_v::text END
            ) as value
        FROM tenant.tbl_data_timeseries 
        WHERE ship_id = %s 
        AND created_time >= %s 
        AND created_time < %s
        ORDER BY created_time
        LIMIT %s
        """
        
        return db_manager.execute_query(query, (ship_id, start_time, end_time, self.max_chunk_records))
    
    def _transform_chunk_to_wide(self, chunk_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform chunk data to wide format"""
        # Group by timestamp
        grouped_data = {}
        for row in chunk_data:
            timestamp = row['created_time']
            if timestamp not in grouped_data:
                grouped_data[timestamp] = {}
            
            channel_id = row['data_channel_id']
            grouped_data[timestamp][channel_id] = row['value']
        
        # Convert to wide format
        wide_data = []
        for timestamp, channels in grouped_data.items():
            wide_row = {'created_time': timestamp}
            wide_row.update(channels)
            wide_data.append(wide_row)
        
        return wide_data
    
    def _insert_chunk_data(self, table_name: str, wide_data: List[Dict[str, Any]]) -> int:
        """Insert chunk data using batch processing"""
        if not wide_data:
            logger.warning("⚠️ No data to insert")
            return 0
        
        logger.info(f"📦 Inserting {len(wide_data)} records into {table_name}")
        
        # Process in batches
        total_inserted = 0
        batch_count = 0
        
        for i in range(0, len(wide_data), self.batch_size):
            batch_count += 1
            batch = wide_data[i:i + self.batch_size]
            
            logger.info(f"🔄 Processing batch {batch_count}: {len(batch)} records")
            
            try:
                inserted = self._insert_batch(table_name, batch)
                total_inserted += inserted
                
                logger.info(f"✅ Batch {batch_count} completed: {inserted} records inserted")
                
            except Exception as e:
                logger.error(f"❌ Batch {batch_count} failed: {e}")
                raise
        
        logger.info(f"🎯 Total inserted: {total_inserted} records in {batch_count} batches")
        return total_inserted
    
    def _insert_batch(self, table_name: str, batch_data: List[Dict[str, Any]]) -> int:
        """Insert a batch of data"""
        if not batch_data:
            return 0
        
        # Prepare data for insertion
        columns = list(batch_data[0].keys())
        values_list = []
        
        for row in batch_data:
            values = []
            for col in columns:
                value = row.get(col)
                values.append(str(value) if value is not None else None)
            values_list.append(tuple(values))
        
        # Generate INSERT SQL with conflict handling
        columns_str = ', '.join([f'"{col}"' if any(c in col for c in ['/', '-', ' ', '.', '(', ')']) else col for col in columns])
        placeholders = ', '.join(['%s'] * len(columns))
        
        # Create update clause for non-primary key columns
        update_clause = ', '.join([
            f'"{col}" = EXCLUDED."{col}"' if any(c in col for c in ['/', '-', ' ', '.', '(', ')']) else f'{col} = EXCLUDED.{col}'
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
            affected_rows = db_manager.execute_batch(insert_sql, values_list)
            return affected_rows
            
        except Exception as e:
            logger.error(f"Failed to insert batch: {e}")
            raise


# Global chunked migration strategy instance
chunked_migration_strategy = ChunkedMigrationStrategy()
