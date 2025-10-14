"""
Multi-Table Chunked Migration Strategy
3개 테이블로 데이터를 분산하여 저장하는 청크 기반 마이그레이션 전략
"""
import time
from typing import Dict, List, Any, Optional, Tuple, Set
from datetime import datetime, timedelta
from loguru import logger
from database import db_manager
from channel_router import channel_router
from config import migration_config
from thread_logger import get_ship_thread_logger


class MultiTableChunkedStrategy:
    """Multi-table chunked migration strategy"""
    
    def __init__(self):
        self.chunk_size_hours = migration_config.chunk_size_hours
        self.max_chunk_records = migration_config.max_records_per_chunk
        self.adaptive_chunking = migration_config.adaptive_chunking
        self.batch_size = migration_config.batch_size
        self.channel_router = channel_router
    
    def get_data_chunks(self, ship_id: str, cutoff_time: Optional[datetime] = None, thread_logger=None) -> List[Tuple[datetime, datetime]]:
        """
        시간 범위를 청크로 분할 (고정 시간 범위 사용 - DB 쿼리 없이 빠름!)
        
        Args:
            ship_id: 선박 ID
            cutoff_time: 마이그레이션 종료 시간
            thread_logger: 스레드 전용 로거
            
        Returns:
            (start_time, end_time) 튜플의 리스트
        """
        if thread_logger is None:
            thread_logger = get_ship_thread_logger(ship_id)
        
        thread_logger.info(f"🔍 Getting data chunks for {ship_id} (cutoff: {cutoff_time})")
        
        # 고정 시간 범위 사용 (DB 쿼리 없이 즉시 생성!)
        if cutoff_time:
            end_time = cutoff_time
        else:
            end_time = datetime.now()
        
        # Config에서 설정한 기간만큼 과거로 설정
        lookback_days = migration_config.batch_lookback_days
        start_time = end_time - timedelta(days=lookback_days)
        
        thread_logger.info(f"📅 Using fixed time range: {start_time} to {end_time}")
        thread_logger.info(f"📅 Processing {lookback_days} days of data (configurable in config.py)")
        
        # Generate chunks
        chunks = []
        current_start = start_time
        
        while current_start < end_time:
            current_end = min(
                current_start + timedelta(hours=self.chunk_size_hours),
                end_time
            )
            chunks.append((current_start, current_end))
            current_start = current_end
        
        thread_logger.info(f"📊 Generated {len(chunks)} chunks ({self.chunk_size_hours}-hour chunks)")
        return chunks
    
    def migrate_chunk(
        self, 
        ship_id: str, 
        start_time: datetime, 
        end_time: datetime,
        thread_logger=None
    ) -> Dict[str, Any]:
        """
        단일 청크를 3개 테이블로 마이그레이션
        
        Args:
            ship_id: 선박 ID
            start_time: 청크 시작 시간
            end_time: 청크 종료 시간
            thread_logger: 스레드 전용 로거
            
        Returns:
            마이그레이션 결과
        """
        if thread_logger is None:
            thread_logger = get_ship_thread_logger(ship_id)
        
        thread_logger.info(f"🔄 Processing chunk: {start_time} to {end_time}")
        
        try:
            # Step 1: Extract data from narrow table
            thread_logger.info(f"📥 Extracting data from narrow table...")
            chunk_data = self._extract_chunk_data(ship_id, start_time, end_time, thread_logger)
            
            if not chunk_data:
                thread_logger.info(f"⏭️ No data in this chunk, skipping...")
                return {
                    'status': 'skipped',
                    'message': 'No data in chunk',
                    'records_processed': 0
                }
            
            thread_logger.info(f"✅ Extracted {len(chunk_data)} records from narrow table")
            
            # Step 2: Transform to wide format
            thread_logger.info(f"🔄 Transforming {len(chunk_data):,} narrow records to wide format...")
            import time as time_module
            transform_start = time_module.time()
            
            wide_data = self._transform_chunk_to_wide(chunk_data, thread_logger)
            
            transform_duration = time_module.time() - transform_start
            thread_logger.info(f"✅ Transformed to {len(wide_data):,} wide records in {transform_duration:.2f}s (grouped by timestamp)")
            
            # Step 3: Split by table type and insert
            thread_logger.info(f"🔄 Splitting data for 3 tables...")
            total_inserted = 0
            insert_summary = {}
            
            for table_type in self.channel_router.get_all_table_types():
                table_name = f"tbl_{table_type}_{ship_id.lower()}"
                
                # Filter data for this table
                filter_start = time_module.time()
                table_data = self._filter_data_for_table(wide_data, table_type)
                filter_duration = time_module.time() - filter_start
                
                if not table_data:
                    thread_logger.debug(f"⏭️ No data for table: {table_name} (filtered in {filter_duration:.2f}s)")
                    insert_summary[table_type] = 0
                    continue
                
                thread_logger.info(f"✅ Filtered {len(table_data)} records for {table_name} in {filter_duration:.2f}s")
                thread_logger.info(f"💾 Inserting {len(table_data):,} records into {table_name}...")
                
                try:
                    insert_start = time_module.time()
                    inserted = self._insert_wide_data(table_name, table_data, thread_logger)
                    insert_duration = time_module.time() - insert_start
                    
                    total_inserted += inserted
                    insert_summary[table_type] = inserted
                    thread_logger.success(f"✅ Inserted {inserted:,} records into {table_name} in {insert_duration:.2f}s ({inserted/insert_duration:.0f} rows/s)")
                    
                except Exception as e:
                    thread_logger.error(f"❌ Failed to insert into {table_name}: {e}")
                    insert_summary[table_type] = 0
                    # Continue with next table instead of failing entire chunk
                    continue
            
            # Summary log
            thread_logger.success(f"📊 Chunk completed: {len(chunk_data)} narrow records → {total_inserted} wide records inserted")
            thread_logger.info(f"   └─ tbl_1 (auxiliary): {insert_summary.get('1', 0)} rows")
            thread_logger.info(f"   └─ tbl_2 (engine): {insert_summary.get('2', 0)} rows")
            thread_logger.info(f"   └─ tbl_3 (navigation): {insert_summary.get('3', 0)} rows")
            
            return {
                'status': 'completed',
                'records_processed': total_inserted,
                'chunk_start': start_time,
                'chunk_end': end_time,
                'insert_summary': insert_summary,
                'narrow_records': len(chunk_data)
            }
            
        except Exception as e:
            thread_logger.error(f"❌ Chunk migration failed: {e}")
            return {
                'status': 'failed',
                'error': str(e),
                'chunk_start': start_time,
                'chunk_end': end_time
            }
    
    def _extract_chunk_data(
        self,
        ship_id: str,
        start_time: datetime,
        end_time: datetime,
        thread_logger
    ) -> List[Dict[str, Any]]:
        """Extract chunk data from narrow table"""
        
        # Get all channels from all tables
        all_channels = set()
        for table_type in self.channel_router.get_all_table_types():
            all_channels.update(self.channel_router.get_all_channels_by_table(table_type))
        
        if not all_channels:
            thread_logger.warning("⚠️ No allowed channels found")
            return []
        
        # Don't use IN clause with 1037 items - it's slow!
        # Get all data for the ship/time range, filter in application
        query = """
        SELECT 
            created_time,
            data_channel_id,
            CASE 
                WHEN value_format = 'Decimal' THEN double_v::text
                WHEN value_format = 'Integer' THEN long_v::text
                WHEN value_format = 'String' THEN str_v
                WHEN value_format = 'Boolean' THEN bool_v::text
                ELSE NULL
            END as value
        FROM tenant.tbl_data_timeseries 
        WHERE created_time >= %s 
        AND created_time < %s
        AND ship_id = %s
        ORDER BY created_time
        """
        
        params = (start_time, end_time, ship_id)
        result = db_manager.execute_query(query, params)
        
        if not result:
            return []
        
        # Filter by allowed channels in application
        filtered_result = [
            row for row in result 
            if row['data_channel_id'] in all_channels
        ]
        
        thread_logger.info(f"   📊 Fetched {len(result):,} rows, filtered to {len(filtered_result):,} rows")
        
        # Debugging: Show sample channels if filtering removed everything
        if len(result) > 0 and len(filtered_result) == 0:
            thread_logger.warning(f"⚠️ All {len(result):,} rows were filtered out!")
            
            # Show sample of fetched channels
            sample_fetched = set(row['data_channel_id'] for row in result[:10])
            thread_logger.warning(f"   Sample fetched channels: {list(sample_fetched)[:3]}")
            
            # Show sample of allowed channels
            sample_allowed = list(all_channels)[:3]
            thread_logger.warning(f"   Sample allowed channels: {sample_allowed}")
            
            # Check if it's a prefix mismatch issue
            if len(sample_fetched) > 0 and len(sample_allowed) > 0:
                fetched_first = list(sample_fetched)[0]
                allowed_first = sample_allowed[0]
                thread_logger.warning(f"   Fetched starts with '/': {fetched_first.startswith('/')}")
                thread_logger.warning(f"   Allowed starts with '/': {allowed_first.startswith('/')}")
        
        return filtered_result
    
    def _transform_chunk_to_wide(self, chunk_data: List[Dict[str, Any]], thread_logger=None) -> List[Dict[str, Any]]:
        """Transform chunk data to wide format"""
        from collections import defaultdict
        import time as time_module
        
        total_rows = len(chunk_data)
        
        # Group by timestamp
        timestamp_groups = defaultdict(dict)
        
        for idx, row in enumerate(chunk_data):
            # Progress log for large datasets (every 100k rows)
            if thread_logger and (idx + 1) % 100000 == 0:
                progress_pct = ((idx + 1) / total_rows) * 100
                thread_logger.info(f"   🔄 Grouping progress: {idx+1:,}/{total_rows:,} rows ({progress_pct:.1f}%)")
            
            timestamp = row['created_time']
            channel_id = row['data_channel_id']
            value = row['value']
            
            # Convert channel to column name
            col_name = self._channel_to_column_name(channel_id)
            
            # Parse value
            if value is not None:
                try:
                    timestamp_groups[timestamp][col_name] = float(value)
                except (ValueError, TypeError):
                    timestamp_groups[timestamp][col_name] = None
            else:
                timestamp_groups[timestamp][col_name] = None
        
        # Convert to list of dicts
        if thread_logger:
            thread_logger.info(f"   🔄 Converting {len(timestamp_groups):,} timestamp groups to wide rows...")
        
        wide_data = []
        for timestamp, values in sorted(timestamp_groups.items()):
            row = {'created_time': timestamp}
            row.update(values)
            wide_data.append(row)
        
        return wide_data
    
    def _filter_data_for_table(self, wide_data: List[Dict[str, Any]], table_type: str) -> List[Dict[str, Any]]:
        """
        특정 테이블용 데이터만 필터링
        
        Args:
            wide_data: Wide 포맷 데이터
            table_type: 테이블 타입
            
        Returns:
            필터링된 데이터
        """
        # Get channels for this table
        channels = self.channel_router.get_all_channels_by_table(table_type)
        
        # Convert channels to column names
        col_names = {self._channel_to_column_name(ch) for ch in channels}
        col_names.add('created_time')  # Always include timestamp
        
        # Filter data
        filtered_data = []
        for row in wide_data:
            filtered_row = {}
            has_data = False
            
            for col in col_names:
                if col in row:
                    filtered_row[col] = row[col]
                    if col != 'created_time' and row[col] is not None:
                        has_data = True
            
            # Only include rows that have at least one non-null value
            if has_data and 'created_time' in filtered_row:
                filtered_data.append(filtered_row)
        
        return filtered_data
    
    def _channel_to_column_name(self, channel: str) -> str:
        """Convert channel ID to column name (use as-is, no transformation)"""
        # 채널 ID를 그대로 컬럼명으로 사용 (변환 없음)
        return channel
    
    def _insert_wide_data(
        self,
        table_name: str,
        wide_data: List[Dict[str, Any]],
        thread_logger
    ) -> int:
        """Insert wide data in batches"""
        total_inserted = 0
        batch_count = 0
        
        for i in range(0, len(wide_data), self.batch_size):
            batch_count += 1
            batch = wide_data[i:i + self.batch_size]
            
            thread_logger.debug(f"🔄 Processing batch {batch_count}: {len(batch)} records")
            
            try:
                inserted = self._insert_batch(table_name, batch, thread_logger)
                total_inserted += inserted
                
            except Exception as e:
                thread_logger.error(f"Batch {batch_count} failed: {e}")
                raise
        
        return total_inserted
    
    def _insert_batch(self, table_name: str, batch_data: List[Dict[str, Any]], thread_logger) -> int:
        """Insert a batch of data using executemany"""
        if not batch_data:
            return 0
        
        # Prepare columns and values
        columns = list(batch_data[0].keys())
        values_list = []
        
        for row in batch_data:
            values = []
            for col in columns:
                value = row.get(col)
                values.append(str(value) if value is not None else None)
            values_list.append(tuple(values))
        
        # Generate INSERT SQL with ON CONFLICT
        special_chars = ['/', '-', ' ', '.', '(', ')', '[', ']', '{', '}', '@', '#', '$', '%', '^', '&', '*', '+', '=', '|', '\\', ':', ';', '"', "'", '<', '>', ',', '?', '!', '~', '`']
        columns_str = ', '.join([f'"{col}"' if any(c in col for c in special_chars) else col for col in columns])
        placeholders = ', '.join(['%s'] * len(columns))
        
        # Create update clause
        update_clauses = []
        for col in columns:
            if col != 'created_time':
                quoted_col = f'"{col}"' if any(c in col for c in special_chars) else col
                update_clauses.append(f'{quoted_col} = EXCLUDED.{quoted_col}')
        
        if update_clauses:
            update_clause = ', '.join(update_clauses)
            insert_sql = f"""
            INSERT INTO tenant.{table_name} ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT (created_time) DO UPDATE SET
            {update_clause}
            """
        else:
            insert_sql = f"""
            INSERT INTO tenant.{table_name} ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT (created_time) DO NOTHING
            """
        
        try:
            affected_rows = db_manager.execute_batch(insert_sql, values_list)
            thread_logger.debug(f"✅ Batch insert: {affected_rows} rows affected")
            return affected_rows
            
        except Exception as e:
            thread_logger.error(f"❌ Batch insert failed: {e}")
            raise


# Global instance
multi_table_chunked_strategy = MultiTableChunkedStrategy()

