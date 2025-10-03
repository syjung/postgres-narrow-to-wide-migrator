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
        self.chunk_size_hours = migration_config.chunk_size_hours  # Use config setting
        self.max_chunk_records = migration_config.max_records_per_chunk  # Use config setting
        self.adaptive_chunking = migration_config.adaptive_chunking  # Use config setting
        self.batch_size = migration_config.batch_size
        self.allowed_columns = self._load_allowed_columns()
    
    def _load_allowed_columns(self) -> set:
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
        
        # 간단한 접근법: 고정된 시간 범위로 청크 생성
        # 실제 데이터가 있는지 확인하지 않고 청크를 생성
        logger.info("🚀 Using simplified chunk generation (no time range check)")
        
        # 기본 시간 범위 설정 (예: 최근 1년)
        if cutoff_time:
            end_time = cutoff_time
        else:
            end_time = datetime.now()
        
        # 시작 시간을 과거로 설정 (실제 데이터가 있을 가능성이 높은 시점)
        # 실제 운영 환경에서는 데이터가 과거에 있을 것이므로
        start_time = end_time - timedelta(days=365)
        
        logger.info(f"📅 Processing historical data from {start_time} to {end_time}")
        logger.info(f"📅 This will cover the past year of data")
        
        logger.info(f"📅 Using fixed time range: {start_time} to {end_time}")
        
        # Generate chunks
        current_start = start_time
        chunk_count = 0
        
        while current_start < end_time:
            current_end = min(
                current_start + timedelta(hours=self.chunk_size_hours),
                end_time
            )
            
            chunk_count += 1
            logger.info(f"📦 Generated chunk {chunk_count}: {current_start} to {current_end}")
            yield (current_start, current_end)
            
            current_start = current_end
        
        logger.info(f"✅ Generated {chunk_count} chunks for ship_id: {ship_id}")
    
    def _adjust_chunk_size_if_needed(self, ship_id: str, chunk_start: datetime, chunk_end: datetime, record_count: int) -> int:
        """
        동적으로 청크 크기를 조정합니다.
        레코드 수가 많으면 다음 청크부터 크기를 줄입니다.
        """
        if record_count > self.max_chunk_records:
            # 현재 청크가 너무 크면 다음 청크부터 크기를 줄임
            new_chunk_hours = max(6, self.chunk_size_hours // 2)  # 최소 6시간
            logger.warning(f"⚠️ Chunk too large ({record_count:,} records), reducing next chunk size to {new_chunk_hours}h")
            return new_chunk_hours
        elif record_count < self.max_chunk_records // 4:  # 레코드가 적으면 청크 크기 증가
            new_chunk_hours = min(48, self.chunk_size_hours * 2)  # 최대 48시간
            logger.info(f"📈 Chunk small ({record_count:,} records), increasing next chunk size to {new_chunk_hours}h")
            return new_chunk_hours
        else:
            return self.chunk_size_hours
    
    def _get_data_time_range(self, ship_id: str, cutoff_time: Optional[datetime] = None) -> Optional[Tuple[datetime, datetime]]:
        """Get the time range of data for a ship"""
        logger.info(f"🔍 Getting data time range for ship_id: {ship_id}")
        logger.info(f"📅 Cutoff time: {cutoff_time}")
        
        # 최적화된 쿼리: LIMIT을 사용하여 빠른 샘플링
        query = """
        SELECT 
            created_time
        FROM tenant.tbl_data_timeseries 
        WHERE ship_id = %s
        """
        params = [ship_id]
        
        if cutoff_time:
            query += " AND created_time < %s"
            params.append(cutoff_time)
        
        query += " ORDER BY created_time LIMIT 1"
        
        logger.info(f"📊 Executing optimized query: {query}")
        logger.info(f"📊 Query params: {params}")
        
        try:
            logger.info("🔄 Getting earliest timestamp...")
            earliest_result = db_manager.execute_query(query, tuple(params))
            
            if not earliest_result:
                logger.warning(f"⚠️ No data found for ship_id: {ship_id}")
                return None
            
            earliest_time = earliest_result[0]['created_time']
            logger.info(f"📅 Earliest time: {earliest_time}")
            
            # 최신 시간도 비슷하게 가져오기
            latest_query = query.replace("ORDER BY created_time LIMIT 1", "ORDER BY created_time DESC LIMIT 1")
            logger.info("🔄 Getting latest timestamp...")
            latest_result = db_manager.execute_query(latest_query, tuple(params))
            
            if not latest_result:
                logger.warning(f"⚠️ No latest data found for ship_id: {ship_id}")
                return None
            
            latest_time = latest_result[0]['created_time']
            logger.info(f"📅 Latest time: {latest_time}")
            
            # Skip count query for performance - just return time range
            logger.info(f"✅ Found data time range: {earliest_time} to {latest_time}")
            return (earliest_time, latest_time)
            
        except Exception as e:
            logger.error(f"❌ Failed to get data time range for ship_id {ship_id}: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _chunk_has_data(self, ship_id: str, start_time: datetime, end_time: datetime) -> bool:
        """
        Check if a time chunk has data using LIMIT 1 for performance
        
        ⚠️ OPTIMIZED: Uses LIMIT 1 instead of COUNT(*) for better performance
        """
        logger.info(f"🔍 Checking if chunk has data: {ship_id} [{start_time} to {end_time}]")
        
        # Use LIMIT 1 instead of COUNT(*) for better performance
        query = """
        SELECT 1
        FROM tenant.tbl_data_timeseries 
        WHERE ship_id = %s 
        AND created_time >= %s 
        AND created_time < %s
        LIMIT 1
        """
        
        try:
            logger.info("🔄 Executing optimized chunk data check...")
            result = db_manager.execute_query(query, (ship_id, start_time, end_time))
            has_data = len(result) > 0 if result else False
            logger.info(f"✅ Chunk has data: {has_data}")
            return has_data
        except Exception as e:
            logger.error(f"❌ Failed to check chunk data for {ship_id}: {e}")
            import traceback
            traceback.print_exc()
            return False
    
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
            # Step 1: Extract chunk data (with LIMIT to avoid large queries)
            logger.info(f"📊 Extracting data for chunk: {start_time} to {end_time}")
            chunk_data = self._extract_chunk_data_safe(ship_id, start_time, end_time)
            
            if not chunk_data:
                logger.info(f"ℹ️ No data found in chunk: {start_time} to {end_time}")
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
            
            # 📊 상세한 로그 정보
            data_columns = len(self.allowed_columns)
            time_range = f"{start_time} ~ {end_time}"
            
            logger.info(f"✅ CHUNK MIGRATION SUCCESS: {ship_id}")
            logger.info(f"   📊 Records processed: {inserted_count}")
            logger.info(f"   📊 Columns: {data_columns} data columns")
            logger.info(f"   📊 Time Range: {time_range}")
            logger.info(f"   📊 Method: Chunked migration (24-hour chunks)")
            
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
    
    def _extract_chunk_data_safe(self, ship_id: str, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        """Extract data for a specific chunk with safe LIMIT and column filtering"""
        import time
        
        logger.info(f"🔍 Starting data extraction for chunk: {ship_id} [{start_time} to {end_time}]")
        logger.info(f"🔒 Filtering by allowed columns: {len(self.allowed_columns)} columns")
        
        # Create a placeholder for IN clause with allowed columns
        if not self.allowed_columns:
            logger.warning("⚠️ No allowed columns found, skipping data extraction")
            return []
        
        # Convert set to list for SQL IN clause
        allowed_columns_list = list(self.allowed_columns)
        placeholders = ','.join(['%s'] * len(allowed_columns_list))
        
        query = f"""
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
        AND data_channel_id IN ({placeholders})
        ORDER BY created_time
        """
        
        start_time_query = time.time()
        logger.info(f"🚀 Executing large table query (this may take time)...")
        
        try:
            # Prepare parameters: start_time, end_time, ship_id, then all allowed columns
            params = [start_time, end_time, ship_id] + allowed_columns_list
            result = db_manager.execute_query(query, tuple(params))
            
            end_time_query = time.time()
            execution_time = end_time_query - start_time_query
            
            record_count = len(result) if result else 0
            logger.info(f"✅ Query completed successfully!")
            logger.info(f"📈 Execution time: {execution_time:.2f} seconds")
            logger.info(f"📊 Records extracted: {record_count}")
            
            # Check for large chunks and suggest optimization
            if record_count >= 1000000:  # 1M+ records
                logger.warning(f"⚠️ Very large chunk detected: {record_count:,} records")
                logger.warning(f"⚠️ Consider reducing chunk size from 24h to 12h or 6h")
            elif record_count >= 500000:  # 500K+ records
                logger.warning(f"⚠️ Large chunk detected: {record_count:,} records")
                logger.warning(f"⚠️ Consider reducing chunk size if performance degrades")
            
            # 성능 분석 및 권장사항
            if execution_time > 60.0:  # 1분 이상
                logger.error(f"❌ Very slow query: {execution_time:.2f}s execution time")
                logger.error(f"❌ Consider: 1) Reduce chunk size to 6-12h, 2) Check database performance")
            elif execution_time > 30.0:  # 30초 이상
                logger.warning(f"⚠️ Slow query detected: {execution_time:.2f}s execution time")
                logger.warning(f"⚠️ Consider reducing chunk size if this persists")
            elif execution_time > 10.0:  # 10초 이상
                logger.info(f"📊 Query execution time: {execution_time:.2f}s (acceptable for large chunks)")
            
            return result
            
        except Exception as e:
            end_time_query = time.time()
            execution_time = end_time_query - start_time_query
            logger.error(f"❌ Query failed after {execution_time:.2f} seconds: {e}")
            raise
    
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
        special_chars = ['/', '-', ' ', '.', '(', ')', '[', ']', '{', '}', '@', '#', '$', '%', '^', '&', '*', '+', '=', '|', '\\', ':', ';', '"', "'", '<', '>', ',', '?', '!', '~', '`']
        columns_str = ', '.join([f'"{col}"' if any(c in col for c in special_chars) else col for col in columns])
        placeholders = ', '.join(['%s'] * len(columns))
        
        # Create update clause for non-primary key columns
        update_clauses = []
        for col in columns:
            if col != 'created_time':
                quoted_col = f'"{col}"' if any(c in col for c in special_chars) else col
                update_clauses.append(f'{quoted_col} = EXCLUDED.{quoted_col}')
        
        # Check if we have any columns to update
        if not update_clauses:
            logger.warning(f"No columns to update for {table_name}, using simple INSERT")
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
            affected_rows = db_manager.execute_batch(insert_sql, values_list)
            
            # 📊 상세한 로그 정보
            data_columns = len(columns) - 1  # created_time 제외
            time_range = f"{min(row['created_time'] for row in batch_data)} ~ {max(row['created_time'] for row in batch_data)}"
            
            logger.info(f"✅ BATCH INSERT SUCCESS: {table_name}")
            logger.info(f"   📊 Records: {len(batch_data)} rows inserted")
            logger.info(f"   📊 Columns: {data_columns} data columns (total: {len(columns)})")
            logger.info(f"   📊 Time Range: {time_range}")
            logger.info(f"   📊 Affected Rows: {affected_rows}")
            
            # Status tracking log for check_status.sh
            ship_id = table_name.split('_')[-1].lower()  # Extract ship_id from table_name
            logger.info(f"STATUS:BATCH:{ship_id}:{len(batch_data)}:{data_columns}:{time_range}:{affected_rows}")
            
            return affected_rows
            
        except Exception as e:
            logger.error(f"❌ BATCH INSERT FAILED: {table_name}")
            logger.error(f"   📊 Records: {len(batch_data)} rows failed")
            logger.error(f"   📊 Columns: {len(columns)} columns")
            logger.error(f"   📊 Error: {e}")
            raise


# Global chunked migration strategy instance
chunked_migration_strategy = ChunkedMigrationStrategy()
