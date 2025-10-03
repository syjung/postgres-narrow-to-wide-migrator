"""
Ultra-fast migrator using COPY + single query approach
"""
import csv
import io
import tempfile
import os
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from loguru import logger
from database import db_manager
from config import migration_config
from chunked_migration_strategy import chunked_migration_strategy


class UltraFastMigrator:
    """Ultra-fast migrator using COPY + single query approach"""
    
    def __init__(self, column_list_file: str = "column_list.txt"):
        self.column_list_file = column_list_file
        self.value_format_mapping = migration_config.VALUE_FORMAT_MAPPING
        self.target_columns = self._load_column_list()
    
    def _load_column_list(self) -> List[str]:
        """Load column list from file"""
        try:
            with open(self.column_list_file, 'r', encoding='utf-8') as f:
                columns = [line.strip().strip('"') for line in f.readlines() if line.strip()]
            
            logger.info(f"Loaded {len(columns)} columns from {self.column_list_file}")
            return columns
            
        except FileNotFoundError:
            logger.error(f"Column list file not found: {self.column_list_file}")
            return []
        except Exception as e:
            logger.error(f"Failed to load column list: {e}")
            return []
    
    def migrate_ship_data_ultra_fast(self, ship_id: str, cutoff_time: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Ultra-fast migration using COPY + single query
        
        Args:
            ship_id: Ship identifier
            cutoff_time: Only migrate data before this time
            
        Returns:
            Migration result dictionary
        """
        logger.info(f"Starting ultra-fast migration for ship_id: {ship_id}")
        
        try:
            if not self.target_columns:
                return {
                    'ship_id': ship_id,
                    'status': 'failed',
                    'migrated_count': 0,
                    'error_count': 0,
                    'message': 'No target columns loaded'
                }
            
            table_name = f"tbl_data_timeseries_{ship_id.lower()}"
            
            # Create table with predefined columns
            self._create_table_with_columns(table_name)
            
            # Get total count
            total_count = self._get_migration_count(ship_id, cutoff_time)
            logger.info(f"Total records to migrate: {total_count}")
            
            if total_count == 0:
                return {
                    'ship_id': ship_id,
                    'status': 'completed',
                    'migrated_count': 0,
                    'error_count': 0,
                    'message': 'No data to migrate'
                }
            
            # Ultra-fast migration using COPY + single query
            migrated_count = self._migrate_ultra_fast(ship_id, table_name, cutoff_time)
            
            # Validate migration
            validation = self._validate_migration(ship_id, table_name, total_count)
            
            result = {
                'ship_id': ship_id,
                'status': 'completed',
                'migrated_count': migrated_count,
                'error_count': 0,
                'validation': validation,
                'message': f'Ultra-fast migration completed: {migrated_count} records migrated'
            }
            
            logger.info(f"Ultra-fast migration completed for ship_id {ship_id}: {result['message']}")
            return result
            
        except Exception as e:
            logger.error(f"Ultra-fast migration failed for ship_id {ship_id}: {e}")
            return {
                'ship_id': ship_id,
                'status': 'failed',
                'migrated_count': 0,
                'error_count': 0,
                'message': f'Ultra-fast migration failed: {str(e)}'
            }
    
    def migrate_ship_data_chunked(self, ship_id: str, cutoff_time: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Chunked migration for large-scale data
        
        Args:
            ship_id: Ship identifier
            cutoff_time: Only migrate data before this time
            
        Returns:
            Migration result dictionary
        """
        logger.info(f"Starting chunked migration for ship_id: {ship_id}")
        
        try:
            if not self.target_columns:
                return {
                    'ship_id': ship_id,
                    'status': 'failed',
                    'migrated_count': 0,
                    'error_count': 0,
                    'message': 'No target columns loaded'
                }
            
            table_name = f"tbl_data_timeseries_{ship_id.lower()}"
            
            # Create table with predefined columns
            self._create_table_with_columns(table_name)
            
            # Skip count query - start migration directly
            logger.info("🚀 Starting chunked migration without count query (optimized)")
            
            # Chunked migration
            migrated_count = self._migrate_chunked(ship_id, table_name, cutoff_time)
            
            # Validate migration (without expected count)
            validation = self._validate_migration_optimized(ship_id, table_name)
            
            result = {
                'ship_id': ship_id,
                'status': 'completed',
                'migrated_count': migrated_count,
                'error_count': 0,
                'validation': validation,
                'message': f'Chunked migration completed: {migrated_count} records migrated'
            }
            
            logger.info(f"Chunked migration completed for ship_id {ship_id}: {result['message']}")
            return result
            
        except Exception as e:
            logger.error(f"Chunked migration failed for ship_id {ship_id}: {e}")
            return {
                'ship_id': ship_id,
                'status': 'failed',
                'migrated_count': 0,
                'error_count': 0,
                'message': f'Chunked migration failed: {str(e)}'
            }
    
    def _migrate_chunked(self, ship_id: str, table_name: str, cutoff_time: Optional[datetime] = None) -> int:
        """Migrate data using chunked approach"""
        logger.info("Starting chunked migration...")
        
        total_migrated = 0
        chunk_count = 0
        failed_chunks = 0
        
        # Generate data chunks
        chunks = chunked_migration_strategy.get_data_chunks(ship_id, cutoff_time)
        
        for start_time, end_time in chunks:
            chunk_count += 1
            logger.info(f"Processing chunk {chunk_count}: {start_time} to {end_time}")
            
            try:
                # Migrate chunk
                chunk_result = chunked_migration_strategy.migrate_chunk(
                    ship_id, start_time, end_time, table_name
                )
                
                if chunk_result['status'] == 'completed':
                    total_migrated += chunk_result['records_processed']
                    logger.info(f"Chunk {chunk_count} completed: {chunk_result['records_processed']} records")
                elif chunk_result['status'] == 'skipped':
                    logger.info(f"Chunk {chunk_count} skipped: {chunk_result['message']}")
                else:
                    failed_chunks += 1
                    logger.error(f"Chunk {chunk_count} failed: {chunk_result.get('error', 'Unknown error')}")
                
            except Exception as e:
                failed_chunks += 1
                logger.error(f"Failed to process chunk {chunk_count}: {e}")
        
        logger.info(f"Chunked migration completed: {chunk_count} chunks, {total_migrated} records, {failed_chunks} failed")
        return total_migrated
    
    def _create_table_with_columns(self, table_name: str) -> None:
        """Create table with predefined columns (preserve existing data)"""
        # Check if table already exists
        if db_manager.check_table_exists(table_name):
            logger.info(f"Table {table_name} already exists, skipping creation")
            return
        
        # Create table with predefined columns
        column_definitions = ["created_time TIMESTAMP PRIMARY KEY"]
        
        for column_name in self.target_columns:
            # Quote column name if it contains special characters
            special_chars = ['/', '-', ' ', '.', '(', ')', '[', ']', '{', '}', '@', '#', '$', '%', '^', '&', '*', '+', '=', '|', '\\', ':', ';', '"', "'", '<', '>', ',', '?', '!', '~', '`']
            quoted_column = f'"{column_name}"' if any(c in column_name for c in special_chars) else column_name
            column_definitions.append(f"{quoted_column} TEXT")
        
        create_sql = f"""
        CREATE TABLE tenant.{table_name} (
            {', '.join(column_definitions)}
        )
        """
        
        with db_manager.get_cursor() as cursor:
            cursor.execute(create_sql)
        
        # Create index
        index_sql = f"""
        CREATE INDEX IF NOT EXISTS {table_name}_created_time_idx 
        ON tenant.{table_name} (created_time)
        """
        
        with db_manager.get_cursor() as cursor:
            cursor.execute(index_sql)
        
        logger.info(f"Created table {table_name} with {len(self.target_columns)} data columns")
    
    def _migrate_ultra_fast(self, ship_id: str, table_name: str, cutoff_time: Optional[datetime] = None) -> int:
        """Ultra-fast migration using COPY + single query approach"""
        import time
        
        # Step 1: Extract data using COPY TO (fastest way to get data)
        logger.info(f"🔍 Starting ultra-fast migration for ship: {ship_id}")
        logger.info(f"📊 Method: PostgreSQL COPY TO/FROM (optimized for large datasets)")
        logger.info("Step 1: Extracting data using COPY TO...")
        
        extract_query = f"""
        COPY (
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
            WHERE ship_id = '{ship_id}'
            AND data_channel_id IN ({','.join([f"'{col}'" for col in self.target_columns])})
        """
        
        if cutoff_time:
            extract_query += f" AND created_time < '{cutoff_time}'"
            logger.info(f"📅 Cutoff time applied: {cutoff_time}")
        
        extract_query += " ORDER BY created_time ) TO STDOUT WITH CSV"
        
        logger.info(f"🚀 Executing COPY TO query (this may take time for large datasets)...")
        start_time_extract = time.time()
        
        # Create temporary file for extracted data
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as temp_file:
            temp_file_path = temp_file.name
        
        try:
            # Execute COPY TO
            with db_manager.get_cursor() as cursor:
                logger.info(f"📝 Writing data to temporary file: {temp_file_path}")
                with open(temp_file_path, 'w') as f:
                    cursor.copy_expert(extract_query, f)
            
            end_time_extract = time.time()
            extract_time = end_time_extract - start_time_extract
            
            # Get file size
            import os
            file_size = os.path.getsize(temp_file_path)
            file_size_mb = file_size / (1024 * 1024)
            
            logger.info(f"✅ COPY TO completed successfully!")
            logger.info(f"📈 Extract time: {extract_time:.2f} seconds")
            logger.info(f"📊 File size: {file_size_mb:.2f} MB")
            
            if extract_time > 10.0:
                logger.warning(f"⚠️ Slow COPY TO detected: {extract_time:.2f}s execution time")
            
            logger.info(f"Data extracted to {temp_file_path}")
            
            # Step 2: Transform data in memory (pivot narrow to wide)
            logger.info("Step 2: Transforming data (narrow to wide)...")
            
            transformed_data = self._transform_data_to_wide(temp_file_path)
            
            # Step 3: Insert using COPY FROM (fastest way to insert data)
            logger.info("Step 3: Inserting data using COPY FROM...")
            
            inserted_count = self._insert_wide_data_copy(table_name, transformed_data)
            
            return inserted_count
            
        finally:
            # Clean up temporary file
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
    
    def _transform_data_to_wide(self, csv_file_path: str) -> List[Dict[str, Any]]:
        """Transform narrow data to wide format"""
        # Read extracted data
        narrow_data = []
        with open(csv_file_path, 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) >= 3:
                    narrow_data.append({
                        'created_time': row[0],
                        'data_channel_id': row[1],
                        'value': row[2] if row[2] else None
                    })
        
        # Group by timestamp
        grouped_data = {}
        for row in narrow_data:
            timestamp = row['created_time']
            if timestamp not in grouped_data:
                grouped_data[timestamp] = {}
            
            channel_id = row['data_channel_id']
            # Only include channels that are in our target columns
            if channel_id in self.target_columns:
                grouped_data[timestamp][channel_id] = row['value']
        
        # Convert to wide format
        wide_data = []
        for timestamp, channels in grouped_data.items():
            wide_row = {'created_time': timestamp}
            wide_row.update(channels)
            wide_data.append(wide_row)
        
        # Debug logging
        logger.info(f"Transformed {len(narrow_data)} narrow records to {len(wide_data)} wide records")
        if wide_data:
            sample_row = wide_data[0]
            logger.info(f"Sample wide row has {len(sample_row)-1} channels (excluding created_time)")
            logger.info(f"Sample channels: {list(sample_row.keys())[:5]}...")
        
        return wide_data
    
    def _insert_wide_data_copy(self, table_name: str, wide_data: List[Dict[str, Any]]) -> int:
        """Insert wide data using COPY FROM"""
        if not wide_data:
            return 0
        
        # Prepare CSV data
        csv_data = []
        for row in wide_data:
            csv_row = []
            for col_name in ['created_time'] + self.target_columns:
                value = row.get(col_name)
                csv_row.append(str(value) if value is not None else '')
            csv_data.append(csv_row)
        
        # Create CSV buffer
        csv_buffer = io.StringIO()
        writer = csv.writer(csv_buffer)
        writer.writerows(csv_data)
        csv_buffer.seek(0)
        
        # Prepare column names for COPY
        special_chars = ['/', '-', ' ', '.', '(', ')', '[', ']', '{', '}', '@', '#', '$', '%', '^', '&', '*', '+', '=', '|', '\\', ':', ';', '"', "'", '<', '>', ',', '?', '!', '~', '`']
        quoted_columns = []
        for col in ['created_time'] + self.target_columns:
            quoted_columns.append(f'"{col}"' if any(c in col for c in special_chars) else col)
        
        columns_str = ', '.join(quoted_columns)
        
        # Execute COPY FROM with conflict handling
        # First, create a temporary table
        temp_table = f"{table_name}_temp"
        
        # Create temp table with same structure
        temp_create_sql = f"""
        CREATE TEMP TABLE {temp_table} (
            LIKE tenant.{table_name}
        )
        """
        
        with db_manager.get_cursor() as cursor:
            cursor.execute(temp_create_sql)
            
            # Insert into temp table
            copy_sql = f"""
            COPY {temp_table} ({columns_str})
            FROM STDIN
            WITH CSV
            """
            cursor.copy_expert(copy_sql, csv_buffer)
            
            # Insert from temp table with conflict handling
            special_chars = ['/', '-', ' ', '.', '(', ')', '[', ']', '{', '}', '@', '#', '$', '%', '^', '&', '*', '+', '=', '|', '\\', ':', ';', '"', "'", '<', '>', ',', '?', '!', '~', '`']
            update_clauses = []
            for col in self.target_columns:
                quoted_col = f'"{col}"' if any(c in col for c in special_chars) else col
                update_clauses.append(f'{quoted_col} = EXCLUDED.{quoted_col}')
            
            # Check if we have any columns to update
            if not update_clauses:
                logger.warning(f"No columns to update for {table_name}, using simple INSERT")
                insert_sql = f"""
                INSERT INTO tenant.{table_name} 
                SELECT * FROM {temp_table}
                ON CONFLICT (created_time) DO NOTHING
                """
            else:
                insert_sql = f"""
                INSERT INTO tenant.{table_name} 
                SELECT * FROM {temp_table}
                ON CONFLICT (created_time) DO UPDATE SET
                {', '.join(update_clauses)}
                """
            cursor.execute(insert_sql)
        
            logger.info(f"Inserted {len(csv_data)} records using COPY FROM with conflict handling")
            
            # 📊 상세한 로그 정보
            data_columns = len(self.target_columns)
            time_range = f"{min(row['created_time'] for row in wide_data)} ~ {max(row['created_time'] for row in wide_data)}"
            
            logger.info(f"✅ ULTRA-FAST INSERT SUCCESS: {table_name}")
            logger.info(f"   📊 Records: {len(csv_data)} rows inserted")
            logger.info(f"   📊 Columns: {data_columns} data columns (total: {data_columns + 1})")
            logger.info(f"   📊 Time Range: {time_range}")
            logger.info(f"   📊 Method: PostgreSQL COPY FROM (optimized)")
            
            return len(csv_data)
    
    def _get_migration_count(self, ship_id: str, cutoff_time: Optional[datetime] = None) -> int:
        """
        Get total count of unique timestamps to migrate
        
        ⚠️ WARNING: This function uses COUNT(DISTINCT) which is very slow on large tables.
        This function is deprecated and should not be used in production.
        Use chunked migration without count queries for better performance.
        """
        logger.warning("⚠️ _get_migration_count is deprecated - use chunked migration without count queries")
        
        query = "SELECT COUNT(DISTINCT created_time) as count FROM tenant.tbl_data_timeseries WHERE ship_id = %s"
        params = [ship_id]
        
        if cutoff_time:
            query += " AND created_time < %s"
            params.append(cutoff_time)
        
        result = db_manager.execute_query(query, tuple(params))
        return result[0]['count'] if result else 0
    
    def _validate_migration(self, ship_id: str, table_name: str, expected_count: int) -> Dict[str, Any]:
        """Validate migration results"""
        try:
            # Count unique timestamps in target table
            count_query = f"SELECT COUNT(DISTINCT created_time) as count FROM tenant.{table_name}"
            result = db_manager.execute_query(count_query)
            actual_count = result[0]['count'] if result else 0
            
            return {
                'expected_count': expected_count,
                'actual_count': actual_count,
                'count_match': expected_count == actual_count,
                'status': 'passed' if expected_count == actual_count else 'failed'
            }
            
        except Exception as e:
            logger.error(f"Validation failed for ship_id {ship_id}: {e}")
            return {
                'expected_count': expected_count,
                'actual_count': 0,
                'count_match': False,
                'status': 'failed'
            }
    
    def _validate_migration_optimized(self, ship_id: str, table_name: str) -> Dict[str, Any]:
        """Validate migration results without count queries"""
        try:
            # Simple validation: check if table exists and has data
            logger.info(f"🔍 Validating migration for {ship_id}")
            
            # Check if table exists
            check_table_query = f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'tenant' 
                AND table_name = %s
            ) as table_exists
            """
            result = db_manager.execute_query(check_table_query, (table_name,))
            table_exists = result[0]['table_exists'] if result else False
            
            if not table_exists:
                return {
                    'status': 'failed',
                    'message': 'Target table does not exist',
                    'table_exists': False
                }
            
            # Check if table has any data (LIMIT 1 for speed)
            check_data_query = f"SELECT 1 FROM tenant.{table_name} LIMIT 1"
            data_result = db_manager.execute_query(check_data_query)
            has_data = len(data_result) > 0 if data_result else False
            
            logger.info(f"✅ Migration validation completed: table_exists={table_exists}, has_data={has_data}")
            
            return {
                'status': 'completed',
                'message': 'Migration validation completed',
                'table_exists': table_exists,
                'has_data': has_data
            }
            
        except Exception as e:
            logger.error(f"Migration validation failed: {e}")
            return {
                'status': 'failed',
                'message': f'Validation error: {str(e)}',
                'error': str(e)
            }


# Create instance
ultra_fast_migrator = UltraFastMigrator()
