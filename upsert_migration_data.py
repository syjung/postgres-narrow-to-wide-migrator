"""
CSV Migration Data Upserter
migration_data 폴더의 CSV 파일들을 읽어서 3개의 wide 테이블에 upsert
"""
import os
import csv
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from pathlib import Path
from loguru import logger
import sys

# 프로젝트 모듈 import
from database import db_manager
from channel_router import channel_router
from config import migration_config

# 선박 번호 매핑
SHIP_MAPPING = {
    'H2546': 'IMO9976903',
    'H2547': 'IMO9976915',
    'H2548': 'IMO9976927',
    'H2549': 'IMO9976939',
    'H2559': 'IMO9986051',
    'H2560': 'IMO9986087',
}

# 로깅 설정
logger.remove()
logger.add(
    "logs/csv_upsert.log",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    level="INFO",
    rotation="100 MB"
)
logger.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | {message}",
    level="INFO"
)


class CSVMigrationUpserter:
    """CSV 파일을 읽어서 wide 테이블에 upsert하는 클래스"""
    
    def __init__(self, base_dir: str = "migration_data", dry_run: bool = False):
        self.base_dir = Path(base_dir)
        self.channel_router = channel_router
        self.dry_run = dry_run
        self.stats = {
            'total_files': 0,
            'processed_files': 0,
            'failed_files': 0,
            'csv_rows_read': 0,  # CSV에서 읽은 실제 row 수
            'table_1_rows': 0,   # Table 1에 upsert된 row 수
            'table_2_rows': 0,   # Table 2에 upsert된 row 수
            'table_3_rows': 0,   # Table 3에 upsert된 row 수
        }
        
        # 테이블 컬럼 수 캐시 (테이블명 -> 컬럼 수)
        self.table_column_count_cache: Dict[str, int] = {}
        
        if dry_run:
            logger.warning("🔍 DRY-RUN MODE: No data will be inserted into DB")
    
    def process_all_ships(self):
        """모든 선박의 CSV 파일 처리"""
        logger.info("🚀 Starting CSV migration data upsert")
        logger.info(f"📂 Base directory: {self.base_dir.absolute()}")
        
        if not self.base_dir.exists():
            logger.error(f"❌ Directory not found: {self.base_dir}")
            return
        
        # 각 선박 폴더 처리
        for ship_folder in sorted(self.base_dir.iterdir()):
            if not ship_folder.is_dir():
                continue
            
            ship_code = ship_folder.name
            if ship_code not in SHIP_MAPPING:
                logger.warning(f"⚠️ Unknown ship code: {ship_code}, skipping...")
                continue
            
            imo_number = SHIP_MAPPING[ship_code]
            logger.info(f"\n{'='*80}")
            logger.info(f"🚢 Processing ship: {ship_code} → {imo_number}")
            logger.info(f"{'='*80}")
            
            self.process_ship_folder(ship_folder, imo_number)
        
        # 최종 통계
        self.print_summary()
    
    def process_ship_folder(self, ship_folder: Path, imo_number: str):
        """특정 선박 폴더의 모든 CSV 파일 처리"""
        csv_files = sorted(ship_folder.glob("*.csv"))
        
        if not csv_files:
            logger.warning(f"   ⚠️ No CSV files found in {ship_folder}")
            return
        
        logger.info(f"   📊 Found {len(csv_files)} CSV files")
        
        # 테이블 존재 확인 (Dry-run이 아닐 때만)
        if not self.dry_run:
            logger.info(f"   🔍 Checking if tables exist for {imo_number}...")
            for table_type in ['1', '2', '3']:
                table_name = f"tbl_data_timeseries_{imo_number.lower()}_{table_type}"
                if not db_manager.check_table_exists(table_name):
                    logger.error(f"   ❌ Table does not exist: {table_name}")
                    logger.error(f"   💡 Run Realtime or Batch first to create tables, or use multi_table_generator")
                    raise RuntimeError(f"Table {table_name} does not exist")
            logger.info(f"   ✅ All 3 tables exist")
            
            # 테이블 컬럼 개수 확인 및 경고
            self.check_table_columns(imo_number)
        
        for csv_file in csv_files:
            try:
                self.process_csv_file(csv_file, imo_number)
                self.stats['processed_files'] += 1
            except Exception as e:
                logger.error(f"   ❌ Failed to process {csv_file.name}: {e}")
                self.stats['failed_files'] += 1
    
    def process_csv_file(self, csv_file: Path, imo_number: str):
        """단일 CSV 파일 처리"""
        logger.info(f"\n   📄 Processing: {csv_file.name}")
        self.stats['total_files'] += 1
        
        # CSV 읽기
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            # 헤더에서 채널 목록 추출
            fieldnames = reader.fieldnames
            if not fieldnames or 'timestamp' not in fieldnames:
                raise ValueError(f"Invalid CSV format: missing 'timestamp' column")
            
            # 원본 채널 ID (CSV 헤더 그대로)
            channel_ids_original = [col for col in fieldnames if col != 'timestamp']
            logger.info(f"      📊 Columns: {len(channel_ids_original)} channels")
            
            # 채널을 테이블별로 분류 (normalize된 ID 사용)
            channels_by_table, channel_mapping = self.classify_channels(channel_ids_original)
            
            # 매칭된 채널 총 수 확인
            total_matched = sum(len(chs) for chs in channels_by_table.values())
            if total_matched == 0:
                logger.error(f"      ❌ No channels matched! All {len(channel_ids_original)} channels are unknown.")
                logger.error(f"         Sample unmapped channels: {channel_ids_original[:5]}")
                raise ValueError(f"No channels matched for {csv_file.name}")
            
            if total_matched < len(channel_ids_original):
                unmapped_count = len(channel_ids_original) - total_matched
                logger.warning(f"      ⚠️ {unmapped_count}/{len(channel_ids_original)} channels not mapped (will be skipped)")
            
            # 테이블별 통계 및 Coverage 확인
            for table_type, channels in channels_by_table.items():
                if channels:
                    # 테이블의 전체 컬럼 수 조회
                    table_name = f"tbl_data_timeseries_{imo_number.lower()}_{table_type}"
                    table_col_count = self.get_table_column_count(table_name)
                    
                    csv_col_count = len(channels)
                    coverage = (csv_col_count / table_col_count * 100) if table_col_count > 0 else 0
                    
                    logger.info(f"      - Table {table_type}: {csv_col_count}/{table_col_count} channels ({coverage:.1f}% coverage)")
                    
                    # 정보: Coverage가 낮으면 (경고 아님, 정보)
                    if coverage < 50 and table_col_count > 0 and not self.dry_run:
                        unmapped_count = table_col_count - csv_col_count
                        logger.info(f"         📊 Partial update: {unmapped_count} columns not in CSV (will be NULL for new rows, unchanged for existing rows)")
                else:
                    logger.debug(f"      - Table {table_type}: 0 channels")
            
            # 데이터 처리
            rows_processed = 0
            batch_size = 1000
            batch_data = {
                '1': [],
                '2': [],
                '3': []
            }
            
            for row in reader:
                timestamp_str = row['timestamp']
                
                # timestamp 파싱
                try:
                    timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    logger.warning(f"      ⚠️ Invalid timestamp: {timestamp_str}, skipping row")
                    continue
                
                # 테이블별로 데이터 준비
                for table_type, table_channels in channels_by_table.items():
                    # 이 테이블에 매칭되는 채널이 없으면 skip
                    if not table_channels:
                        continue
                    
                    row_data = {'created_time': timestamp}
                    has_valid_data = False
                    
                    for normalized_channel_id in table_channels:
                        # CSV의 원본 컬럼명으로 조회 (매핑 사용!)
                        original_channel_id = channel_mapping[normalized_channel_id]
                        value_str = row.get(original_channel_id, '')
                        
                        if value_str and value_str.strip():
                            try:
                                value = float(value_str)
                                # DB에는 normalized ID를 컬럼명으로 사용
                                row_data[normalized_channel_id] = value
                                has_valid_data = True  # 유효한 데이터가 하나라도 있음
                            except ValueError:
                                # 변환 실패 시 None
                                logger.debug(f"         ⚠️ Failed to convert '{value_str}' to float for {normalized_channel_id}")
                                row_data[normalized_channel_id] = None
                        else:
                            row_data[normalized_channel_id] = None
                    
                    # 유효한 데이터가 하나라도 있을 때만 추가
                    # (created_time만 있는 빈 row 방지)
                    if has_valid_data:
                        batch_data[table_type].append(row_data)
                
                rows_processed += 1
                
                # 배치 처리
                if rows_processed % batch_size == 0:
                    self.upsert_batch_data(imo_number, batch_data, channels_by_table)
                    batch_data = {'1': [], '2': [], '3': []}
                    logger.info(f"      ⏳ Processed {rows_processed} rows...")
            
            # 남은 데이터 처리
            if any(batch_data.values()):
                self.upsert_batch_data(imo_number, batch_data, channels_by_table)
            
            self.stats['csv_rows_read'] += rows_processed
            logger.success(f"      ✅ Completed: {rows_processed} CSV rows processed")
    
    def classify_channels(self, channel_ids: List[str]) -> Tuple[Dict[str, List[str]], Dict[str, str]]:
        """
        채널을 테이블별로 분류
        
        Returns:
            (channels_by_table, channel_mapping)
            - channels_by_table: 테이블별 normalized 채널 리스트
            - channel_mapping: normalized_id -> original_id 매핑
        """
        channels_by_table = {
            '1': [],  # auxiliary_systems
            '2': [],  # engine_generator
            '3': []   # navigation_ship
        }
        
        # Normalized -> Original 매핑
        channel_mapping = {}
        unmapped_channels = []
        
        for original_id in channel_ids:
            # 공백 제거 및 normalize (중요!)
            normalized_id = original_id.strip()
            
            table_type = self.channel_router.get_table_type(normalized_id)
            if table_type:
                channels_by_table[table_type].append(normalized_id)
                # 매핑 저장: normalized -> original (CSV 조회용)
                channel_mapping[normalized_id] = original_id
            else:
                unmapped_channels.append(original_id)
        
        # Unmapped channels 상세 로그
        if unmapped_channels:
            logger.warning(f"         ⚠️ {len(unmapped_channels)} unmapped channels (will be skipped):")
            # 처음 10개만 샘플로 표시
            for ch in unmapped_channels[:10]:
                logger.warning(f"            - '{ch}'")
            if len(unmapped_channels) > 10:
                logger.warning(f"            ... and {len(unmapped_channels) - 10} more")
        
        return channels_by_table, channel_mapping
    
    def get_table_column_count(self, table_name: str) -> int:
        """
        테이블의 데이터 컬럼 수 조회 (created_time 제외)
        캐싱하여 반복 조회 방지
        """
        # 캐시 확인
        if table_name in self.table_column_count_cache:
            return self.table_column_count_cache[table_name]
        
        # Dry-run 모드에서는 channel_router에서 예상 컬럼 수 가져오기
        if self.dry_run:
            # 테이블 타입 추출 (tbl_data_timeseries_imo9976903_1 -> '1')
            table_type = table_name.split('_')[-1]
            if table_type in ['1', '2', '3']:
                col_count = len(self.channel_router.get_all_channels_by_table(table_type))
                self.table_column_count_cache[table_name] = col_count
                return col_count
            return 0
        
        try:
            query = """
                SELECT COUNT(*) as col_count
                FROM information_schema.columns
                WHERE table_schema = 'tenant'
                  AND table_name = %s
                  AND column_name != 'created_time'
            """
            
            result = db_manager.execute_query(query, (table_name,))
            if result:
                col_count = result[0]['col_count']
                # 캐시 저장
                self.table_column_count_cache[table_name] = col_count
                return col_count
            else:
                return 0
                
        except Exception as e:
            logger.warning(f"   ⚠️ Could not get column count for {table_name}: {e}")
            return 0
    
    def check_table_columns(self, imo_number: str):
        """테이블의 실제 컬럼 개수 확인 및 경고"""
        try:
            for table_type in ['1', '2', '3']:
                table_name = f"tbl_data_timeseries_{imo_number.lower()}_{table_type}"
                col_count = self.get_table_column_count(table_name)
                logger.info(f"   📊 Table {table_type}: {col_count} data columns")
                    
        except Exception as e:
            logger.warning(f"   ⚠️ Could not check table columns: {e}")
    
    def upsert_batch_data(self, imo_number: str, batch_data: Dict[str, List[Dict]], 
                          channels_by_table: Dict[str, List[str]]):
        """배치 데이터를 각 테이블에 upsert"""
        for table_type, rows in batch_data.items():
            if not rows:
                continue
            
            table_name = f"tbl_data_timeseries_{imo_number.lower()}_{table_type}"
            channel_list = channels_by_table[table_type]
            
            self.upsert_to_table(table_name, rows, channel_list, table_type)
    
    def upsert_to_table(self, table_name: str, rows: List[Dict], channel_list: List[str], table_type: str):
        """
        특정 테이블에 데이터 upsert
        
        Args:
            table_name: 테이블명
            rows: upsert할 row 데이터 리스트
            channel_list: 채널 ID 리스트
            table_type: 테이블 타입 ('1', '2', '3')
        """
        if not rows:
            return
        
        # Dry-run 모드
        if self.dry_run:
            # 테이블별 통계 업데이트
            stat_key = f'table_{table_type}_rows'
            self.stats[stat_key] += len(rows)
            logger.debug(f"         🔍 [DRY-RUN] Would upsert {len(rows)} rows to {table_name}")
            return
        
        # SQL 쿼리 생성
        # 컬럼명 quoting (특수문자 포함)
        quoted_columns = [f'"{col}"' for col in channel_list]
        all_columns = ['created_time'] + quoted_columns
        
        # INSERT 구문
        columns_str = ', '.join(all_columns)
        placeholders = ', '.join(['%s'] * len(all_columns))
        
        # UPDATE 구문 (created_time 제외)
        # NULL이 아닌 값만 UPDATE (빈 값은 기존 값 유지)
        update_set_parts = []
        for col in channel_list:
            # CASE WHEN을 사용해서 NULL이 아닐 때만 업데이트
            update_set_parts.append(
                f'"{col}" = CASE WHEN EXCLUDED."{col}" IS NOT NULL THEN EXCLUDED."{col}" ELSE {table_name}."{col}" END'
            )
        update_set = ', '.join(update_set_parts)
        
        upsert_query = f"""
            INSERT INTO tenant.{table_name} ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT (created_time) 
            DO UPDATE SET {update_set}
        """
        
        # 데이터 준비
        values_list = []
        for row in rows:
            values = [row['created_time']]
            for channel_id in channel_list:
                values.append(row.get(channel_id))
            values_list.append(tuple(values))
        
        # 실행
        try:
            conn = db_manager.get_connection()
            cursor = conn.cursor()
            
            # executemany로 배치 처리
            from psycopg2.extras import execute_batch
            execute_batch(cursor, upsert_query, values_list, page_size=1000)
            
            conn.commit()
            cursor.close()
            db_manager.return_connection(conn)
            
            # 테이블별 통계 업데이트
            stat_key = f'table_{table_type}_rows'
            self.stats[stat_key] += len(rows)
            
            # Coverage 정보와 함께 로깅
            channel_count = len(channel_list)
            logger.info(f"         ✅ Upserted {len(rows)} rows to {table_name}")
            logger.info(f"            Affected columns: {channel_count} (other columns: NULL for INSERT, unchanged for UPDATE)")
            
        except Exception as e:
            logger.error(f"         ❌ Upsert failed for {table_name}: {e}")
            logger.error(f"         Sample row: {rows[0] if rows else 'N/A'}")
            if 'conn' in locals():
                conn.rollback()
                db_manager.return_connection(conn)
            raise
    
    def print_summary(self):
        """처리 결과 요약 출력"""
        logger.info(f"\n{'='*80}")
        logger.info("📊 UPSERT SUMMARY")
        logger.info(f"{'='*80}")
        logger.info(f"📁 Files:")
        logger.info(f"   Total found: {self.stats['total_files']}")
        logger.info(f"   ✅ Processed: {self.stats['processed_files']}")
        logger.info(f"   ❌ Failed: {self.stats['failed_files']}")
        logger.info(f"")
        logger.info(f"📊 CSV Rows:")
        logger.info(f"   Total read from CSV: {self.stats['csv_rows_read']:,}")
        logger.info(f"")
        logger.info(f"💾 DB Rows Upserted (per table):")
        logger.info(f"   Table 1 (Auxiliary): {self.stats['table_1_rows']:,}")
        logger.info(f"   Table 2 (Engine/Generator): {self.stats['table_2_rows']:,}")
        logger.info(f"   Table 3 (Navigation/Ship): {self.stats['table_3_rows']:,}")
        total_db_rows = self.stats['table_1_rows'] + self.stats['table_2_rows'] + self.stats['table_3_rows']
        logger.info(f"   Total DB rows: {total_db_rows:,}")
        logger.info(f"")
        logger.info(f"ℹ️  Note:")
        logger.info(f"   - 1 CSV row → up to 3 DB rows (one per table)")
        logger.info(f"   - For EXISTING rows: CSV columns updated, others unchanged")
        logger.info(f"   - For NEW rows: CSV columns filled, others set to NULL")
        logger.info(f"{'='*80}")


def main():
    """메인 실행 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Upsert CSV migration data to wide tables')
    parser.add_argument(
        '--dir', 
        type=str, 
        default='migration_data',
        help='Base directory containing ship folders (default: migration_data)'
    )
    parser.add_argument(
        '--ship',
        type=str,
        help='Process only specific ship code (e.g., H2546)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Dry-run mode: check files and channels without inserting data'
    )
    
    args = parser.parse_args()
    
    try:
        upserter = CSVMigrationUpserter(base_dir=args.dir, dry_run=args.dry_run)
        
        if args.ship:
            # 특정 선박만 처리
            ship_folder = Path(args.dir) / args.ship
            if not ship_folder.exists():
                logger.error(f"❌ Ship folder not found: {ship_folder}")
                return 1
            
            if args.ship not in SHIP_MAPPING:
                logger.error(f"❌ Unknown ship code: {args.ship}")
                return 1
            
            imo_number = SHIP_MAPPING[args.ship]
            logger.info(f"🚢 Processing single ship: {args.ship} → {imo_number}")
            upserter.process_ship_folder(ship_folder, imo_number)
        else:
            # 모든 선박 처리
            upserter.process_all_ships()
        
        return 0
        
    except KeyboardInterrupt:
        logger.warning("\n⚠️ Process interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"\n❌ Fatal error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    exit(main())

