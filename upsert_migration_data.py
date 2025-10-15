"""
CSV Migration Data Upserter
migration_data 폴더의 CSV 파일들을 읽어서 3개의 wide 테이블에 upsert
"""
import os
import csv
from datetime import datetime
from typing import Dict, List, Optional
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
            'total_rows': 0,
            'inserted_rows': 0,
            'updated_rows': 0
        }
        
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
            
            channel_ids = [col for col in fieldnames if col != 'timestamp']
            logger.info(f"      📊 Columns: {len(channel_ids)} channels")
            
            # 채널을 테이블별로 분류
            channels_by_table = self.classify_channels(channel_ids)
            
            # 매칭된 채널 총 수 확인
            total_matched = sum(len(chs) for chs in channels_by_table.values())
            if total_matched == 0:
                logger.error(f"      ❌ No channels matched! All {len(channel_ids)} channels are unknown.")
                logger.error(f"         Sample unmapped channels: {channel_ids[:5]}")
                raise ValueError(f"No channels matched for {csv_file.name}")
            
            if total_matched < len(channel_ids):
                unmapped_count = len(channel_ids) - total_matched
                logger.warning(f"      ⚠️ {unmapped_count}/{len(channel_ids)} channels not mapped (will be skipped)")
            
            # 테이블별 통계
            for table_type, channels in channels_by_table.items():
                logger.info(f"      - Table {table_type}: {len(channels)} channels")
            
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
                    
                    for channel_id in table_channels:
                        value_str = row.get(channel_id, '')
                        if value_str and value_str.strip():
                            try:
                                value = float(value_str)
                                row_data[channel_id] = value
                                has_valid_data = True  # 유효한 데이터가 하나라도 있음
                            except ValueError:
                                # 변환 실패 시 None
                                row_data[channel_id] = None
                        else:
                            row_data[channel_id] = None
                    
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
            
            self.stats['total_rows'] += rows_processed
            logger.success(f"      ✅ Completed: {rows_processed} rows processed")
    
    def classify_channels(self, channel_ids: List[str]) -> Dict[str, List[str]]:
        """채널을 테이블별로 분류"""
        channels_by_table = {
            '1': [],  # auxiliary_systems
            '2': [],  # engine_generator
            '3': []   # navigation_ship
        }
        
        for channel_id in channel_ids:
            table_type = self.channel_router.get_table_type(channel_id)
            if table_type:
                channels_by_table[table_type].append(channel_id)
            else:
                logger.debug(f"         ⚠️ Channel not mapped: {channel_id}")
        
        return channels_by_table
    
    def upsert_batch_data(self, imo_number: str, batch_data: Dict[str, List[Dict]], 
                          channels_by_table: Dict[str, List[str]]):
        """배치 데이터를 각 테이블에 upsert"""
        for table_type, rows in batch_data.items():
            if not rows:
                continue
            
            table_name = f"tbl_data_timeseries_{imo_number.lower()}_{table_type}"
            channel_list = channels_by_table[table_type]
            
            self.upsert_to_table(table_name, rows, channel_list)
    
    def upsert_to_table(self, table_name: str, rows: List[Dict], channel_list: List[str]):
        """특정 테이블에 데이터 upsert"""
        if not rows:
            return
        
        # Dry-run 모드
        if self.dry_run:
            self.stats['inserted_rows'] += len(rows)
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
        update_set = ', '.join([f'"{col}" = EXCLUDED."{col}"' for col in channel_list])
        
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
            
            self.stats['inserted_rows'] += len(rows)
            logger.debug(f"         ✅ Upserted {len(rows)} rows to {table_name}")
            
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
        logger.info(f"📁 Total files found: {self.stats['total_files']}")
        logger.info(f"✅ Successfully processed: {self.stats['processed_files']}")
        logger.info(f"❌ Failed: {self.stats['failed_files']}")
        logger.info(f"📊 Total rows processed: {self.stats['total_rows']:,}")
        logger.info(f"💾 Total rows upserted: {self.stats['inserted_rows']:,}")
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

