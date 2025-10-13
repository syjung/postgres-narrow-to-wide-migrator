"""
실패한 청크 재처리 스크립트
post_proc.csv 파일을 읽어서 실패한 청크들을 재처리합니다.
"""
import csv
import sys
import time
import argparse
import os
from datetime import datetime
from typing import List, Tuple, Dict, Any
from loguru import logger
from database import db_manager
from table_generator import table_generator
from chunked_migration_strategy import chunked_migration_strategy
from thread_logger import get_ship_thread_logger
from config import migration_config


def setup_reprocess_logger():
    """재처리 전용 로거 설정"""
    # 기존 핸들러 제거 (선택적)
    # logger.remove()
    
    # logs 디렉토리가 없으면 생성
    os.makedirs("logs", exist_ok=True)
    
    # 타임스탬프가 포함된 로그 파일명
    log_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 재처리 전용 로그 파일 추가
    logger.add(
        f"logs/reprocess_{log_timestamp}.log",
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
        level="INFO",
        rotation="100 MB",
        retention="30 days",
        compression="zip"
    )
    
    # 최신 로그 파일 링크 (심볼릭 링크)
    latest_log = "logs/reprocess_latest.log"
    try:
        if os.path.exists(latest_log):
            os.remove(latest_log)
        os.symlink(f"reprocess_{log_timestamp}.log", latest_log)
    except Exception:
        pass  # Windows에서는 실패할 수 있음
    
    logger.info(f"📝 Reprocess log file: logs/reprocess_{log_timestamp}.log")
    return f"logs/reprocess_{log_timestamp}.log"


class FailedChunkReprocessor:
    """실패한 청크 재처리 클래스"""
    
    def __init__(self, csv_file: str = "post_proc.csv", dry_run: bool = False):
        self.csv_file = csv_file
        self.chunked_strategy = chunked_migration_strategy
        self.dry_run = dry_run
        
        # 통계 정보
        self.total_chunks = 0
        self.successful_chunks = 0
        self.failed_chunks = 0
        self.skipped_chunks = 0
        self.total_records = 0
        
        # 실패한 청크 추적
        self.failed_chunk_details: List[Dict[str, Any]] = []
    
    def load_failed_chunks(self) -> List[Tuple[str, datetime, datetime]]:
        """
        CSV 파일에서 실패한 청크 정보를 로드합니다.
        
        Returns:
            List of (ship_id, start_time, end_time) tuples
        """
        logger.info(f"📂 Loading failed chunks from {self.csv_file}")
        
        chunks = []
        try:
            with open(self.csv_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                for line_num, row in enumerate(reader, start=1):
                    if not row or len(row) != 3:
                        logger.warning(f"⚠️ Skipping invalid line {line_num}: {row}")
                        continue
                    
                    ship_id = row[0].strip()
                    start_time_str = row[1].strip()
                    end_time_str = row[2].strip()
                    
                    try:
                        # Parse datetime strings
                        start_time = datetime.fromisoformat(start_time_str)
                        end_time = datetime.fromisoformat(end_time_str)
                        
                        chunks.append((ship_id, start_time, end_time))
                    except ValueError as e:
                        logger.error(f"❌ Failed to parse datetime on line {line_num}: {e}")
                        continue
            
            logger.info(f"✅ Loaded {len(chunks)} failed chunks")
            return chunks
            
        except FileNotFoundError:
            logger.error(f"❌ File not found: {self.csv_file}")
            return []
        except Exception as e:
            logger.error(f"❌ Error loading CSV file: {e}")
            return []
    
    def reprocess_chunk(self, ship_id: str, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """
        단일 청크를 재처리합니다.
        
        Args:
            ship_id: 선박 ID
            start_time: 청크 시작 시간
            end_time: 청크 종료 시간
            
        Returns:
            처리 결과 딕셔너리
        """
        thread_logger = get_ship_thread_logger(ship_id)
        
        # Dry-run mode
        if self.dry_run:
            thread_logger.info(f"🔍 [DRY-RUN] Would reprocess chunk: {ship_id} [{start_time} to {end_time}]")
            table_name = f"tbl_data_timeseries_{ship_id.lower()}"
            thread_logger.info(f"🔍 [DRY-RUN] Target table: {table_name}")
            return {
                'status': 'completed',
                'records_processed': 0,
                'message': 'Dry-run mode - no actual processing'
            }
        
        thread_logger.info(f"🔄 Reprocessing chunk: {ship_id} [{start_time} to {end_time}]")
        
        try:
            # Get target table name
            table_name = f"tbl_data_timeseries_{ship_id.lower()}"
            
            # Ensure table exists
            thread_logger.info(f"🔍 Checking if table exists: {table_name}")
            if not table_generator.ensure_ship_table_exists(ship_id):
                thread_logger.error(f"❌ Failed to ensure table exists: {table_name}")
                return {
                    'status': 'failed',
                    'error': 'Failed to ensure table exists'
                }
            
            # Migrate chunk using chunked strategy
            result = self.chunked_strategy.migrate_chunk(
                ship_id=ship_id,
                start_time=start_time,
                end_time=end_time,
                table_name=table_name,
                thread_logger=thread_logger
            )
            
            return result
            
        except Exception as e:
            thread_logger.error(f"❌ Failed to reprocess chunk: {e}")
            return {
                'status': 'failed',
                'error': str(e)
            }
    
    def reprocess_all(self, delay_seconds: float = 0.5) -> Dict[str, Any]:
        """
        모든 실패한 청크를 재처리합니다.
        
        Args:
            delay_seconds: 각 청크 처리 사이의 지연 시간 (초)
            
        Returns:
            전체 재처리 결과
        """
        logger.info("=" * 80)
        if self.dry_run:
            logger.info("🔍 Starting failed chunk reprocessing (DRY-RUN MODE)")
            logger.warning("⚠️  This is a dry-run - no actual processing will be done")
        else:
            logger.info("🚀 Starting failed chunk reprocessing")
        logger.info("=" * 80)
        
        # Load failed chunks
        chunks = self.load_failed_chunks()
        
        if not chunks:
            logger.warning("⚠️ No chunks to reprocess")
            return {
                'status': 'completed',
                'total_chunks': 0,
                'successful_chunks': 0,
                'failed_chunks': 0,
                'skipped_chunks': 0,
                'total_records': 0
            }
        
        self.total_chunks = len(chunks)
        logger.info(f"📊 Total chunks to reprocess: {self.total_chunks}")
        
        # Process each chunk
        start_time = time.time()
        
        for idx, (ship_id, chunk_start, chunk_end) in enumerate(chunks, start=1):
            logger.info(f"\n{'=' * 80}")
            logger.info(f"📦 Processing chunk {idx}/{self.total_chunks}")
            logger.info(f"   Ship: {ship_id}")
            logger.info(f"   Range: {chunk_start} to {chunk_end}")
            logger.info(f"{'=' * 80}")
            
            # Reprocess chunk
            result = self.reprocess_chunk(ship_id, chunk_start, chunk_end)
            
            # Update statistics
            if result.get('status') == 'completed':
                self.successful_chunks += 1
                records = result.get('records_processed', 0)
                self.total_records += records
                logger.success(f"✅ Chunk {idx}/{self.total_chunks} completed: {records} records")
            elif result.get('status') == 'skipped':
                self.skipped_chunks += 1
                logger.info(f"⏭️ Chunk {idx}/{self.total_chunks} skipped: {result.get('message', 'No data')}")
            else:
                self.failed_chunks += 1
                error = result.get('error', 'Unknown error')
                logger.error(f"❌ Chunk {idx}/{self.total_chunks} failed: {error}")
                
                # Track failed chunk
                self.failed_chunk_details.append({
                    'ship_id': ship_id,
                    'start_time': chunk_start.isoformat(),
                    'end_time': chunk_end.isoformat(),
                    'error': error
                })
            
            # Progress update
            progress = (idx / self.total_chunks) * 100
            logger.info(f"📊 Progress: {progress:.1f}% ({idx}/{self.total_chunks})")
            logger.info(f"📊 Stats: ✅ {self.successful_chunks} | ❌ {self.failed_chunks} | ⏭️ {self.skipped_chunks}")
            
            # Delay between chunks to avoid overwhelming the database
            if delay_seconds > 0 and idx < self.total_chunks:
                time.sleep(delay_seconds)
        
        # Final statistics
        elapsed_time = time.time() - start_time
        
        logger.info("\n" + "=" * 80)
        logger.info("🏁 Reprocessing completed")
        logger.info("=" * 80)
        logger.info(f"📊 Total chunks: {self.total_chunks}")
        logger.info(f"✅ Successful: {self.successful_chunks}")
        logger.info(f"❌ Failed: {self.failed_chunks}")
        logger.info(f"⏭️ Skipped: {self.skipped_chunks}")
        logger.info(f"📈 Total records processed: {self.total_records:,}")
        logger.info(f"⏱️ Total time: {elapsed_time:.2f} seconds")
        
        if self.total_chunks > 0:
            logger.info(f"📊 Success rate: {(self.successful_chunks/self.total_chunks)*100:.1f}%")
            logger.info(f"⚡ Average time per chunk: {elapsed_time/self.total_chunks:.2f} seconds")
        
        # Save failed chunks to a new file if any
        if self.failed_chunk_details:
            failed_file = "failed_chunks_after_reprocess.csv"
            logger.warning(f"⚠️ Saving {len(self.failed_chunk_details)} failed chunks to {failed_file}")
            
            try:
                with open(failed_file, 'w', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    for detail in self.failed_chunk_details:
                        writer.writerow([
                            detail['ship_id'],
                            detail['start_time'],
                            detail['end_time']
                        ])
                logger.info(f"💾 Failed chunks saved to {failed_file}")
            except Exception as e:
                logger.error(f"❌ Failed to save failed chunks: {e}")
        
        return {
            'status': 'completed',
            'total_chunks': self.total_chunks,
            'successful_chunks': self.successful_chunks,
            'failed_chunks': self.failed_chunks,
            'skipped_chunks': self.skipped_chunks,
            'total_records': self.total_records,
            'elapsed_time': elapsed_time,
            'failed_details': self.failed_chunk_details
        }


def main():
    """메인 함수"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='실패한 청크 재처리 스크립트',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예제:
  # 기본 실행 (post_proc.csv 사용)
  python3 reprocess_failed_chunks.py
  
  # 다른 CSV 파일 사용
  python3 reprocess_failed_chunks.py -f my_failed_chunks.csv
  
  # Dry-run 모드 (실제 처리 없이 확인만)
  python3 reprocess_failed_chunks.py --dry-run
  
  # 지연 시간 조정
  python3 reprocess_failed_chunks.py -d 1.0
  
  # 선박별 필터링 (특정 선박만 처리)
  python3 reprocess_failed_chunks.py --ship IMO9986063
        """
    )
    
    parser.add_argument(
        '-f', '--file',
        default='post_proc.csv',
        help='실패한 청크 CSV 파일 경로 (기본값: post_proc.csv)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Dry-run 모드 (실제 처리 없이 확인만)'
    )
    
    parser.add_argument(
        '-d', '--delay',
        type=float,
        default=0.5,
        help='청크 처리 사이의 지연 시간 (초, 기본값: 0.5)'
    )
    
    parser.add_argument(
        '--ship',
        type=str,
        help='특정 선박만 처리 (예: IMO9986063)'
    )
    
    args = parser.parse_args()
    
    # 재처리 전용 로거 설정
    log_file = setup_reprocess_logger()
    
    logger.info("=" * 80)
    logger.info("📋 Failed Chunk Reprocessor")
    logger.info("=" * 80)
    logger.info(f"📂 Input file: {args.file}")
    logger.info(f"📝 Log file: {log_file}")
    logger.info(f"⏱️  Delay: {args.delay} seconds")
    
    if args.dry_run:
        logger.warning("🔍 Running in DRY-RUN mode - no actual processing")
    
    if args.ship:
        logger.info(f"🚢 Filtering by ship: {args.ship}")
    
    # Create reprocessor
    reprocessor = FailedChunkReprocessor(csv_file=args.file, dry_run=args.dry_run)
    
    # Filter by ship if specified
    if args.ship:
        original_load = reprocessor.load_failed_chunks
        def filtered_load():
            chunks = original_load()
            filtered = [(s, st, et) for s, st, et in chunks if s == args.ship]
            logger.info(f"📊 Filtered to {len(filtered)} chunks for {args.ship}")
            return filtered
        reprocessor.load_failed_chunks = filtered_load
    
    # Reprocess all failed chunks
    result = reprocessor.reprocess_all(delay_seconds=args.delay)
    
    # Exit with appropriate status code
    if args.dry_run:
        logger.success("✅ Dry-run completed successfully!")
        sys.exit(0)
    elif result['failed_chunks'] == 0:
        logger.success("🎉 All chunks processed successfully!")
        sys.exit(0)
    else:
        logger.warning(f"⚠️ {result['failed_chunks']} chunks still failed")
        sys.exit(1)


if __name__ == "__main__":
    main()

