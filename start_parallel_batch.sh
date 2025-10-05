#!/bin/bash

# 병렬 배치 데이터 마이그레이션 스크립트
# 선박별 스레드 기반으로 모든 과거 데이터를 병렬 처리한 후 자동으로 종료됩니다.

set -e

# 스크립트 디렉토리로 이동
cd "$(dirname "$0")"

# 로그 디렉토리 생성
mkdir -p logs

# Dynamic PostgreSQL optimization settings based on ship count
# This will be calculated at runtime based on target_ship_ids
export PGOPTIONS="
    -c work_mem=64MB
    -c maintenance_work_mem=256MB
    -c max_parallel_workers_per_gather=4
    -c max_parallel_workers=8
    -c max_parallel_maintenance_workers=4
    -c random_page_cost=1.1
    -c effective_cache_size=4GB
"

# PID 파일 경로
PID_FILE="logs/parallel_batch.pid"
LOG_FILE="logs/parallel_batch.log"

# 함수 정의
start_parallel_batch() {
    echo "🚀 Starting parallel batch data migration..."
    
    # 이미 실행 중인지 확인
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p "$PID" > /dev/null 2>&1; then
            echo "❌ Parallel batch migration is already running (PID: $PID)"
            echo "   Use './stop_parallel_batch.sh' to stop it first"
            exit 1
        else
            echo "⚠️  Stale PID file found, removing..."
            rm -f "$PID_FILE"
        fi
    fi
    
    # 병렬 배치 마이그레이션 시작
    echo "📊 Starting parallel batch migration..."
    nohup python3 -c "
import sys
sys.path.append('.')
from main import MigrationManager
from loguru import logger
import time

# 병렬 배치 처리용 로그 설정
# 로그 파일명 규칙:
# - parallel_batch.log: 현재 로그 (날짜 없음)
# - parallel_batch_YYYY-MM-DD.log.gz: 이전 로그 (간단한 날짜 형식)
logger.remove()

# 간단한 날짜별 로그 로테이션 설정
from clean_log_rotation import setup_clean_log_rotation
current_log_file = setup_clean_log_rotation('logs/parallel_batch.log', retention_days=30)

logger.add(current_log_file, 
           format='{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} - {message}',
           level='INFO')

logger.add(sys.stdout, 
           format='<green>{time:HH:mm:ss}</green> | <level>{level}</level> | {message}',
           level='INFO')

logger.info('🚀 Parallel batch migration starting...')

try:
    manager = MigrationManager()
    
    # 병렬 배치 마이그레이션 시작
    logger.info('📊 Starting parallel batch migration (ship-based threading)...')
    result = manager.start_parallel_batch_migration()
    
    if result and result.get('success', True):
        logger.info('✅ Parallel batch migration completed successfully!')
        logger.info(f'📊 Total ships processed: {result.get(\"total_ships\", 0)}')
        logger.info(f'📊 Completed ships: {result.get(\"completed_ships\", 0)}')
        logger.info(f'📊 Failed ships: {result.get(\"failed_ships\", 0)}')
        logger.info(f'📊 Total records processed: {result.get(\"total_records_processed\", 0)}')
        logger.info(f'📊 Total processing time: {result.get(\"total_processing_time\", 0):.2f}s')
        logger.info(f'📊 Average time per ship: {result.get(\"average_time_per_ship\", 0):.2f}s')
        
        # 성능 비교 정보
        if result.get('total_ships', 0) > 1:
            sequential_time = result.get('average_time_per_ship', 0) * result.get('total_ships', 0)
            parallel_time = result.get('total_processing_time', 0)
            speedup = sequential_time / parallel_time if parallel_time > 0 else 1
            logger.info(f'📈 Performance improvement: {speedup:.2f}x faster than sequential')
    else:
        logger.error('❌ Parallel batch migration failed')
        logger.error(f'Error: {result.get(\"error\", \"Unknown error\")}')
        sys.exit(1)
        
except KeyboardInterrupt:
    logger.info('🛑 Parallel batch migration stopped by user')
except Exception as e:
    logger.error(f'❌ Parallel batch migration error: {e}')
    raise
" > "$LOG_FILE" 2>&1 &
    
    # PID 저장
    echo $! > "$PID_FILE"
    
    echo "✅ Parallel batch migration started successfully!"
    echo "   PID: $(cat "$PID_FILE")"
    echo "   Log: $LOG_FILE"
    echo ""
    echo "📊 To monitor progress:"
    echo "   ./view_logs.sh -f parallel_batch"
    echo ""
    echo "🛑 To stop:"
    echo "   ./stop_parallel_batch.sh"
}

# 메인 실행
start_parallel_batch
