#!/bin/bash

# 배치 데이터 마이그레이션 스크립트
# 이 스크립트는 모든 과거 데이터를 처리한 후 자동으로 종료됩니다.

set -e

# 스크립트 디렉토리로 이동
cd "$(dirname "$0")"

# 로그 디렉토리 생성
mkdir -p logs

# PostgreSQL 최적화 설정 (배치 처리용)
export PGOPTIONS="
    -c work_mem=32MB
    -c maintenance_work_mem=128MB
    -c max_parallel_workers_per_gather=0
    -c max_parallel_workers=0
    -c max_parallel_maintenance_workers=0
    -c random_page_cost=1.1
"

# PID 파일 경로
PID_FILE="logs/batch.pid"
LOG_FILE="logs/batch.log"

# 함수 정의
start_batch() {
    echo "🚀 Starting batch data migration..."
    
    # 이미 실행 중인지 확인
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p "$PID" > /dev/null 2>&1; then
            echo "❌ Batch migration is already running (PID: $PID)"
            echo "   Use './stop_batch.sh' to stop it first"
            exit 1
        else
            echo "⚠️  Stale PID file found, removing..."
            rm -f "$PID_FILE"
        fi
    fi
    
    # 배치 마이그레이션 시작
    echo "📊 Starting batch migration..."
    nohup python3 -c "
import sys
sys.path.append('.')
from main import MigrationManager
from loguru import logger
import time

# 배치 처리용 로그 설정
# 로그 파일명 규칙:
# - batch.log: 현재 로그 (날짜 없음)
# - batch_YYYY-MM-DD.log.gz: 이전 로그 (간단한 날짜 형식)
logger.remove()

# 간단한 날짜별 로그 로테이션 설정
from clean_log_rotation import setup_clean_log_rotation
current_log_file = setup_clean_log_rotation('logs/batch.log', retention_days=30)

logger.add(current_log_file, 
           format='{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} - {message}',
           level='INFO')

logger.add(sys.stdout, 
           format='<green>{time:HH:mm:ss}</green> | <level>{level}</level> | {message}',
           level='INFO')

logger.info('🚀 Batch migration starting...')

try:
    manager = MigrationManager()
    
    # 배치 마이그레이션 시작 (chunked mode)
    logger.info('📊 Starting batch data migration (chunked mode)...')
    result = manager.run_full_migration()
    
    if result:
        logger.info('✅ Batch migration completed successfully!')
        logger.info('📊 All historical data has been migrated')
    else:
        logger.error('❌ Batch migration failed')
        sys.exit(1)
        
except KeyboardInterrupt:
    logger.info('🛑 Batch migration stopped by user')
except Exception as e:
    logger.error(f'❌ Batch migration error: {e}')
    raise
" > "$LOG_FILE" 2>&1 &
    
    # PID 저장
    echo $! > "$PID_FILE"
    
    echo "✅ Batch migration started successfully!"
    echo "   PID: $(cat "$PID_FILE")"
    echo "   Log: $LOG_FILE"
    echo ""
    echo "📊 To monitor progress:"
    echo "   ./view_logs.sh -f batch"
    echo ""
    echo "🛑 To stop:"
    echo "   ./stop_batch.sh"
}

start_concurrent() {
    echo "🚀 Starting concurrent migration (batch + real-time)..."
    
    # 이미 실행 중인지 확인
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p "$PID" > /dev/null 2>&1; then
            echo "❌ Concurrent migration is already running (PID: $PID)"
            echo "   Use './stop_batch.sh' to stop it first"
            exit 1
        else
            echo "⚠️  Stale PID file found, removing..."
            rm -f "$PID_FILE"
        fi
    fi
    
    # 동시 마이그레이션 시작
    echo "📊 Starting concurrent migration..."
    nohup python3 -c "
import sys
sys.path.append('.')
from main import MigrationManager
from loguru import logger
import time

# 동시 처리용 로그 설정
logger.remove()
logger.add('logs/batch.log', 
           format='{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} - {message}',
           level='INFO',
           rotation='100 MB',
           retention='7 days')

logger.add(sys.stdout, 
           format='<green>{time:HH:mm:ss}</green> | <level>{level}</level> | {message}',
           level='INFO')

logger.info('🚀 Concurrent migration starting...')

try:
    manager = MigrationManager()
    
    # 동시 마이그레이션 시작
    logger.info('📊 Starting concurrent migration (batch + real-time)...')
    manager.start_concurrent_migration(interval_minutes=1)
    
    # 계속 실행 (배치가 완료되어도 실시간은 계속)
    while True:
        time.sleep(60)
        logger.info('💓 Concurrent migration heartbeat...')
        
except KeyboardInterrupt:
    logger.info('🛑 Concurrent migration stopped by user')
except Exception as e:
    logger.error(f'❌ Concurrent migration error: {e}')
    raise
" > "$LOG_FILE" 2>&1 &
    
    # PID 저장
    echo $! > "$PID_FILE"
    
    echo "✅ Concurrent migration started successfully!"
    echo "   PID: $(cat "$PID_FILE")"
    echo "   Log: $LOG_FILE"
    echo ""
    echo "📊 To monitor progress:"
    echo "   ./view_logs.sh -f batch"
    echo ""
    echo "🛑 To stop:"
    echo "   ./stop_batch.sh"
}

start_parallel_batch() {
    echo "🚀 Starting parallel batch migration (ship-based threading)..."
    
    # 이미 실행 중인지 확인
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p "$PID" > /dev/null 2>&1; then
            echo "❌ Parallel batch migration is already running (PID: $PID)"
            echo "   Use './stop_batch.sh' to stop it first"
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
logger.remove()
logger.add('logs/batch.log', 
           format='{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} - {message}',
           level='INFO',
           rotation='100 MB',
           retention='7 days')

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
    echo "   ./view_logs.sh -f batch"
    echo ""
    echo "🛑 To stop:"
    echo "   ./stop_batch.sh"
}

# 메인 실행
case "${1:-batch}" in
    batch)
        start_batch
        ;;
    concurrent)
        start_concurrent
        ;;
    parallel)
        start_parallel_batch
        ;;
    *)
        echo "Usage: $0 [batch|concurrent|parallel]"
        echo ""
        echo "Commands:"
        echo "  batch       Start batch migration (processes all historical data then exits)"
        echo "  concurrent  Start concurrent migration (batch + real-time, runs continuously)"
        echo "  parallel    Start parallel batch migration (ship-based threading, then exits)"
        echo ""
        echo "Examples:"
        echo "  $0 batch       # Process all historical data sequentially"
        echo "  $0 concurrent  # Process historical data + real-time data"
        echo "  $0 parallel    # Process all historical data in parallel (faster)"
        echo ""
        exit 1
        ;;
esac
