#!/bin/bash

# 실시간 데이터 처리 스크립트
# 이 스크립트는 실시간 데이터를 계속 처리하며 종료되지 않습니다.

set -e

# 스크립트 디렉토리로 이동
cd "$(dirname "$0")"

# 로그 디렉토리 생성
mkdir -p logs

# PostgreSQL 최적화 설정 (실시간 처리용)
export PGOPTIONS="
    -c work_mem=16MB
    -c maintenance_work_mem=64MB
    -c max_parallel_workers_per_gather=0
    -c max_parallel_workers=0
    -c max_parallel_maintenance_workers=0
    -c random_page_cost=1.1
"

# PID 파일 경로
PID_FILE="logs/realtime.pid"
LOG_FILE="logs/realtime.log"

# 함수 정의
start_realtime() {
    echo "🚀 Starting real-time data processing..."
    
    # 이미 실행 중인지 확인
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p "$PID" > /dev/null 2>&1; then
            echo "❌ Real-time processing is already running (PID: $PID)"
            echo "   Use './stop_realtime.sh' to stop it first"
            exit 1
        else
            echo "⚠️  Stale PID file found, removing..."
            rm -f "$PID_FILE"
        fi
    fi
    
    # 실시간 처리 시작
    echo "📊 Starting real-time processor..."
    nohup python3 -c "
import sys
sys.path.append('.')
from main import MigrationManager
from loguru import logger
import time

# 실시간 처리용 로그 설정
# 로그 파일명 규칙:
# - realtime.log: 현재 로그 (날짜 없음)
# - realtime_YYYY-MM-DD.log.gz: 이전 로그 (간단한 날짜 형식)
logger.remove()

# 간단한 날짜별 로그 로테이션 설정
from clean_log_rotation import setup_clean_log_rotation
current_log_file = setup_clean_log_rotation('logs/realtime.log', retention_days=30)

logger.add(current_log_file, 
           format='{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} - {message}',
           level='INFO')

logger.add(sys.stdout, 
           format='<green>{time:HH:mm:ss}</green> | <level>{level}</level> | {message}',
           level='INFO')

logger.info('🚀 Real-time processor starting...')

try:
    manager = MigrationManager()
    
    # 실시간 처리 시작 (1분 간격)
    logger.info('📊 Starting real-time data processing (1-minute intervals)...')
    manager.start_dual_write_processing(interval_minutes=1)
    
    # 계속 실행
    while True:
        time.sleep(60)
        logger.info('💓 Real-time processor heartbeat...')
        
except KeyboardInterrupt:
    logger.info('🛑 Real-time processor stopped by user')
except Exception as e:
    logger.error(f'❌ Real-time processor error: {e}')
    raise
" > "$LOG_FILE" 2>&1 &
    
    # PID 저장
    echo $! > "$PID_FILE"
    
    echo "✅ Real-time processing started successfully!"
    echo "   PID: $(cat "$PID_FILE")"
    echo "   Log: $LOG_FILE"
    echo ""
    echo "📊 To monitor progress:"
    echo "   ./view_logs.sh -f realtime"
    echo ""
    echo "🛑 To stop:"
    echo "   ./stop_realtime.sh"
}

# 메인 실행
case "${1:-start}" in
    start)
        start_realtime
        ;;
    *)
        echo "Usage: $0 [start]"
        echo ""
        echo "Commands:"
        echo "  start    Start real-time data processing (default)"
        echo ""
        echo "Examples:"
        echo "  $0 start"
        echo ""
        exit 1
        ;;
esac
