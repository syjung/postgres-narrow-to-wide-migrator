#!/bin/bash

# 병렬 배치 마이그레이션 중지 스크립트

set -e

# 스크립트 디렉토리로 이동
cd "$(dirname "$0")"

# PID 파일 경로
PID_FILE="logs/parallel_batch.pid"

# 함수 정의
stop_parallel_batch() {
    echo "🛑 Stopping parallel batch migration..."
    
    if [ ! -f "$PID_FILE" ]; then
        echo "❌ No parallel batch migration process found (PID file not found)"
        exit 1
    fi
    
    PID=$(cat "$PID_FILE")
    
    if ! ps -p "$PID" > /dev/null 2>&1; then
        echo "⚠️  Parallel batch migration process not found (PID: $PID)"
        echo "   Removing stale PID file..."
        rm -f "$PID_FILE"
        exit 1
    fi
    
    echo "📊 Stopping parallel batch migration process (PID: $PID)..."
    
    # SIGTERM으로 정상 종료 시도
    kill -TERM "$PID"
    
    # 5초 대기
    sleep 5
    
    # 여전히 실행 중이면 강제 종료
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "⚠️  Process still running, forcing termination..."
        kill -KILL "$PID"
    fi
    
    # PID 파일 제거
    rm -f "$PID_FILE"
    
    echo "✅ Parallel batch migration stopped successfully!"
}

# 메인 실행
stop_parallel_batch
