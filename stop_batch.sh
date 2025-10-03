#!/bin/bash

# 배치 데이터 마이그레이션 중지 스크립트

set -e

# 스크립트 디렉토리로 이동
cd "$(dirname "$0")"

# PID 파일 경로
PID_FILE="logs/batch.pid"

stop_batch() {
    echo "🛑 Stopping batch data migration..."
    
    if [ ! -f "$PID_FILE" ]; then
        echo "❌ No PID file found. Batch migration may not be running."
        exit 1
    fi
    
    PID=$(cat "$PID_FILE")
    
    if ! ps -p "$PID" > /dev/null 2>&1; then
        echo "⚠️  Process $PID is not running. Removing stale PID file."
        rm -f "$PID_FILE"
        exit 1
    fi
    
    echo "📊 Stopping process $PID..."
    
    # SIGTERM으로 정상 종료 시도
    kill -TERM "$PID"
    
    # 5초 대기
    sleep 5
    
    # 여전히 실행 중이면 SIGKILL
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "⚠️  Process still running, force killing..."
        kill -KILL "$PID"
    fi
    
    # PID 파일 제거
    rm -f "$PID_FILE"
    
    echo "✅ Batch migration stopped successfully!"
}

# 메인 실행
stop_batch
