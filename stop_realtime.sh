#!/bin/bash

# 실시간 데이터 처리 중지 스크립트

set -e

# 스크립트 디렉토리로 이동
cd "$(dirname "$0")"

# PID 파일 경로
PID_FILE="logs/realtime.pid"

stop_realtime() {
    echo "🛑 Stopping real-time data processing..."
    
    if [ ! -f "$PID_FILE" ]; then
        echo "❌ No PID file found. Real-time processing may not be running."
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
    
    echo "✅ Real-time processing stopped successfully!"
}

# 메인 실행
stop_realtime
