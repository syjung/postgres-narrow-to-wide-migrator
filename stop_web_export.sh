#!/bin/bash

# Web Export Service 중지 스크립트

PID_FILE="web_export.pid"

if [ ! -f "$PID_FILE" ]; then
    echo "⚠️  Web export service is not running (PID file not found)"
    exit 1
fi

PID=$(cat "$PID_FILE")

if ! ps -p $PID > /dev/null 2>&1; then
    echo "⚠️  Process $PID is not running"
    echo "🧹 Removing stale PID file..."
    rm -f "$PID_FILE"
    exit 1
fi

echo "🛑 Stopping web export service (PID: $PID)..."
kill $PID

# 프로세스가 종료될 때까지 대기 (최대 5초)
for i in {1..5}; do
    if ! ps -p $PID > /dev/null 2>&1; then
        echo "✅ Web export service stopped successfully"
        rm -f "$PID_FILE"
        exit 0
    fi
    sleep 1
done

# 강제 종료
echo "⚠️  Process did not stop gracefully, forcing..."
kill -9 $PID
sleep 1

if ! ps -p $PID > /dev/null 2>&1; then
    echo "✅ Web export service force stopped"
    rm -f "$PID_FILE"
    exit 0
else
    echo "❌ Failed to stop web export service"
    exit 1
fi

