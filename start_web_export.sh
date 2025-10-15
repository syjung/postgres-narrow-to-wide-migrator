#!/bin/bash

# Web Export Service 시작 스크립트 (백그라운드 실행)

PID_FILE="web_export.pid"

# Flask 설치 확인
if ! python3 -c "import flask" 2>/dev/null; then
    echo "📦 Installing Flask..."
    pip3 install flask==3.0.0
fi

# 이미 실행 중인지 확인
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if ps -p $PID > /dev/null 2>&1; then
        echo "⚠️  Web export service is already running (PID: $PID)"
        echo "📝 Access: http://localhost:8888"
        echo "💡 To stop: ./stop_web_export.sh"
        exit 1
    else
        echo "🧹 Removing stale PID file..."
        rm -f "$PID_FILE"
    fi
fi

# 백그라운드로 서비스 시작
echo "🚀 Starting web export service in background..."
echo "📝 Port: 8888 (configurable in config.py)"
echo "📝 Access: http://localhost:8888"
echo "📝 Logs: logs/web_export.log"
echo ""

# 로그 디렉토리 확인
mkdir -p logs

# nohup으로 백그라운드 실행
nohup python3 web_export_service.py > logs/web_export.log 2>&1 &

# PID 저장
echo $! > "$PID_FILE"

echo "✅ Web export service started (PID: $(cat $PID_FILE))"
echo ""
echo "Commands:"
echo "  📊 Check logs:  tail -f logs/web_export.log"
echo "  🛑 Stop:        ./stop_web_export.sh"
echo "  📝 Access:      http://localhost:8888"
