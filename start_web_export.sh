#!/bin/bash

# Web Export Service 시작 스크립트

echo "🌐 Starting Wide Table Data Export Web Service..."

# Flask 설치 확인
if ! python3 -c "import flask" 2>/dev/null; then
    echo "📦 Installing Flask..."
    pip3 install flask==3.0.0
fi

# 서비스 시작
echo "🚀 Starting web service on http://0.0.0.0:5000"
echo "📝 Access from browser: http://localhost:5000"
echo ""
echo "Press Ctrl+C to stop"
echo "===================="

python3 web_export_service.py

