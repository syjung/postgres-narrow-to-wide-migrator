#!/bin/bash

# 로그 뷰어 스크립트 (실시간/배치 분리)

set -e

# 스크립트 디렉토리로 이동
cd "$(dirname "$0")"

# 로그 디렉토리 확인
if [ ! -d "logs" ]; then
    echo "❌ Logs directory not found. Run migration first."
    exit 1
fi

# 함수 정의
show_help() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -f, --follow TYPE    Follow specific log type (realtime|batch|all)"
    echo "  -t, --tail TYPE      Show last N lines of specific log type"
    echo "  -e, --error          Show only error messages"
    echo "  -s, --success        Show only success messages"
    echo "  -c, --count          Show log file sizes and line counts"
    echo "  -h, --help           Show this help message"
    echo ""
    echo "Log Types:"
    echo "  realtime            Real-time processing logs"
    echo "  batch               Batch migration logs"
    echo "  all                 All logs combined"
    echo ""
    echo "Examples:"
    echo "  $0 -f realtime      # Follow real-time logs"
    echo "  $0 -f batch         # Follow batch logs"
    echo "  $0 -t realtime 100  # Show last 100 lines of real-time logs"
    echo "  $0 -e               # Show all error messages"
    echo "  $0 -s               # Show all success messages"
    echo "  $0 -c               # Show log statistics"
}

follow_logs() {
    local log_type="$1"
    
    case "$log_type" in
        realtime)
            if [ -f "logs/realtime.log" ]; then
                echo "📊 Following real-time logs..."
                tail -f "logs/realtime.log"
            else
                echo "❌ Real-time log file not found"
                exit 1
            fi
            ;;
        batch)
            if [ -f "logs/batch.log" ]; then
                echo "📊 Following batch logs..."
                tail -f "logs/batch.log"
            else
                echo "❌ Batch log file not found"
                exit 1
            fi
            ;;
        all)
            echo "📊 Following all logs..."
            if [ -f "logs/realtime.log" ] && [ -f "logs/batch.log" ]; then
                tail -f "logs/realtime.log" "logs/batch.log"
            elif [ -f "logs/realtime.log" ]; then
                tail -f "logs/realtime.log"
            elif [ -f "logs/batch.log" ]; then
                tail -f "logs/batch.log"
            else
                echo "❌ No log files found"
                exit 1
            fi
            ;;
        *)
            echo "❌ Invalid log type: $log_type"
            echo "Valid types: realtime, batch, all"
            exit 1
            ;;
    esac
}

tail_logs() {
    local log_type="$1"
    local lines="${2:-50}"
    
    case "$log_type" in
        realtime)
            if [ -f "logs/realtime.log" ]; then
                echo "📊 Last $lines lines of real-time logs:"
                tail -n "$lines" "logs/realtime.log"
            else
                echo "❌ Real-time log file not found"
                exit 1
            fi
            ;;
        batch)
            if [ -f "logs/batch.log" ]; then
                echo "📊 Last $lines lines of batch logs:"
                tail -n "$lines" "logs/batch.log"
            else
                echo "❌ Batch log file not found"
                exit 1
            fi
            ;;
        all)
            echo "📊 Last $lines lines of all logs:"
            for log_file in logs/*.log; do
                if [ -f "$log_file" ]; then
                    echo "=== $(basename "$log_file") ==="
                    tail -n "$lines" "$log_file"
                    echo ""
                fi
            done
            ;;
        *)
            echo "❌ Invalid log type: $log_type"
            echo "Valid types: realtime, batch, all"
            exit 1
            ;;
    esac
}

show_errors() {
    echo "📊 Error messages from all logs:"
    echo "=================================="
    
    for log_file in logs/*.log; do
        if [ -f "$log_file" ]; then
            echo "=== $(basename "$log_file") ==="
            grep -i "error\|failed\|exception" "$log_file" || echo "No errors found"
            echo ""
        fi
    done
}

show_success() {
    echo "📊 Success messages from all logs:"
    echo "===================================="
    
    for log_file in logs/*.log; do
        if [ -f "$log_file" ]; then
            echo "=== $(basename "$log_file") ==="
            grep -i "success\|completed\|inserted.*rows" "$log_file" || echo "No success messages found"
            echo ""
        fi
    done
}

show_stats() {
    echo "📊 Log file statistics:"
    echo "======================"
    
    for log_file in logs/*.log; do
        if [ -f "$log_file" ]; then
            local size=$(du -h "$log_file" | cut -f1)
            local lines=$(wc -l < "$log_file")
            local errors=$(grep -c -i "error\|failed\|exception" "$log_file" 2>/dev/null || echo "0")
            local success=$(grep -c -i "success\|completed\|inserted.*rows" "$log_file" 2>/dev/null || echo "0")
            
            echo "$(basename "$log_file"):"
            echo "  Size: $size"
            echo "  Lines: $lines"
            echo "  Errors: $errors"
            echo "  Success: $success"
            echo ""
        fi
    done
}

# 기본값 설정
FOLLOW_TYPE=""
TAIL_TYPE=""
TAIL_LINES=""
SHOW_ERRORS=false
SHOW_SUCCESS=false
SHOW_STATS=false

# 인수 파싱
while [[ $# -gt 0 ]]; do
    case $1 in
        -f|--follow)
            FOLLOW_TYPE="$2"
            shift 2
            ;;
        -t|--tail)
            TAIL_TYPE="$2"
            TAIL_LINES="$3"
            shift 3
            ;;
        -e|--error)
            SHOW_ERRORS=true
            shift
            ;;
        -s|--success)
            SHOW_SUCCESS=true
            shift
            ;;
        -c|--count)
            SHOW_STATS=true
            shift
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            echo "❌ Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# 메인 실행
if [ -n "$FOLLOW_TYPE" ]; then
    follow_logs "$FOLLOW_TYPE"
elif [ -n "$TAIL_TYPE" ]; then
    tail_logs "$TAIL_TYPE" "$TAIL_LINES"
elif [ "$SHOW_ERRORS" = true ]; then
    show_errors
elif [ "$SHOW_SUCCESS" = true ]; then
    show_success
elif [ "$SHOW_STATS" = true ]; then
    show_stats
else
    # 기본: 모든 로그의 마지막 50줄 표시
    echo "📊 Recent log entries (last 50 lines):"
    echo "====================================="
    
    for log_file in logs/*.log; do
        if [ -f "$log_file" ]; then
            echo "=== $(basename "$log_file") ==="
            tail -n 50 "$log_file"
            echo ""
        fi
    done
fi
