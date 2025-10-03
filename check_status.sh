#!/bin/bash

# PostgreSQL Narrow-to-Wide Migration Status Checker
# Enhanced version with detailed work progress and table statistics
# Usage: ./check_status.sh

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR"
PID_FILE="$PROJECT_DIR/migration.pid"
LOG_FILE="$PROJECT_DIR/logs/migration.log"
REALTIME_LOG="$PROJECT_DIR/logs/realtime.log"
BATCH_LOG="$PROJECT_DIR/logs/batch.log"
PARALLEL_BATCH_LOG="$PROJECT_DIR/logs/parallel_batch.log"
PYTHON_CMD="python3"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

print_error() {
    echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ERROR:${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] WARNING:${NC} $1"
}

print_info() {
    echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')] INFO:${NC} $1"
}

print_header() {
    echo -e "${CYAN}========================================${NC}"
    echo -e "${CYAN}  Migration Status Check${NC}"
    echo -e "${CYAN}========================================${NC}"
}

print_section() {
    echo -e "${MAGENTA}--- $1 ---${NC}"
}

# Function to check if migration is running
check_migration_status() {
    print_header
    
    # Check for various PID files
    PID_FILES=("$PROJECT_DIR/migration.pid" "$PROJECT_DIR/logs/realtime.pid" "$PROJECT_DIR/logs/batch.pid" "$PROJECT_DIR/logs/parallel_batch.pid")
    RUNNING_PROCESSES=()
    
    for pid_file in "${PID_FILES[@]}"; do
        if [ -f "$pid_file" ]; then
            PID=$(cat "$pid_file")
            if ps -p "$PID" > /dev/null 2>&1; then
                RUNNING_PROCESSES+=("$PID")
                PROCESS_NAME=$(basename "$pid_file" .pid)
                print_status "$PROCESS_NAME process is running with PID: $PID"
            else
                print_warning "Stale PID file found: $pid_file (PID: $PID)"
                rm -f "$pid_file"
            fi
        fi
    done
    
    # Check for running Python migration processes
    PYTHON_PROCESSES=$(ps aux | grep -E "(python.*main|python.*realtime|python.*batch)" | grep -v grep | awk '{print $2}')
    
    if [ -n "$PYTHON_PROCESSES" ]; then
        print_status "Found running Python migration processes: $PYTHON_PROCESSES"
        for pid in $PYTHON_PROCESSES; do
            RUNNING_PROCESSES+=("$pid")
        done
    fi
    
    if [ ${#RUNNING_PROCESSES[@]} -eq 0 ]; then
        print_warning "No migration processes found running."
        return 1
    fi
    
    # Show process info for all running processes
    for pid in "${RUNNING_PROCESSES[@]}"; do
        PROCESS_INFO=$(ps -p "$pid" -o pid,ppid,cmd,etime,pcpu,pmem --no-headers 2>/dev/null)
        if [ $? -eq 0 ]; then
            print_info "Process info: $PROCESS_INFO"
        fi
    done
    
    return 0
}

# Function to get detailed work progress from logs
get_work_progress() {
    print_section "Work Progress Analysis"
    
    cd "$PROJECT_DIR"
    
    # Get work progress from Python
    PROGRESS_OUTPUT=$($PYTHON_CMD -c "
import sys
sys.path.append('.')
try:
    from database import db_manager
    import os
    
    # Get wide table statistics
    print('=== WIDE TABLE STATISTICS ===')
    stats = db_manager.get_wide_table_stats()
    
    for ship_id, stat in stats.items():
        print(f'Ship: {ship_id}')
        if stat['exists']:
            print(f'  Table exists: Yes')
            print(f'  Record count: {stat[\"record_count\"]:,}')
            if stat['earliest_time']:
                print(f'  Time range: {stat[\"earliest_time\"]} ~ {stat[\"latest_time\"]}')
            else:
                print(f'  Time range: No data')
        else:
            print(f'  Table exists: No')
            if 'error' in stat:
                print(f'  Error: {stat[\"error\"]}')
        print()
    
    # Parse work logs
    print('=== WORK PROGRESS FROM LOGS ===')
    log_files = ['logs/migration.log', 'logs/realtime.log', 'logs/batch.log']
    
    for log_file in log_files:
        if os.path.exists(log_file):
            print(f'\\nProcessing {log_file}:')
            work_stats = db_manager.parse_status_logs(log_file)
            
            for ship_id, processes in work_stats.items():
                print(f'  Ship: {ship_id}')
                
                for process_type, data in processes.items():
                    if data['operations'] > 0:
                        print(f'    {process_type.upper()}:')
                        print(f'      Operations: {data[\"operations\"]}')
                        print(f'      Total records: {data[\"total_records\"]:,}')
                        print(f'      Columns: {data[\"total_columns\"]}')
                        if data['last_time_range']:
                            print(f'      Last time range: {data[\"last_time_range\"]}')
        else:
            print(f'\\nLog file not found: {log_file}')
    
except Exception as e:
    print(f'Error getting work progress: {e}')
" 2>&1)
    
    if [ $? -eq 0 ]; then
        echo "$PROGRESS_OUTPUT"
    else
        print_error "Failed to get work progress: $PROGRESS_OUTPUT"
    fi
}

# Function to show recent activity
show_recent_activity() {
    print_section "Recent Activity"
    
    # Show recent realtime activity
    if [ -f "$REALTIME_LOG" ]; then
        print_info "Recent realtime activity (last 10 lines):"
        echo -e "${CYAN}----------------------------------------${NC}"
        tail -n 10 "$REALTIME_LOG" | grep -E "(REALTIME INSERT|STATUS:REALTIME)" || echo "No recent realtime activity"
        echo -e "${CYAN}----------------------------------------${NC}"
    fi
    
    # Show recent batch activity
    if [ -f "$BATCH_LOG" ]; then
        print_info "Recent batch activity (last 10 lines):"
        echo -e "${CYAN}----------------------------------------${NC}"
        tail -n 10 "$BATCH_LOG" | grep -E "(BATCH INSERT|STATUS:BATCH)" || echo "No recent batch activity"
        echo -e "${CYAN}----------------------------------------${NC}"
    fi
    
    # Show recent parallel batch activity
    if [ -f "$PARALLEL_BATCH_LOG" ]; then
        print_info "Recent parallel batch activity (last 10 lines):"
        echo -e "${CYAN}----------------------------------------${NC}"
        tail -n 10 "$PARALLEL_BATCH_LOG" | grep -E "(BATCH INSERT|STATUS:BATCH)" || echo "No recent parallel batch activity"
        echo -e "${CYAN}----------------------------------------${NC}"
    fi
    
    # Show recent general activity
    if [ -f "$LOG_FILE" ]; then
        print_info "Recent general activity (last 10 lines):"
        echo -e "${CYAN}----------------------------------------${NC}"
        tail -n 10 "$LOG_FILE"
        echo -e "${CYAN}----------------------------------------${NC}"
    fi
}

# Function to show log statistics
show_log_stats() {
    print_section "Log Statistics"
    
    log_files=("$LOG_FILE" "$REALTIME_LOG" "$BATCH_LOG" "$PARALLEL_BATCH_LOG")
    
    for log_file in "${log_files[@]}"; do
        if [ -f "$log_file" ]; then
            LOG_NAME=$(basename "$log_file")
            LOG_SIZE=$(du -h "$log_file" | cut -f1)
            LOG_LINES=$(wc -l < "$log_file")
            LOG_LAST_MODIFIED=$(stat -c %y "$log_file")
            
            echo "  $LOG_NAME:"
            echo "    Size: $LOG_SIZE"
            echo "    Lines: $LOG_LINES"
            echo "    Last modified: $LOG_LAST_MODIFIED"
            
            # Count specific log types
            if [ $LOG_LINES -gt 0 ]; then
                echo "    Log levels:"
                echo "      INFO: $(grep -c "INFO" "$log_file" || echo "0")"
                echo "      WARNING: $(grep -c "WARNING" "$log_file" || echo "0")"
                echo "      ERROR: $(grep -c "ERROR" "$log_file" || echo "0")"
                
                # Count work operations
                REALTIME_OPS=$(grep -c "STATUS:REALTIME" "$log_file" || echo "0")
                BATCH_OPS=$(grep -c "STATUS:BATCH" "$log_file" || echo "0")
                echo "      Realtime operations: $REALTIME_OPS"
                echo "      Batch operations: $BATCH_OPS"
            fi
            echo
        else
            print_warning "Log file not found: $log_file"
        fi
    done
}

# Function to show system resources
show_system_resources() {
    print_section "System Resources"
    
    # CPU usage
    CPU_USAGE=$(top -bn1 | grep "Cpu(s)" | awk '{print $2}' | cut -d'%' -f1)
    echo "  CPU usage: ${CPU_USAGE}%"
    
    # Memory usage
    MEMORY_INFO=$(free -h | grep "Mem:")
    echo "  Memory: $MEMORY_INFO"
    
    # Disk usage
    DISK_USAGE=$(df -h "$PROJECT_DIR" | tail -1 | awk '{print $5}')
    echo "  Disk usage: $DISK_USAGE"
    
    # Load average
    LOAD_AVG=$(uptime | awk -F'load average:' '{print $2}')
    echo "  Load average:$LOAD_AVG"
}

# Function to show cutoff time status
show_cutoff_time_status() {
    print_section "Cutoff Time Status"
    
    cd "$PROJECT_DIR"
    
    CUTOFF_OUTPUT=$($PYTHON_CMD -c "
import sys
sys.path.append('.')
try:
    from cutoff_time_manager import cutoff_time_manager
    status = cutoff_time_manager.get_cutoff_time_status()
    print(f'  Has cutoff time: {status[\"has_cutoff_time\"]}')
    if status['cutoff_time']:
        print(f'  Cutoff time: {status[\"cutoff_time\"]}')
    print(f'  File exists: {status[\"file_exists\"]}')
    print(f'  File path: {status[\"file_path\"]}')
except Exception as e:
    print(f'  Error: {e}')
" 2>&1)
    
    echo "$CUTOFF_OUTPUT"
}

# Function to show process status
show_process_status() {
    print_section "Process Status"
    
    # Check for realtime process
    REALTIME_PID_FILE="$PROJECT_DIR/realtime.pid"
    if [ -f "$REALTIME_PID_FILE" ]; then
        REALTIME_PID=$(cat "$REALTIME_PID_FILE")
        if ps -p "$REALTIME_PID" > /dev/null 2>&1; then
            print_status "Realtime process running (PID: $REALTIME_PID)"
        else
            print_warning "Realtime process not running (stale PID file)"
        fi
    else
        print_info "No realtime process PID file found"
    fi
    
    # Check for batch process
    BATCH_PID_FILE="$PROJECT_DIR/logs/batch.pid"
    if [ -f "$BATCH_PID_FILE" ]; then
        BATCH_PID=$(cat "$BATCH_PID_FILE")
        if ps -p "$BATCH_PID" > /dev/null 2>&1; then
            print_status "Batch process running (PID: $BATCH_PID)"
        else
            print_warning "Batch process not running (stale PID file)"
        fi
    else
        print_info "No batch process PID file found"
    fi
    
    # Check for parallel batch process
    PARALLEL_BATCH_PID_FILE="$PROJECT_DIR/logs/parallel_batch.pid"
    if [ -f "$PARALLEL_BATCH_PID_FILE" ]; then
        PARALLEL_BATCH_PID=$(cat "$PARALLEL_BATCH_PID_FILE")
        if ps -p "$PARALLEL_BATCH_PID" > /dev/null 2>&1; then
            print_status "Parallel batch process running (PID: $PARALLEL_BATCH_PID)"
        else
            print_warning "Parallel batch process not running (stale PID file)"
        fi
    else
        print_info "No parallel batch process PID file found"
    fi
}

# Main execution
main() {
    # Check if migration is running
    if ! check_migration_status; then
        print_info "Migration is not running."
        print_info "Use './start_realtime.sh' or './start_parallel_batch.sh' to start processes."
        print_info "You can run both realtime and parallel batch processes simultaneously."
        exit 0
    fi
    
    echo ""
    
    # Show process status
    show_process_status
    
    echo ""
    
    # Get detailed work progress
    get_work_progress
    
    echo ""
    
    # Show recent activity
    show_recent_activity
    
    echo ""
    
    # Show log statistics
    show_log_stats
    
    echo ""
    
    # Show system resources
    show_system_resources
    
    echo ""
    
    # Show cutoff time status
    show_cutoff_time_status
    
    echo ""
    print_status "Enhanced status check completed!"
    print_info "Use './view_logs.sh' for full log viewing"
    print_info "Use './stop_realtime.sh' or './stop_parallel_batch.sh' to stop processes"
}

# Run main function
main "$@"