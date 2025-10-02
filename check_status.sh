#!/bin/bash

# PostgreSQL Narrow-to-Wide Migration Status Checker
# Usage: ./check_status.sh

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR"
PID_FILE="$PROJECT_DIR/migration.pid"
LOG_FILE="$PROJECT_DIR/logs/migration.log"
PYTHON_CMD="python3"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
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

# Function to check if migration is running
check_migration_status() {
    print_header
    
    if [ ! -f "$PID_FILE" ]; then
        print_warning "No PID file found. Migration is not running."
        return 1
    fi
    
    PID=$(cat "$PID_FILE")
    
    if ! ps -p "$PID" > /dev/null 2>&1; then
        print_error "Migration process (PID: $PID) is not running."
        print_info "Removing stale PID file..."
        rm -f "$PID_FILE"
        return 1
    fi
    
    print_status "Migration is running with PID: $PID"
    
    # Get process info
    PROCESS_INFO=$(ps -p "$PID" -o pid,ppid,cmd,etime,pcpu,pmem --no-headers)
    print_info "Process info: $PROCESS_INFO"
    
    return 0
}

# Function to get migration status from Python
get_python_status() {
    print_info "Getting detailed migration status..."
    
    cd "$PROJECT_DIR"
    
    # Get status from Python
    STATUS_OUTPUT=$($PYTHON_CMD -c "
import sys
sys.path.append('.')
try:
    from main import MigrationManager
    migration_manager = MigrationManager()
    status_report = migration_manager.get_status_report()
    print('=== MIGRATION STATUS ===')
    print(status_report)
    print('=== END STATUS ===')
except Exception as e:
    print(f'Error getting status: {e}')
" 2>&1)
    
    if [ $? -eq 0 ]; then
        echo "$STATUS_OUTPUT"
    else
        print_error "Failed to get Python status: $STATUS_OUTPUT"
    fi
}

# Function to show recent logs
show_recent_logs() {
    print_info "Recent log entries (last 20 lines):"
    echo -e "${CYAN}----------------------------------------${NC}"
    
    if [ -f "$LOG_FILE" ]; then
        tail -n 20 "$LOG_FILE"
    else
        print_warning "Log file not found: $LOG_FILE"
    fi
    
    echo -e "${CYAN}----------------------------------------${NC}"
}

# Function to show log statistics
show_log_stats() {
    print_info "Log file statistics:"
    
    if [ -f "$LOG_FILE" ]; then
        LOG_SIZE=$(du -h "$LOG_FILE" | cut -f1)
        LOG_LINES=$(wc -l < "$LOG_FILE")
        LOG_LAST_MODIFIED=$(stat -c %y "$LOG_FILE")
        
        echo "  Size: $LOG_SIZE"
        echo "  Lines: $LOG_LINES"
        echo "  Last modified: $LOG_LAST_MODIFIED"
        
        # Count log levels
        if [ $LOG_LINES -gt 0 ]; then
            echo "  Log levels:"
            echo "    INFO: $(grep -c "INFO" "$LOG_FILE" || echo "0")"
            echo "    WARNING: $(grep -c "WARNING" "$LOG_FILE" || echo "0")"
            echo "    ERROR: $(grep -c "ERROR" "$LOG_FILE" || echo "0")"
        fi
    else
        print_warning "Log file not found: $LOG_FILE"
    fi
}

# Function to show system resources
show_system_resources() {
    print_info "System resources:"
    
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
    print_info "Cutoff time status:"
    
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

# Main execution
main() {
    # Check if migration is running
    if ! check_migration_status; then
        print_info "Migration is not running."
        print_info "Use './run_migration.sh' to start migration."
        exit 0
    fi
    
    echo ""
    
    # Get detailed status
    get_python_status
    
    echo ""
    
    # Show recent logs
    show_recent_logs
    
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
    print_status "Status check completed!"
    print_info "Use './view_logs.sh' for full log viewing"
    print_info "Use './stop_migration.sh' to stop migration"
}

# Run main function
main "$@"
