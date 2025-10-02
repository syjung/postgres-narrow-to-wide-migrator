#!/bin/bash

# PostgreSQL Narrow-to-Wide Migration Log Viewer
# Usage: ./view_logs.sh [options]

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR"
LOG_FILE="$PROJECT_DIR/logs/migration.log"

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

# Function to show usage
show_usage() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  -f, --follow     Follow log file (like tail -f)"
    echo "  -n, --lines N    Show last N lines (default: 50)"
    echo "  -e, --errors     Show only error messages"
    echo "  -w, --warnings   Show only warning messages"
    echo "  -i, --info       Show only info messages"
    echo "  -g, --grep TEXT  Filter logs containing TEXT"
    echo "  -s, --stats      Show log statistics"
    echo "  -h, --help       Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                    # Show last 50 lines"
    echo "  $0 -n 100             # Show last 100 lines"
    echo "  $0 -f                 # Follow log file"
    echo "  $0 -e                 # Show only errors"
    echo "  $0 -g 'chunk'         # Show logs containing 'chunk'"
    echo "  $0 -s                 # Show log statistics"
}

# Function to check if log file exists
check_log_file() {
    if [ ! -f "$LOG_FILE" ]; then
        print_error "Log file not found: $LOG_FILE"
        print_info "Make sure migration is running or has been run before."
        exit 1
    fi
}

# Function to show log statistics
show_log_stats() {
    print_info "Log file statistics:"
    
    if [ -f "$LOG_FILE" ]; then
        LOG_SIZE=$(du -h "$LOG_FILE" | cut -f1)
        LOG_LINES=$(wc -l < "$LOG_FILE")
        LOG_LAST_MODIFIED=$(stat -c %y "$LOG_FILE")
        
        echo "  File: $LOG_FILE"
        echo "  Size: $LOG_SIZE"
        echo "  Lines: $LOG_LINES"
        echo "  Last modified: $LOG_LAST_MODIFIED"
        
        # Count log levels
        if [ $LOG_LINES -gt 0 ]; then
            echo "  Log levels:"
            echo "    INFO: $(grep -c "INFO" "$LOG_FILE" || echo "0")"
            echo "    WARNING: $(grep -c "WARNING" "$LOG_FILE" || echo "0")"
            echo "    ERROR: $(grep -c "ERROR" "$LOG_FILE" || echo "0")"
            
            # Count specific patterns
            echo "  Migration patterns:"
            echo "    Chunk migrations: $(grep -c "chunk migration" "$LOG_FILE" || echo "0")"
            echo "    Batch processing: $(grep -c "batch" "$LOG_FILE" || echo "0")"
            echo "    Ship processing: $(grep -c "ship" "$LOG_FILE" || echo "0")"
        fi
    else
        print_warning "Log file not found: $LOG_FILE"
    fi
}

# Function to colorize log output
colorize_logs() {
    while IFS= read -r line; do
        if echo "$line" | grep -q "ERROR"; then
            echo -e "${RED}$line${NC}"
        elif echo "$line" | grep -q "WARNING"; then
            echo -e "${YELLOW}$line${NC}"
        elif echo "$line" | grep -q "INFO"; then
            echo -e "${GREEN}$line${NC}"
        else
            echo "$line"
        fi
    done
}

# Function to filter logs by level
filter_logs() {
    local filter_type="$1"
    local lines="${2:-50}"
    
    case "$filter_type" in
        "error")
            tail -n "$lines" "$LOG_FILE" | grep "ERROR" | colorize_logs
            ;;
        "warning")
            tail -n "$lines" "$LOG_FILE" | grep "WARNING" | colorize_logs
            ;;
        "info")
            tail -n "$lines" "$LOG_FILE" | grep "INFO" | colorize_logs
            ;;
        *)
            tail -n "$lines" "$LOG_FILE" | colorize_logs
            ;;
    esac
}

# Function to grep logs
grep_logs() {
    local search_text="$1"
    local lines="${2:-50}"
    
    tail -n "$lines" "$LOG_FILE" | grep -i "$search_text" | colorize_logs
}

# Function to follow logs
follow_logs() {
    print_info "Following log file: $LOG_FILE"
    print_info "Press Ctrl+C to stop"
    echo ""
    
    tail -f "$LOG_FILE" | colorize_logs
}

# Main execution
main() {
    local lines=50
    local follow=false
    local filter=""
    local grep_text=""
    local stats=false
    
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -f|--follow)
                follow=true
                shift
                ;;
            -n|--lines)
                lines="$2"
                shift 2
                ;;
            -e|--errors)
                filter="error"
                shift
                ;;
            -w|--warnings)
                filter="warning"
                shift
                ;;
            -i|--info)
                filter="info"
                shift
                ;;
            -g|--grep)
                grep_text="$2"
                shift 2
                ;;
            -s|--stats)
                stats=true
                shift
                ;;
            -h|--help)
                show_usage
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                show_usage
                exit 1
                ;;
        esac
    done
    
    # Check if log file exists
    check_log_file
    
    # Show statistics if requested
    if [ "$stats" = true ]; then
        show_log_stats
        echo ""
    fi
    
    # Show logs based on options
    if [ "$follow" = true ]; then
        follow_logs
    elif [ -n "$grep_text" ]; then
        grep_logs "$grep_text" "$lines"
    elif [ -n "$filter" ]; then
        filter_logs "$filter" "$lines"
    else
        tail -n "$lines" "$LOG_FILE" | colorize_logs
    fi
}

# Run main function
main "$@"
