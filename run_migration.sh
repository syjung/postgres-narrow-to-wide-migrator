#!/bin/bash

# PostgreSQL Narrow-to-Wide Migration Background Runner
# Usage: ./run_migration.sh [mode] [interval]

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR"
LOG_DIR="$PROJECT_DIR/logs"
PID_FILE="$PROJECT_DIR/migration.pid"
LOG_FILE="$LOG_DIR/migration.log"
PYTHON_CMD="python3"

# Create logs directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Default values
MODE=${1:-"concurrent"}
INTERVAL=${2:-1}

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
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

# Function to check if migration is already running
check_running() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p "$PID" > /dev/null 2>&1; then
            print_warning "Migration is already running with PID: $PID"
            return 0
        else
            print_info "Stale PID file found, removing..."
            rm -f "$PID_FILE"
        fi
    fi
    return 1
}

# Function to start migration
start_migration() {
    print_status "Starting PostgreSQL Narrow-to-Wide Migration..."
    print_info "Mode: $MODE"
    print_info "Interval: $INTERVAL minutes"
    print_info "Log file: $LOG_FILE"
    print_info "PID file: $PID_FILE"
    
    # Change to project directory
    cd "$PROJECT_DIR"
    
    # Start migration in background
    nohup $PYTHON_CMD main.py --mode "$MODE" --interval "$INTERVAL" > "$LOG_FILE" 2>&1 &
    PID=$!
    
    # Save PID
    echo "$PID" > "$PID_FILE"
    
    print_status "Migration started with PID: $PID"
    print_info "Use './check_status.sh' to monitor progress"
    print_info "Use './stop_migration.sh' to stop migration"
    print_info "Use './view_logs.sh' to view logs"
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [mode] [interval]"
    echo ""
    echo "Modes:"
    echo "  concurrent    - Concurrent strategy (real-time + backfill) [DEFAULT]"
    echo "  hybrid        - Hybrid strategy (enhanced concurrent)"
    echo "  streaming     - Streaming strategy (all data streaming)"
    echo "  full          - Full migration (schema + tables + migration + realtime)"
    echo "  schema-only   - Schema analysis only"
    echo "  tables-only   - Table creation only"
    echo "  migration-only - Data migration only"
    echo "  realtime      - Real-time processing only"
    echo "  dual-write    - Dual-write mode"
    echo ""
    echo "Interval: Processing interval in minutes (default: 1)"
    echo ""
    echo "Examples:"
    echo "  $0                                    # Start concurrent migration with 1min interval"
    echo "  $0 concurrent 5                      # Start concurrent migration with 5min interval"
    echo "  $0 hybrid 2                         # Start hybrid migration with 2min interval"
    echo "  $0 full                             # Start full migration"
    echo ""
    echo "Management commands:"
    echo "  ./check_status.sh                    # Check migration status"
    echo "  ./stop_migration.sh                  # Stop migration"
    echo "  ./view_logs.sh                       # View logs"
    echo "  ./restart_migration.sh [mode] [interval] # Restart migration"
}

# Main execution
main() {
    # Check if help is requested
    if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
        show_usage
        exit 0
    fi
    
    # Check if migration is already running
    if check_running; then
        print_error "Migration is already running. Use './restart_migration.sh' to restart."
        exit 1
    fi
    
    # Validate mode
    case "$MODE" in
        concurrent|hybrid|streaming|full|schema-only|tables-only|migration-only|realtime|dual-write)
            ;;
        *)
            print_error "Invalid mode: $MODE"
            show_usage
            exit 1
            ;;
    esac
    
    # Validate interval
    if ! [[ "$INTERVAL" =~ ^[0-9]+$ ]] || [ "$INTERVAL" -lt 1 ]; then
        print_error "Invalid interval: $INTERVAL. Must be a positive integer."
        exit 1
    fi
    
    # Start migration
    start_migration
    
    print_status "Migration started successfully!"
    print_info "Check status: ./check_status.sh"
    print_info "View logs: ./view_logs.sh"
    print_info "Stop migration: ./stop_migration.sh"
}

# Run main function
main "$@"
