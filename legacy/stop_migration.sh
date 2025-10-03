#!/bin/bash

# PostgreSQL Narrow-to-Wide Migration Stopper
# Usage: ./stop_migration.sh

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR"
PID_FILE="$PROJECT_DIR/migration.pid"
LOG_FILE="$PROJECT_DIR/logs/migration.log"

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

# Function to show usage
show_usage() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  -f, --force     Force kill the process"
    echo "  -h, --help      Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0              # Graceful stop"
    echo "  $0 -f           # Force stop"
}

# Function to stop migration gracefully
stop_migration_gracefully() {
    local pid="$1"
    
    print_info "Sending SIGTERM to process $pid..."
    
    # Send SIGTERM
    kill -TERM "$pid" 2>/dev/null
    
    # Wait for graceful shutdown
    local count=0
    while [ $count -lt 30 ]; do
        if ! ps -p "$pid" > /dev/null 2>&1; then
            print_status "Migration stopped gracefully"
            return 0
        fi
        
        print_info "Waiting for graceful shutdown... ($((count + 1))/30)"
        sleep 1
        count=$((count + 1))
    done
    
    print_warning "Graceful shutdown timeout, process still running"
    return 1
}

# Function to force stop migration
force_stop_migration() {
    local pid="$1"
    
    print_warning "Force stopping migration process $pid..."
    
    # Send SIGKILL
    kill -KILL "$pid" 2>/dev/null
    
    # Wait a moment
    sleep 2
    
    if ! ps -p "$pid" > /dev/null 2>&1; then
        print_status "Migration force stopped"
        return 0
    else
        print_error "Failed to force stop migration"
        return 1
    fi
}

# Function to cleanup
cleanup() {
    print_info "Cleaning up..."
    
    # Remove PID file
    if [ -f "$PID_FILE" ]; then
        rm -f "$PID_FILE"
        print_info "Removed PID file: $PID_FILE"
    fi
    
    # Show final log entries
    if [ -f "$LOG_FILE" ]; then
        print_info "Final log entries:"
        echo "----------------------------------------"
        tail -n 5 "$LOG_FILE"
        echo "----------------------------------------"
    fi
}

# Main execution
main() {
    local force=false
    
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -f|--force)
                force=true
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
    
    print_info "Stopping PostgreSQL Narrow-to-Wide Migration..."
    
    # Check if PID file exists
    if [ ! -f "$PID_FILE" ]; then
        print_warning "No PID file found. Migration is not running."
        exit 0
    fi
    
    # Get PID
    PID=$(cat "$PID_FILE")
    
    # Check if process is running
    if ! ps -p "$PID" > /dev/null 2>&1; then
        print_warning "Migration process (PID: $PID) is not running."
        print_info "Removing stale PID file..."
        rm -f "$PID_FILE"
        exit 0
    fi
    
    print_info "Found running migration process with PID: $PID"
    
    # Stop migration
    if [ "$force" = true ]; then
        if force_stop_migration "$PID"; then
            cleanup
            print_status "Migration force stopped successfully!"
        else
            print_error "Failed to force stop migration"
            exit 1
        fi
    else
        if stop_migration_gracefully "$PID"; then
            cleanup
            print_status "Migration stopped successfully!"
        else
            print_warning "Graceful stop failed, trying force stop..."
            if force_stop_migration "$PID"; then
                cleanup
                print_status "Migration force stopped successfully!"
            else
                print_error "Failed to stop migration"
                exit 1
            fi
        fi
    fi
    
    print_info "Use './check_status.sh' to verify migration has stopped"
    print_info "Use './run_migration.sh' to start migration again"
}

# Run main function
main "$@"
