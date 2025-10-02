#!/bin/bash

# PostgreSQL Narrow-to-Wide Migration Restarter
# Usage: ./restart_migration.sh [mode] [interval]

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR"

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
    echo "  $0                                    # Restart with concurrent mode, 1min interval"
    echo "  $0 concurrent 5                      # Restart with concurrent mode, 5min interval"
    echo "  $0 hybrid 2                         # Restart with hybrid mode, 2min interval"
}

# Main execution
main() {
    # Check if help is requested
    if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
        show_usage
        exit 0
    fi
    
    print_info "Restarting PostgreSQL Narrow-to-Wide Migration..."
    
    # Stop existing migration
    print_info "Stopping existing migration..."
    if [ -f "$PROJECT_DIR/stop_migration.sh" ]; then
        "$PROJECT_DIR/stop_migration.sh"
    else
        print_error "stop_migration.sh not found"
        exit 1
    fi
    
    # Wait a moment
    print_info "Waiting for processes to stop..."
    sleep 3
    
    # Start new migration
    print_info "Starting new migration..."
    if [ -f "$PROJECT_DIR/run_migration.sh" ]; then
        "$PROJECT_DIR/run_migration.sh" "$@"
    else
        print_error "run_migration.sh not found"
        exit 1
    fi
    
    print_status "Migration restarted successfully!"
    print_info "Use './check_status.sh' to monitor progress"
}

# Run main function
main "$@"
