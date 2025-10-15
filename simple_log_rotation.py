#!/usr/bin/env python3
"""
Simple log rotation utility for clean date-based naming
"""
import os
import shutil
import gzip
from datetime import datetime, timedelta
from pathlib import Path

def setup_simple_log_rotation(log_file_path: str, retention_days: int = 30):
    """
    Setup simple log rotation with clean date-based naming
    
    Args:
        log_file_path: Path to the main log file (e.g., 'logs/realtime.log')
        retention_days: Number of days to retain old logs
    
    Returns:
        str: Path to the current log file
    """
    log_path = Path(log_file_path)
    log_dir = log_path.parent
    log_name = log_path.stem  # e.g., 'realtime'
    log_ext = log_path.suffix  # e.g., '.log'
    
    # Create log directory if it doesn't exist
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Check if we need to rotate (if log file exists and is from yesterday or earlier)
    if log_path.exists():
        file_time = datetime.fromtimestamp(log_path.stat().st_mtime)
        today = datetime.now().date()
        
        if file_time.date() < today:
            # Rotate the log file
            yesterday = file_time.strftime('%Y-%m-%d')
            rotated_name = f"{log_name}_{yesterday}{log_ext}.gz"
            rotated_path = log_dir / rotated_name
            
            # Compress and move current log to rotated name
            with open(log_path, 'rb') as f_in:
                with gzip.open(rotated_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            # Remove original file
            log_path.unlink()
            
            print(f"✅ Log rotated: {log_path.name} -> {rotated_name}")
    
    # Clean up old log files
    cleanup_old_logs(log_dir, log_name, retention_days)
    
    return str(log_path)

def cleanup_old_logs(log_dir: Path, log_name: str, retention_days: int):
    """Clean up old log files beyond retention period"""
    cutoff_date = datetime.now().date() - timedelta(days=retention_days)
    
    for log_file in log_dir.glob(f"{log_name}_*.log.gz"):
        try:
            # Extract date from filename (e.g., realtime_2025-10-04.log.gz)
            date_str = log_file.stem.replace(f"{log_name}_", "").replace(".log", "")
            file_date = datetime.strptime(date_str, '%Y-%m-%d').date()
            
            if file_date < cutoff_date:
                log_file.unlink()
                print(f"🗑️  Cleaned up old log: {log_file.name}")
                
        except (ValueError, OSError) as e:
            print(f"⚠️  Could not process log file {log_file.name}: {e}")

if __name__ == "__main__":
    # Test the log rotation
    print("Testing simple log rotation...")
    
    # Create a test log file
    test_log = "logs/test_simple.log"
    Path("logs").mkdir(exist_ok=True)
    
    with open(test_log, 'w') as f:
        f.write("Test log content\n")
    
    # Simulate old file by setting modification time to yesterday
    yesterday = datetime.now() - timedelta(days=1)
    os.utime(test_log, (yesterday.timestamp(), yesterday.timestamp()))
    
    setup_simple_log_rotation(test_log, retention_days=7)
    
    # Clean up test files
    import glob
    test_files = glob.glob("logs/test_simple*")
    for file in test_files:
        os.remove(file)
    print("Test files cleaned up")
