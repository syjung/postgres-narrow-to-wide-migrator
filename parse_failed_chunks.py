#!/usr/bin/env python3
"""
Parse failed chunks from log file and generate CSV for reprocessing.

This script reads a log file containing migration errors and extracts
the IMO number, start time, and end time for each failed chunk.
"""

import re
import csv
from typing import List, Tuple, Set
from pathlib import Path


def parse_log_file(log_file: str) -> List[Tuple[str, str, str]]:
    """
    Parse log file and extract failed chunk information.
    
    The log pattern is:
    1. One or more "Query failed" lines with IMO numbers
    2. Followed by "Failed to migrate chunk" lines
    3. The order matches: first IMO -> first chunk, second IMO -> second chunk, etc.
    
    Args:
        log_file: Path to the log file
        
    Returns:
        List of tuples (imo, start_time, end_time)
    """
    failed_chunks = []
    seen_chunks = set()  # To avoid duplicates
    
    # Pattern to match "Query failed" with IMO
    # Example: 2025-10-11 09:36:34 | ERROR | thread_logger:error:42 - [IMO9986087:Thread-133354095633984] Query failed after 8126.46 seconds: connection already closed
    query_failed_pattern = re.compile(r'\[IMO(\d+):.*\] Query failed')
    
    # Pattern to match "Failed to migrate chunk"
    # Example: 2025-10-11 09:36:34 | ERROR | chunked_migration_strategy:migrate_chunk:255 - ❌ Failed to migrate chunk 2025-07-15 20:40:40.391527 to 2025-07-16 02:40:40.391527: connection already closed
    chunk_pattern = re.compile(
        r'Failed to migrate chunk (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+) to (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+)'
    )
    
    with open(log_file, 'r') as f:
        lines = f.readlines()
    
    # Queue to store IMO numbers from "Query failed" lines
    imo_queue = []
    
    for line in lines:
        # Check for "Query failed" with IMO
        query_match = query_failed_pattern.search(line)
        if query_match:
            imo = f"IMO{query_match.group(1)}"
            imo_queue.append(imo)
        
        # Check for "Failed to migrate chunk"
        chunk_match = chunk_pattern.search(line)
        if chunk_match and imo_queue:
            # Match with the first IMO in queue (FIFO)
            imo = imo_queue.pop(0)
            start_time = chunk_match.group(1)
            end_time = chunk_match.group(2)
            
            # Create unique key to avoid duplicates
            chunk_key = (imo, start_time, end_time)
            
            if chunk_key not in seen_chunks:
                seen_chunks.add(chunk_key)
                failed_chunks.append(chunk_key)
    
    return failed_chunks


def write_csv(failed_chunks: List[Tuple[str, str, str]], output_file: str):
    """
    Write failed chunks to CSV file.
    
    Args:
        failed_chunks: List of tuples (imo, start_time, end_time)
        output_file: Path to output CSV file
    """
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        for chunk in failed_chunks:
            writer.writerow(chunk)
    
    print(f"✅ Wrote {len(failed_chunks)} failed chunks to {output_file}")


def main():
    """Main function."""
    log_file = "a.log"
    output_file = "post_proc.csv"
    
    print(f"📖 Reading log file: {log_file}")
    failed_chunks = parse_log_file(log_file)
    
    print(f"📊 Found {len(failed_chunks)} unique failed chunks")
    
    if failed_chunks:
        write_csv(failed_chunks, output_file)
        
        # Show first few entries
        print("\n📋 First 5 entries:")
        for i, chunk in enumerate(failed_chunks[:5], 1):
            print(f"  {i}. {chunk[0]}: {chunk[1]} -> {chunk[2]}")
        
        if len(failed_chunks) > 5:
            print(f"  ... and {len(failed_chunks) - 5} more")
    else:
        print("⚠️  No failed chunks found in log file")


if __name__ == "__main__":
    main()

