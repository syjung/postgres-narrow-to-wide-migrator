#!/usr/bin/env python3
"""
column_list_long_with_groups.txt를 group별로 분리하는 스크립트
"""
from collections import defaultdict

def split_by_group(input_file):
    """Group별로 파일 분리"""
    
    # Group별로 데이터 수집
    groups = defaultdict(list)
    
    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            parts = line.split(',', 1)
            if len(parts) == 2:
                group = parts[0]
                channel_id = parts[1]
                groups[group].append(channel_id)
    
    # Group별로 파일 생성
    total_files = 0
    total_channels = 0
    
    for group, channels in sorted(groups.items()):
        output_file = f"column_list_group_{group}.txt"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            for channel in channels:
                f.write(f"{channel}\n")
        
        total_files += 1
        total_channels += len(channels)
        
        print(f"✅ Created {output_file}: {len(channels)} channels")
    
    return total_files, total_channels, groups

def main():
    print("=" * 80)
    print("Column List Splitter by Group")
    print("=" * 80)
    
    input_file = "column_list_2562.txt"
    
    print(f"\n📂 Processing {input_file}...")
    
    total_files, total_channels, groups = split_by_group(input_file)
    
    print("\n" + "=" * 80)
    print("Summary")
    print("=" * 80)
    print(f"📊 Total groups: {total_files}")
    print(f"📊 Total channels: {total_channels}")
    
    print("\n📋 Breakdown by group:")
    for group, channels in sorted(groups.items()):
        print(f"  Group {group}: {len(channels):,} channels")
    
    print("\n✅ Files created:")
    for group in sorted(groups.keys()):
        print(f"  - column_list_group_{group}.txt")

if __name__ == "__main__":
    main()

