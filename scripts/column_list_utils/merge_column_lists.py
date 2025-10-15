#!/usr/bin/env python3
"""
column_list_2562.txt를 기준으로 column_list_long.txt에 group을 추가하는 스크립트
"""

def load_group_mapping(file_path):
    """column_list_2562.txt에서 group-channel_id 매핑 로드"""
    mapping = {}
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            parts = line.split(',', 1)
            if len(parts) == 2:
                group = parts[0]
                channel_id = parts[1]
                # 앞에 /를 추가하여 column_list_long.txt 형식과 맞춤
                mapping[f"/{channel_id}"] = group
    
    return mapping

def process_long_list(long_file, mapping, output_file):
    """column_list_long.txt 처리하여 group 추가"""
    missing_channels = []
    matched_count = 0
    
    with open(long_file, 'r', encoding='utf-8') as f_in, \
         open(output_file, 'w', encoding='utf-8') as f_out:
        
        for line in f_in:
            channel_id = line.strip()
            if not channel_id:
                continue
            
            # group 찾기
            if channel_id in mapping:
                group = mapping[channel_id]
                # group,channel_id 형식으로 출력 (앞의 / 제거)
                f_out.write(f"{group},{channel_id[1:]}\n")
                matched_count += 1
            else:
                # 매칭되지 않는 항목
                missing_channels.append(channel_id)
                # group 없이 출력 (또는 특별한 표시)
                f_out.write(f"?,{channel_id[1:]}\n")
    
    return matched_count, missing_channels

def main():
    print("=" * 80)
    print("Column List Merger")
    print("=" * 80)
    
    # 1. group 매핑 로드
    print("\n📂 Loading group mapping from column_list_2562.txt...")
    mapping = load_group_mapping('column_list_2562.txt')
    print(f"✅ Loaded {len(mapping)} channel-group mappings")
    
    # 2. long list 처리
    print("\n📂 Processing column_list_long.txt...")
    matched_count, missing_channels = process_long_list(
        'column_list_long.txt',
        mapping,
        'column_list_long_with_groups.txt'
    )
    
    # 3. 결과 출력
    print("\n" + "=" * 80)
    print("Results")
    print("=" * 80)
    print(f"✅ Matched channels: {matched_count}")
    print(f"❌ Missing channels: {len(missing_channels)}")
    
    if missing_channels:
        print("\n⚠️  Channels in column_list_long.txt but NOT in column_list_2562.txt:")
        print("-" * 80)
        for idx, channel in enumerate(missing_channels, 1):
            print(f"{idx:4d}. {channel}")
    
    print("\n💾 Output file: column_list_long_with_groups.txt")
    print("   (Channels with '?' as group are missing from column_list_2562.txt)")
    
if __name__ == "__main__":
    main()

