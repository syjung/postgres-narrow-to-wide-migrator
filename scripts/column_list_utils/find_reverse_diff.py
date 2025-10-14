#!/usr/bin/env python3
"""
column_list_2562.txt에는 있지만 column_list_long.txt에는 없는 항목 찾기
"""

def load_channels_from_2562(file_path):
    """column_list_2562.txt에서 channel_id 로드"""
    channels = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            parts = line.split(',', 1)
            if len(parts) == 2:
                channel_id = parts[1]
                channels.append(channel_id)
    
    return set(channels)

def load_channels_from_long(file_path):
    """column_list_long.txt에서 channel_id 로드"""
    channels = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            channel_id = line.strip()
            if not channel_id:
                continue
            # 앞의 / 제거
            if channel_id.startswith('/'):
                channel_id = channel_id[1:]
            channels.append(channel_id)
    
    return set(channels)

def main():
    print("=" * 80)
    print("Reverse Difference Finder")
    print("=" * 80)
    
    # 1. 두 파일 로드
    print("\n📂 Loading channels from column_list_2562.txt...")
    channels_2562 = load_channels_from_2562('column_list_2562.txt')
    print(f"✅ Loaded {len(channels_2562)} channels")
    
    print("\n📂 Loading channels from column_list_long.txt...")
    channels_long = load_channels_from_long('column_list_long.txt')
    print(f"✅ Loaded {len(channels_long)} channels")
    
    # 2. 차이점 찾기
    only_in_2562 = channels_2562 - channels_long
    
    # 3. 결과 출력
    print("\n" + "=" * 80)
    print("Results")
    print("=" * 80)
    print(f"📊 Channels in column_list_2562.txt: {len(channels_2562)}")
    print(f"📊 Channels in column_list_long.txt: {len(channels_long)}")
    print(f"❗ Only in column_list_2562.txt: {len(only_in_2562)}")
    
    if only_in_2562:
        print("\n⚠️  Channels in column_list_2562.txt but NOT in column_list_long.txt:")
        print("-" * 80)
        for idx, channel in enumerate(sorted(only_in_2562), 1):
            print(f"{idx:4d}. {channel}")
    
if __name__ == "__main__":
    main()

