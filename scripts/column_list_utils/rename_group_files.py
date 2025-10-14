#!/usr/bin/env python3
"""
그룹 파일명을 내용에 맞게 변경하는 스크립트
"""
import os
from collections import Counter

def analyze_file_content(filename):
    """파일 내용을 분석하여 주요 시스템 찾기"""
    systems = []
    
    with open(filename, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            # hs4sd_v1/ 이후의 첫 번째 부분이 시스템명
            parts = line.split('/')
            if len(parts) >= 2:
                system = parts[1]  # hs4sd_v1 다음
                systems.append(system)
    
    # 가장 많이 나오는 시스템들
    counter = Counter(systems)
    return counter

def suggest_filename(group_num, counter):
    """그룹 번호와 시스템 카운터로 적절한 파일명 제안"""
    top_systems = counter.most_common(5)
    
    print(f"\n📊 Group {group_num} 분석:")
    print(f"   총 채널 수: {sum(counter.values())}")
    print(f"   고유 시스템 수: {len(counter)}")
    print(f"   상위 5개 시스템:")
    for system, count in top_systems:
        percentage = (count / sum(counter.values())) * 100
        print(f"      - {system}: {count}개 ({percentage:.1f}%)")
    
    # 그룹 특성에 따라 이름 제안
    if group_num == 1:
        # ab, ct, fgc, bwts, aprs 등 보조 시스템
        return "auxiliary_systems"
    elif group_num == 2:
        # ge, me - 주기관 및 발전기
        return "engine_generator"
    elif group_num == 3:
        # ship, vdr, vap - 항해 및 선박 정보
        return "navigation_ship"
    
    return f"group_{group_num}"

def main():
    print("=" * 80)
    print("Group File Renamer")
    print("=" * 80)
    
    rename_map = {}
    
    # 각 그룹 파일 분석
    for group_num in [1, 2, 3]:
        old_filename = f"column_list_group_{group_num}.txt"
        
        if not os.path.exists(old_filename):
            print(f"⚠️  {old_filename} not found, skipping...")
            continue
        
        counter = analyze_file_content(old_filename)
        new_name = suggest_filename(group_num, counter)
        new_filename = f"column_list_{new_name}.txt"
        
        rename_map[old_filename] = new_filename
    
    # 이름 변경 확인
    print("\n" + "=" * 80)
    print("제안된 파일명 변경:")
    print("=" * 80)
    for old, new in rename_map.items():
        print(f"  {old}")
        print(f"  → {new}")
        print()
    
    print("\n🔄 파일명 변경 중...")
    for old, new in rename_map.items():
        os.rename(old, new)
        print(f"✅ {old} → {new}")
    
    print("\n✅ 모든 파일명이 변경되었습니다!")

if __name__ == "__main__":
    main()

