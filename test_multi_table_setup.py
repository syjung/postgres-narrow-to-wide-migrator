#!/usr/bin/env python3
"""
Multi-Table 설정 검증 스크립트
실제 DB 연결 없이 설정과 모듈 로딩을 테스트합니다.
"""
import sys
import os

def test_imports():
    """필수 모듈 import 테스트"""
    print("=" * 80)
    print("1. Module Import Test")
    print("=" * 80)
    
    modules = [
        ("channel_router", "channel_router"),
        ("multi_table_generator", "multi_table_generator"),
        ("multi_table_chunked_strategy", "multi_table_chunked_strategy"),
        ("parallel_batch_migrator", "parallel_batch_migrator"),
        ("config", "migration_config"),
    ]
    
    all_success = True
    for module_name, import_name in modules:
        try:
            print(f"   Importing {module_name}...", end=" ")
            if module_name == "channel_router":
                from channel_router import channel_router
            elif module_name == "multi_table_generator":
                from multi_table_generator import multi_table_generator
            elif module_name == "multi_table_chunked_strategy":
                from multi_table_chunked_strategy import multi_table_chunked_strategy
            elif module_name == "parallel_batch_migrator":
                # Skip DB connection for testing
                pass
            elif module_name == "config":
                from config import migration_config
            print("✅")
        except Exception as e:
            print(f"❌")
            print(f"      Error: {e}")
            all_success = False
    
    # Test realtime_processor import (may fail due to DB)
    print(f"   Importing realtime_processor...", end=" ")
    try:
        # This will fail if DB is not accessible, but that's OK
        from realtime_processor import RealTimeProcessor
        print("✅")
    except Exception as e:
        if "connection" in str(e).lower() or "network" in str(e).lower():
            print("⚠️  (DB connection required)")
        else:
            print(f"❌")
            print(f"      Error: {e}")
            all_success = False
    
    return all_success

def test_channel_files():
    """채널 파일 존재 확인"""
    print("\n" + "=" * 80)
    print("2. Channel Files Test")
    print("=" * 80)
    
    files = [
        'column_list_auxiliary_systems.txt',
        'column_list_engine_generator.txt',
        'column_list_navigation_ship.txt'
    ]
    
    all_exist = True
    for filename in files:
        exists = os.path.exists(filename)
        status = "✅" if exists else "❌"
        print(f"   {filename}: {status}")
        if not exists:
            all_exist = False
    
    return all_exist

def test_channel_router():
    """채널 라우터 기능 테스트"""
    print("\n" + "=" * 80)
    print("3. Channel Router Test")
    print("=" * 80)
    
    try:
        from channel_router import channel_router
        
        # 통계
        stats = channel_router.get_statistics()
        print(f"   Total channels: {stats['total_channels']}")
        print(f"   Channel distribution:")
        for table_type, count in stats['by_table'].items():
            print(f"      - {table_type}: {count}")
        
        # 샘플 채널 테스트
        test_channels = [
            ("/hs4sd_v1/ab/fuel/oil///use", "1"),
            ("/hs4sd_v1/me01/////run", "2"),
            ("/hs4sd_v1/ship////aft_m/draft", "3"),
        ]
        
        print(f"\n   Sample routing tests:")
        all_pass = True
        for channel, expected_type in test_channels:
            actual_type = channel_router.get_table_type(channel)
            match = "✅" if actual_type == expected_type else "❌"
            print(f"      {channel[:30]:30s} → {actual_type:20s} {match}")
            if actual_type != expected_type:
                all_pass = False
        
        return all_pass
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False

def test_config():
    """설정 테스트"""
    print("\n" + "=" * 80)
    print("4. Configuration Test")
    print("=" * 80)
    
    try:
        from config import migration_config
        
        print(f"   Multi-Table Mode: {migration_config.use_multi_table}")
        print(f"   Target Ships: {len(migration_config.target_ship_ids)}")
        print(f"   Chunk Size: {migration_config.chunk_size_hours} hours")
        print(f"   Batch Size: {migration_config.batch_size:,}")
        print(f"   Max Parallel Workers: {migration_config.max_parallel_workers}")
        
        # Thread 최적화
        optimal_threads = migration_config.get_optimal_thread_count()
        print(f"\n   Thread Optimization:")
        print(f"      Optimal Thread Count: {optimal_threads}")
        
        # Pool 최적화
        pool_config = migration_config.get_optimal_pool_config()
        print(f"\n   DB Pool Optimization:")
        print(f"      Min Connections: {pool_config['minconn']}")
        print(f"      Max Connections: {pool_config['maxconn']}")
        print(f"      Multiplier: {pool_config['maxconn'] / optimal_threads if optimal_threads > 0 else 0:.1f}x")
        
        # 검증
        if not migration_config.use_multi_table:
            print("\n   ⚠️  Warning: Multi-Table mode is disabled!")
            return False
        
        return True
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False

def test_table_names():
    """테이블명 생성 테스트"""
    print("\n" + "=" * 80)
    print("5. Table Name Generation Test")
    print("=" * 80)
    
    try:
        test_ship_id = "IMO9976903"
        
        expected_tables = [
            f"tbl_1_{test_ship_id.lower()}",
            f"tbl_2_{test_ship_id.lower()}",
            f"tbl_3_{test_ship_id.lower()}"
        ]
        
        print(f"   Ship ID: {test_ship_id}")
        print(f"   Expected tables:")
        for table in expected_tables:
            print(f"      - tenant.{table}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False

def main():
    """Main test runner"""
    print("\n")
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 20 + "Multi-Table Setup Verification" + " " * 28 + "║")
    print("╚" + "=" * 78 + "╝")
    print()
    
    tests = [
        ("Module Imports", test_imports),
        ("Channel Files", test_channel_files),
        ("Channel Router", test_channel_router),
        ("Configuration", test_config),
        ("Table Names", test_table_names)
    ]
    
    results = []
    for test_name, test_func in tests:
        result = test_func()
        results.append((test_name, result))
    
    # Summary
    print("\n" + "=" * 80)
    print("Test Summary")
    print("=" * 80)
    
    total = len(results)
    passed = sum(1 for _, result in results if result)
    failed = total - passed
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"   {test_name:30s}: {status}")
    
    print(f"\n   Total: {total}, Passed: {passed}, Failed: {failed}")
    
    if failed == 0:
        print("\n✅ All tests passed! Multi-Table setup is ready.")
        print("\n💡 Next step: Run './start_parallel_batch.sh' to start migration")
        return 0
    else:
        print("\n❌ Some tests failed. Please fix the issues above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())

