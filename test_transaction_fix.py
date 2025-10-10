#!/usr/bin/env python3
"""
테스트 스크립트: 트랜잭션 커밋 문제 검증
"""
from datetime import datetime, timedelta
from database import db_manager
from chunked_migration_strategy import chunked_migration_strategy
from loguru import logger

def test_batch_insert():
    """배치 삽입이 제대로 커밋되는지 테스트"""
    
    # 테스트 데이터 준비
    test_data = []
    base_time = datetime.now()
    
    for i in range(10):
        test_data.append({
            'created_time': base_time + timedelta(minutes=i),
            'test_column_1': f'value_{i}',
            'test_column_2': f'value_{i * 2}'
        })
    
    logger.info("=" * 60)
    logger.info("트랜잭션 커밋 테스트 시작")
    logger.info("=" * 60)
    
    # 테스트 테이블명
    table_name = 'tbl_data_timeseries_imo9976927'
    
    try:
        # 배치 삽입 실행
        logger.info(f"📦 테스트 데이터 {len(test_data)}개 삽입 중...")
        result = chunked_migration_strategy._insert_batch(table_name, test_data)
        
        logger.info(f"✅ 삽입 완료: {result} rows affected")
        
        # 검증: 실제로 데이터가 들어갔는지 확인
        start_time = min(row['created_time'] for row in test_data)
        end_time = max(row['created_time'] for row in test_data)
        
        verify_query = f"""
        SELECT COUNT(*) as count 
        FROM tenant.{table_name} 
        WHERE created_time >= %s AND created_time <= %s
        """
        verify_result = db_manager.execute_query(verify_query, (start_time, end_time))
        actual_count = verify_result[0]['count'] if verify_result else 0
        
        logger.info(f"🔍 검증 결과: 데이터베이스에 {actual_count}개 레코드 존재")
        
        if actual_count > 0:
            logger.info("✅ 테스트 성공: 데이터가 정상적으로 커밋되었습니다!")
            return True
        else:
            logger.error("❌ 테스트 실패: 데이터가 커밋되지 않았습니다!")
            return False
            
    except Exception as e:
        logger.error(f"❌ 테스트 실패: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_batch_insert()
    exit(0 if success else 1)

