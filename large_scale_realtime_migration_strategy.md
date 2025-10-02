# 대용량 실시간 데이터 마이그레이션 전략

## 1. 현재 상황 분석

### 1.1 데이터 현황
- **소스 테이블**: `tenant.tbl_data_timeseries` (narrow type)
- **데이터량**: 대용량 (800만+ 레코드 추정)
- **실시간 데이터**: 1분 간격, 15초 데이터 수집
- **데이터 구조**: ship_id별로 분리된 wide type 테이블로 변환 필요

### 1.2 기술적 도전과제
- **대용량 데이터**: 기존 데이터와 실시간 데이터 동시 처리
- **실시간성**: 1분 간격 데이터 수집 중단 없이 마이그레이션
- **성능**: 메모리 효율성과 처리 속도 최적화
- **안정성**: 데이터 손실 방지 및 롤백 지원

## 2. 마이그레이션 전략 개요

### 2.1 핵심 원칙
1. **Zero-Downtime**: 실시간 데이터 수집 중단 없이 마이그레이션
2. **Data Integrity**: 데이터 손실 및 중복 방지
3. **Performance**: 최적화된 처리 속도
4. **Scalability**: 새로운 ship_id/data_channel_id 자동 처리

### 2.2 전략적 접근법
- **Hybrid Approach**: 기존 데이터 배치 처리 + 실시간 데이터 스트리밍 처리
- **Incremental Migration**: ship_id별 단계적 마이그레이션
- **Dual-Write Pattern**: 마이그레이션 기간 중 양방향 동기화

## 3. 마이그레이션 단계별 계획

### 3.1 Phase 1: 사전 준비 및 분석 (1-2일)

#### 3.1.1 데이터 분석
```sql
-- 전체 데이터량 확인
SELECT COUNT(*) FROM tenant.tbl_data_timeseries;

-- ship_id별 데이터 분포
SELECT ship_id, COUNT(*) as record_count 
FROM tenant.tbl_data_timeseries 
GROUP BY ship_id 
ORDER BY record_count DESC;

-- 최근 데이터 패턴 분석
SELECT 
    DATE_TRUNC('hour', created_time) as hour,
    COUNT(*) as records_per_hour
FROM tenant.tbl_data_timeseries 
WHERE created_time >= NOW() - INTERVAL '24 hours'
GROUP BY hour 
ORDER BY hour;
```

#### 3.1.2 시스템 리소스 확인
- **디스크 공간**: 원본 + 타겟 테이블 공간 확보
- **메모리**: PostgreSQL 설정 최적화 (`work_mem`, `shared_buffers`)
- **네트워크**: 연결 안정성 및 대역폭 확인

#### 3.1.3 백업 및 복구 계획
- **전체 백업**: 마이그레이션 시작 전 전체 데이터베이스 백업
- **Point-in-Time Recovery**: WAL 아카이빙 설정
- **롤백 시나리오**: 실패 시 원상복구 절차

### 3.2 Phase 2: 스키마 생성 및 테이블 준비 (1일)

#### 3.2.1 스키마 분석
```python
# 각 ship_id별 고유 data_channel_id 추출
for ship_id in ship_ids:
    channels = db_manager.get_data_channels_for_ship(ship_id)
    schema = schema_analyzer.analyze_ship_data(ship_id, sample_minutes=60)
```

#### 3.2.2 Wide Table 생성
- **테이블 명**: `tbl_data_timeseries_{ship_id}`
- **컬럼 타입**: 모든 data_channel_id를 text 타입으로 생성
- **인덱스**: `created_time` 기준 인덱스 생성
- **제약조건**: PRIMARY KEY (created_time)

#### 3.2.3 성능 최적화 설정
```sql
-- 인덱스 비활성화 (마이그레이션 중)
ALTER TABLE tenant.tbl_data_timeseries_{ship_id} DISABLE TRIGGER ALL;

-- PostgreSQL 설정 최적화
SET work_mem = '256MB';
SET maintenance_work_mem = '1GB';
SET checkpoint_segments = 32;
```

### 3.3 Phase 3: 기존 데이터 마이그레이션 (3-5일)

#### 3.3.1 배치 마이그레이션 전략
- **방식**: PostgreSQL COPY 명령어 사용
- **배치 크기**: 50,000-100,000 레코드
- **처리 순서**: 데이터량이 적은 ship_id부터 시작

#### 3.3.2 마이그레이션 프로세스
```python
def migrate_historical_data(ship_id: str, cutoff_time: datetime):
    """기존 데이터 마이그레이션"""
    
    # 1. 데이터 추출 (CSV 파일로)
    extract_query = f"""
    COPY (
        SELECT 
            created_time,
            data_channel_id,
            CASE 
                WHEN value_format = 'Decimal' THEN double_v::text
                WHEN value_format = 'Integer' THEN long_v::text
                WHEN value_format = 'String' THEN str_v
                WHEN value_format = 'Boolean' THEN bool_v::text
                ELSE NULL
            END as value
        FROM tenant.tbl_data_timeseries 
        WHERE ship_id = '{ship_id}' 
        AND created_time < '{cutoff_time}'
        ORDER BY created_time
    ) TO '/tmp/migration_data_{ship_id}.csv' WITH CSV HEADER;
    """
    
    # 2. 데이터 변환 (Python에서 wide format으로)
    wide_data = transform_to_wide_format(csv_file)
    
    # 3. 타겟 테이블에 삽입
    insert_query = f"""
    COPY tenant.tbl_data_timeseries_{ship_id} 
    FROM '/tmp/wide_data_{ship_id}.csv' 
    WITH CSV HEADER;
    """
```

#### 3.3.3 진행상황 모니터링
- **실시간 진행률**: 처리된 레코드 수 / 전체 레코드 수
- **성능 지표**: 처리 속도 (레코드/초)
- **에러 추적**: 실패한 배치 및 재시도 로직

### 3.4 Phase 4: 실시간 데이터 동기화 (지속적)

#### 3.4.1 Dual-Write 패턴 구현
```python
class DualWriteProcessor:
    """실시간 데이터를 narrow와 wide 테이블에 동시 기록"""
    
    def process_realtime_data(self, data: Dict[str, Any]):
        """실시간 데이터 처리"""
        
        # 1. 기존 narrow 테이블에 기록 (기존 시스템 유지)
        self.write_to_narrow_table(data)
        
        # 2. 새로운 wide 테이블에 기록
        self.write_to_wide_table(data)
        
        # 3. 동기화 상태 확인
        self.verify_sync_status(data)
```

#### 3.4.2 실시간 처리 최적화
- **배치 처리**: 1분 간격으로 수집된 데이터를 배치로 처리
- **UPSERT 로직**: 중복 데이터 방지
- **동적 스키마**: 새로운 data_channel_id 자동 감지 및 컬럼 추가

#### 3.4.3 동기화 검증
```python
def verify_data_consistency(ship_id: str, time_range: tuple):
    """데이터 일관성 검증"""
    
    narrow_count = get_narrow_table_count(ship_id, time_range)
    wide_count = get_wide_table_count(ship_id, time_range)
    
    if narrow_count != wide_count:
        logger.warning(f"Data inconsistency detected for {ship_id}")
        # 자동 복구 로직 실행
        self.auto_recovery(ship_id, time_range)
```

### 3.5 Phase 5: 검증 및 전환 (1-2일)

#### 3.5.1 데이터 검증
- **레코드 수 일치**: narrow vs wide 테이블 레코드 수 비교
- **데이터 정확성**: 샘플 데이터 값 검증
- **성능 테스트**: 쿼리 성능 비교

#### 3.5.2 애플리케이션 전환
- **점진적 전환**: ship_id별로 애플리케이션 전환
- **A/B 테스트**: 일부 요청을 wide 테이블로 라우팅
- **롤백 준비**: 문제 발생 시 즉시 롤백 가능

#### 3.5.3 최종 정리
- **인덱스 재생성**: 마이그레이션 완료 후 인덱스 최적화
- **통계 업데이트**: PostgreSQL 통계 정보 갱신
- **모니터링 설정**: 성능 모니터링 대시보드 구축

## 4. 성능 최적화 방안

### 4.1 PostgreSQL 설정 최적화
```sql
-- 메모리 설정
work_mem = '256MB'                    -- 정렬/해시 작업용 메모리
maintenance_work_mem = '1GB'          -- 인덱스 생성/유지보수용
shared_buffers = '4GB'                -- 공유 버퍼 크기

-- WAL 설정
wal_buffers = '64MB'                  -- WAL 버퍼 크기
checkpoint_completion_target = 0.9    -- 체크포인트 완료 목표
checkpoint_segments = 32              -- 체크포인트 세그먼트

-- 쿼리 최적화
random_page_cost = 1.1                -- SSD 환경 최적화
effective_cache_size = '8GB'           -- 캐시 크기
```

### 4.2 마이그레이션 최적화 기법

#### 4.2.1 COPY 명령어 활용
```sql
-- 가장 빠른 대량 삽입 방법
COPY table_name FROM '/path/to/file.csv' WITH CSV HEADER;
```

#### 4.2.2 인덱스 최적화
```sql
-- 마이그레이션 전 인덱스 비활성화
DROP INDEX IF EXISTS idx_created_time;

-- 마이그레이션 후 인덱스 재생성
CREATE INDEX CONCURRENTLY idx_created_time 
ON tenant.tbl_data_timeseries_{ship_id} (created_time);
```

#### 4.2.3 병렬 처리
```python
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor

def parallel_migration(ship_ids: List[str]):
    """병렬 마이그레이션 처리"""
    
    with ProcessPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(migrate_ship_data, ship_id) 
            for ship_id in ship_ids
        ]
        
        for future in futures:
            result = future.result()
            logger.info(f"Migration completed: {result}")
```

### 4.3 메모리 효율성
- **스트리밍 처리**: 전체 데이터를 메모리에 로드하지 않음
- **청크 단위 처리**: 작은 단위로 나누어 처리
- **가비지 컬렉션**: 주기적인 메모리 정리

## 5. 위험 요소 및 대응 방안

### 5.1 기술적 위험

#### 5.1.1 메모리 부족 (OOM)
- **위험도**: 높음
- **증상**: 대용량 데이터 처리 시 메모리 부족
- **대응방안**:
  - 청크 단위 배치 처리 (10,000-50,000 레코드)
  - 스트리밍 방식 데이터 처리
  - PostgreSQL 메모리 설정 최적화

#### 5.1.2 디스크 공간 부족
- **위험도**: 중간
- **증상**: 마이그레이션 중 디스크 공간 부족
- **대응방안**:
  - 사전 디스크 공간 확인 및 확보
  - 단계별 마이그레이션 (테이블별)
  - 임시 파일 자동 정리

#### 5.1.3 네트워크 연결 불안정
- **위험도**: 중간
- **증상**: 장시간 작업 중 연결 끊김
- **대응방안**:
  - 연결 풀링 및 자동 재연결
  - 작업 상태 저장 및 재시작 기능
  - 네트워크 모니터링

### 5.2 데이터 위험

#### 5.2.1 데이터 손실
- **위험도**: 높음
- **증상**: 마이그레이션 중 데이터 누락
- **대응방안**:
  - 트랜잭션 기반 처리
  - 백업 및 복구 계획
  - 데이터 검증 로직

#### 5.2.2 데이터 중복
- **위험도**: 중간
- **증상**: 동일한 데이터의 중복 삽입
- **대응방안**:
  - UPSERT 로직 구현
  - 타임스탬프 기반 중복 체크
  - 처리 상태 추적

#### 5.2.3 데이터 타입 변환 오류
- **위험도**: 중간
- **증상**: value_format과 실제 데이터 불일치
- **대응방안**:
  - 데이터 검증 로직 구현
  - 예외 데이터 별도 처리
  - 변환 전 데이터 품질 검사

### 5.3 운영 위험

#### 5.3.1 실시간 처리 중단
- **위험도**: 높음
- **증상**: 실시간 데이터 수집 중단
- **대응방안**:
  - Dual-Write 패턴으로 기존 시스템 유지
  - 자동 재시도 및 알림 시스템
  - 롤백 시나리오 준비

#### 5.3.2 성능 저하
- **위험도**: 중간
- **증상**: 마이그레이션 중 시스템 성능 저하
- **대응방안**:
  - 작업 시간대 조정 (업무 시간 외)
  - 리소스 사용량 모니터링
  - 점진적 마이그레이션

## 6. 모니터링 및 알림

### 6.1 실시간 모니터링 지표
- **마이그레이션 진행률**: 처리된 레코드 수 / 전체 레코드 수
- **처리 속도**: 레코드/초, MB/초
- **에러율**: 실패한 배치 비율
- **시스템 리소스**: CPU, 메모리, 디스크 사용률

### 6.2 알림 시스템
```python
class MigrationAlert:
    """마이그레이션 알림 시스템"""
    
    def send_alert(self, level: str, message: str):
        """알림 전송"""
        if level == 'CRITICAL':
            # 즉시 알림 (SMS, 이메일)
            self.send_immediate_alert(message)
        elif level == 'WARNING':
            # 경고 알림 (이메일)
            self.send_email_alert(message)
        else:
            # 정보 알림 (로그)
            logger.info(message)
```

### 6.3 대시보드
- **실시간 진행상황**: 시각적 진행률 표시
- **성능 지표**: 처리 속도, 에러율 그래프
- **시스템 상태**: 리소스 사용률 모니터링
- **알림 로그**: 최근 알림 및 에러 로그

## 7. 롤백 계획

### 7.1 롤백 시나리오
1. **부분 실패**: 특정 ship_id 마이그레이션 실패
2. **전체 실패**: 전체 마이그레이션 프로세스 실패
3. **성능 문제**: 마이그레이션 후 성능 저하

### 7.2 롤백 절차
```python
def rollback_migration(ship_id: str = None):
    """마이그레이션 롤백"""
    
    if ship_id:
        # 특정 ship_id 롤백
        drop_wide_table(ship_id)
        stop_dual_write(ship_id)
    else:
        # 전체 롤백
        drop_all_wide_tables()
        stop_all_dual_write()
        restore_from_backup()
```

### 7.3 데이터 복구
- **백업 복원**: 전체 데이터베이스 백업에서 복원
- **WAL 복구**: Point-in-Time Recovery 활용
- **수동 복구**: 특정 시점 데이터 수동 복구

## 8. 성공 지표 및 검증

### 8.1 기능적 지표
- **데이터 완전성**: 100% 데이터 마이그레이션 성공
- **실시간 처리**: 99.9% 이상 정확도
- **자동화**: 새로운 ship_id/data_channel_id 자동 처리
- **데이터 타입**: value_format별 매핑 정확도 100%

### 8.2 성능 지표
- **처리 속도**: 기존 대비 15배 이상 향상
- **쿼리 성능**: 기존 대비 50% 이상 향상
- **실시간 지연**: 1초 이내 처리 지연
- **리소스 효율성**: 메모리 사용량 최적화

### 8.3 안정성 지표
- **데이터 손실률**: 0%
- **시스템 가용성**: 99.9% 이상
- **에러 복구**: 자동 복구율 95% 이상
- **롤백 시간**: 30분 이내 완료

## 9. 향후 확장 계획

### 9.1 단기 계획 (1-3개월)
- **웹 인터페이스**: 마이그레이션 관리 웹 UI
- **자동화**: 완전 자동화된 마이그레이션 파이프라인
- **모니터링**: 고도화된 모니터링 대시보드

### 9.2 중기 계획 (3-6개월)
- **다중 테이블**: 다른 narrow type 테이블 지원
- **클라우드 확장**: 클라우드 환경 지원
- **성능 최적화**: 머신러닝 기반 성능 튜닝

### 9.3 장기 계획 (6개월+)
- **실시간 스트리밍**: Kafka 기반 실시간 스트리밍
- **분산 처리**: 여러 데이터베이스 분산 처리
- **AI 기반 최적화**: 인공지능 기반 자동 최적화

---

**문서 버전**: 1.0.0  
**작성일**: 2024년  
**작성자**: 개발팀  
**승인자**: 프로젝트 매니저
