# 마이그레이션 전략 (Migration Strategy)

## 📋 개요

PostgreSQL Narrow-to-Wide 테이블 마이그레이션을 위한 종합적인 전략 문서입니다. 대용량 실시간 데이터를 처리하기 위한 다양한 마이그레이션 방법과 최적화 기법을 다룹니다.

## 🎯 마이그레이션 목표

### 주요 목표
- **Zero-Downtime**: 서비스 중단 없이 마이그레이션 수행
- **High Performance**: 대용량 데이터 처리 최적화
- **Data Integrity**: 데이터 무결성 보장
- **Real-time Processing**: 실시간 데이터 처리 연속성

### 기술적 요구사항
- **소스 테이블**: `tenant.tbl_data_timeseries` (Narrow format)
- **타겟 테이블**: `tenant.tbl_data_timeseries_{ship_id}` (Wide format)
- **데이터 볼륨**: 1분 간격, 15초 데이터 수집
- **처리 방식**: 실시간 + 배치 하이브리드

## 🚀 마이그레이션 방법

### 방법 1: 동시 처리 전략 (Concurrent Strategy) - 권장

#### 특징
- 실시간 데이터 처리와 배치 마이그레이션을 동시에 실행
- 멀티스레딩을 통한 효율적인 리소스 활용
- 자동 종료 로직 (배치 완료 시)

#### 실행 순서
```bash
# 1. 스키마 분석 및 테이블 생성
python3 main.py --mode schema-only

# 2. 동시 마이그레이션 시작
./start_batch.sh concurrent
```

#### 장점
- 최단 시간 내 마이그레이션 완료
- 실시간 데이터 처리 중단 없음
- 리소스 최적화

### 방법 2: 하이브리드 전략 (Hybrid Strategy)

#### 특징
- 더 정교한 모니터링과 복구 기능
- 단계별 진행 상황 추적
- 오류 발생 시 자동 복구

#### 실행
```bash
python3 main.py --mode hybrid
```

### 방법 3: 스트리밍 전략 (Streaming Strategy)

#### 특징
- 모든 데이터를 연속 스트림으로 처리
- 메모리 사용량 최적화
- 실시간 처리 우선

#### 실행
```bash
python3 main.py --mode streaming
```

### 방법 4: 안전한 순차 마이그레이션 (전통적 방법)

#### 실행 순서
```bash
# 1. 스키마 분석
python3 main.py --mode schema-only

# 2. 테이블 생성
python3 main.py --mode table-only

# 3. 데이터 마이그레이션 (cutoff_time 설정)
python3 main.py --mode migration-only

# 4. 실시간 처리 시작
python3 main.py --mode realtime-only
```

## 📊 성능 최적화

### PostgreSQL 최적화

#### 실시간 처리용 설정
```bash
export PGOPTIONS="
    -c work_mem=16MB
    -c maintenance_work_mem=64MB
    -c max_parallel_workers_per_gather=0
    -c max_parallel_workers=0
    -c max_parallel_maintenance_workers=0
    -c random_page_cost=1.1
    -c checkpoint_completion_target=0.9
"
```

#### 배치 처리용 설정
```bash
export PGOPTIONS="
    -c work_mem=32MB
    -c maintenance_work_mem=128MB
    -c max_parallel_workers_per_gather=0
    -c max_parallel_workers=0
    -c max_parallel_maintenance_workers=0
    -c random_page_cost=1.1
"
```

### 데이터 처리 최적화

#### 청크 기반 처리
- **청크 크기**: 24시간 단위
- **배치 크기**: 50,000 레코드
- **메모리 제한**: 10,000 레코드 (추출 시)

#### COPY 명령어 활용
- **추출**: `COPY TO STDOUT WITH CSV`
- **삽입**: `COPY FROM STDIN WITH CSV`
- **성능**: 일반 INSERT 대비 10-50배 빠름

## 🔄 데이터 처리 흐름

### 실시간 데이터 처리
```
새로운 데이터 → 실시간 처리 → Wide 테이블 삽입
     ↓
cutoff_time 이후 데이터만 처리
```

### 배치 마이그레이션
```
과거 데이터 → 청크 분할 → 변환 → Wide 테이블 삽입
     ↓
cutoff_time 이전 데이터만 처리
```

### 동시 처리
```
실시간 데이터 ──┐
                ├─→ Wide 테이블
과거 데이터 ────┘
```

## 📈 모니터링 및 로깅

### 상세 로그 정보

#### 실시간 처리 로그
```
✅ REALTIME INSERT SUCCESS: tbl_data_timeseries_imo9999994
   📊 Records: 2 rows inserted
   📊 Columns: 1037 data columns (total: 1038)
   📊 Time Range: 2025-10-03 00:24:08 ~ 2025-10-03 00:24:08
   📊 Affected Rows: 2
```

#### 배치 마이그레이션 로그
```
✅ BATCH INSERT SUCCESS: tbl_data_timeseries_imo9999994
   📊 Records: 1000 rows inserted
   📊 Columns: 1037 data columns (total: 1038)
   📊 Time Range: 2025-10-02 00:00:00 ~ 2025-10-02 23:59:59
   📊 Affected Rows: 1000
```

#### 청크 마이그레이션 로그
```
✅ CHUNK MIGRATION SUCCESS: IMO9999994
   📊 Records processed: 50000
   📊 Columns: 1037 data columns
   📊 Time Range: 2025-10-02 00:00:00 ~ 2025-10-02 23:59:59
   📊 Method: Chunked migration (24-hour chunks)
```

### 모니터링 명령어
```bash
# 실시간 로그 팔로우
./view_logs.sh -f realtime

# 배치 로그 팔로우
./view_logs.sh -f batch

# 에러 메시지만 확인
./view_logs.sh -e

# 성공 메시지만 확인
./view_logs.sh -s

# 로그 통계
./view_logs.sh -c
```

## 🛡️ 안전장치 및 복구

### 데이터 무결성 보장
- **Primary Key**: `created_time` 기반 충돌 해결
- **ON CONFLICT**: `DO UPDATE SET` 또는 `DO NOTHING`
- **트랜잭션**: 배치 단위 트랜잭션 처리

### 오류 처리
- **자동 재시도**: 네트워크 오류 시 재시도
- **체크포인트**: 진행 상황 저장 및 복구
- **롤백**: 실패 시 이전 상태로 복구

### 리소스 관리
- **메모리 제한**: 대용량 쿼리 시 LIMIT 적용
- **연결 풀**: 데이터베이스 연결 효율적 관리
- **CPU 제한**: 병렬 처리 제한으로 시스템 안정성

## 📋 각 단계별 주의사항

### 스키마 분석 단계
- **샘플 데이터**: 최근 60분 데이터 사용
- **컬럼 필터링**: `column_list.txt` 기반 허용 컬럼만 처리
- **메모리 사용량**: 대용량 샘플 데이터 처리 시 주의

### 테이블 생성 단계
- **기존 테이블**: 이미 존재하는 경우 건너뛰기
- **인덱스**: `created_time` 기반 인덱스 자동 생성
- **컬럼명**: 특수문자 포함 컬럼명 따옴표 처리

### 데이터 마이그레이션 단계
- **청크 크기**: 24시간 단위로 분할
- **배치 크기**: 50,000 레코드씩 처리
- **메모리 제한**: 10,000 레코드씩 추출

### 실시간 처리 단계
- **cutoff_time**: 마이그레이션 완료 시점 저장
- **중복 처리**: 이미 처리된 데이터 건너뛰기
- **연속성**: 프로세스 재시작 시 이어서 처리

## 🔧 고급 설정

### 환경 변수
```bash
# 배치 크기 조정
export BATCH_SIZE=50000

# 청크 크기 조정 (시간)
export CHUNK_SIZE_HOURS=24

# 병렬 워커 수 조정
export PARALLEL_WORKERS=4
```

### 데이터베이스 설정
```sql
-- 인덱스 최적화
CREATE INDEX CONCURRENTLY idx_timeseries_ship_time 
ON tenant.tbl_data_timeseries (ship_id, created_time);

-- 통계 정보 업데이트
ANALYZE tenant.tbl_data_timeseries;
```

## 📊 성능 벤치마크

### 예상 성능
- **실시간 처리**: 1분 간격, 지연 시간 < 5초
- **배치 처리**: 100만 레코드/시간
- **메모리 사용량**: < 1GB (청크 단위)
- **디스크 I/O**: COPY 명령어로 최적화

### 모니터링 지표
- **처리량**: 레코드/초
- **지연 시간**: 데이터 수집부터 삽입까지
- **에러율**: 실패한 배치 비율
- **리소스 사용률**: CPU, 메모리, 디스크
