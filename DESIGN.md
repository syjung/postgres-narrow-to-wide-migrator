# PostgreSQL Narrow-to-Wide Migration - Technical Design Document
Version 1.0.0

---

## 1. 시스템 개요

### 1.1 목적
PostgreSQL의 Narrow-type 테이블을 Wide-type 테이블로 변환하여 쿼리 성능과 데이터 접근성 향상

### 1.2 핵심 아키텍처
- **Multi-Table 방식**: 선박당 3개 시스템별 테이블로 분산
- **병렬 처리**: ThreadPoolExecutor 기반 선박별 병렬 처리
- **Dual-Write 패턴**: Batch (과거 데이터) + Realtime (실시간 데이터) 동시 실행

---

## 2. Multi-Table 아키텍처

### 2.1 테이블 분리 전략

**기존 (Single-Table):**
```
선박당 1개 테이블: tbl_data_timeseries_{ship_id}
- 총 1,037개 컬럼 (created_time + 1,036 data channels)
- 문제: 쿼리 시 전체 컬럼 스캔 필요
```

**신규 (Multi-Table):**
```
선박당 3개 테이블:
1. tbl_data_timeseries_{ship_id}_1 (Auxiliary Systems) - 348개 컬럼
2. tbl_data_timeseries_{ship_id}_2 (Engine/Generator) - 651개 컬럼
3. tbl_data_timeseries_{ship_id}_3 (Navigation/Ship) - 41개 컬럼

장점:
- 필요한 테이블만 쿼리 (I/O 감소)
- 테이블 크기 감소 (캐싱 효율 증가)
- 병렬 INSERT 가능
```

### 2.2 채널 분류

#### Table 1: Auxiliary Systems (347 channels)
- **파일**: `column_list_auxiliary_systems.txt`
- **시스템**:
  - Cargo Tanks (ct01~ct04): 207 channels
  - APRS (Air Pressure Relief System): 25 channels
  - Boilers (ab01~ab02): 20 channels
  - Fuel Gas Compressor (fgc01~fgc02): 22 channels
  - Others: bwts, cd, cg_mach, dfge, fgp, fv, gc, gcu

#### Table 2: Engine/Generator (650 channels)
- **파일**: `column_list_engine_generator.txt`
- **시스템**:
  - Main Engines (me01~me02): 298 channels
  - Generators (ge01~ge04): 342 channels
  - Gas Main System (gms): 2 channels
  - Generator Lines (ge_lin01~ge_lin02): 16 channels

#### Table 3: Navigation/Ship (40 channels)
- **파일**: `column_list_navigation_ship.txt`
- **시스템**:
  - VAP (Vapour System): 18 channels
  - VDR (Voyage Data Recorder): 17 channels
  - Ship Information: 5 channels

### 2.3 Channel Routing

**ChannelRouter 클래스**:
```python
class ChannelRouter:
    def get_table_type(self, channel_id: str) -> str:
        # '/hs4sd_v1/ab/...' → '1' (Auxiliary)
        # '/hs4sd_v1/me01/...' → '2' (Engine)
        # '/hs4sd_v1/ship/...' → '3' (Navigation)
```

**매핑 규칙**:
- Channel ID를 파일 기반으로 로딩
- 정확한 문자열 매칭
- Leading slash 포함 (`/hs4sd_v1/...`)

---

## 3. 데이터베이스 스키마

### 3.1 테이블 구조

```sql
CREATE TABLE IF NOT EXISTS tenant.tbl_data_timeseries_{ship_id}_{table_type} (
    created_time TIMESTAMP NOT NULL,
    "/hs4sd_v1/channel/path/..." DOUBLE PRECISION,
    -- ... (channel IDs as column names, quoted)
    CONSTRAINT tbl_data_timeseries_{ship_id}_{table_type}_pk PRIMARY KEY (created_time)
);
```

**특징**:
- `created_time`: PRIMARY KEY (타임스탬프 기준 정렬)
- 채널 ID: 컬럼명으로 사용 (quoted - 특수문자 포함)
- Data type: DOUBLE PRECISION (모든 데이터 채널)

### 3.2 인덱스 전략

**BRIN Index** (Block Range Index):
```sql
CREATE INDEX IF NOT EXISTS idx_{table_name}_created_time 
ON tenant.{table_name} 
USING BRIN (created_time);
```

**선택 이유**:
- 시계열 데이터에 최적화
- 인덱스 크기 최소 (1/1000 이하)
- 순차 INSERT 성능 우수
- Time-range 쿼리 고속화

---

## 4. 마이그레이션 전략

### 4.1 Chunked Migration Strategy

**2시간 단위 청크 처리**:
```
Time Range: [start_time, end_time]
  ↓
Chunks: [00:00-02:00], [02:00-04:00], ..., [22:00-00:00]
  ↓
각 Chunk별로:
  1. Extract: Narrow table에서 조회
  2. Transform: Wide format으로 변환
  3. Load: 3개 테이블에 분산 INSERT
```

**Chunk 크기**: `chunk_size_hours = 2`
- 메모리 효율
- 실패 시 재처리 범위 최소화

### 4.2 Data Flow

```
Narrow Table (tenant.tbl_data_timeseries)
  ↓ SELECT (ship_id, created_time range)
Raw Data (ship_id, data_channel_id, created_time, value)
  ↓ Group by created_time
Grouped Data {timestamp: [channels]}
  ↓ Channel Routing
Table 1 Data / Table 2 Data / Table 3 Data
  ↓ Prepare Wide Row
row = {created_time, "/ch1": val1, "/ch2": val2, ...}
  ↓ Batch INSERT (executemany, 50K/batch)
Wide Tables (3 tables)
```

### 4.3 INSERT 최적화

**psycopg2.extras.execute_batch 사용**:
```python
execute_batch(cursor, insert_query, values_list, page_size=50000)
```

**vs COPY 비교**:
| 방식 | 속도 | 유연성 | 에러 처리 |
|------|------|--------|-----------|
| COPY | ⚡⚡⚡ 가장 빠름 | 제한적 | 어려움 |
| executemany | ⚡⚡ 빠름 | 유연함 | 쉬움 |

**선택**: executemany
- ON CONFLICT 지원
- 에러 핸들링 용이
- 충분한 성능

---

## 5. 동시 실행 아키텍처 (Batch + Realtime)

### 5.1 Cutoff Time 관리

**분리된 Cutoff Time**:
```
선박별 2개 파일:
1. cutoff_times/imo9976903_batch.txt    - Batch 진행상황
2. cutoff_times/imo9976903_realtime.txt - Realtime 시작점
```

**동작 원리**:
```
Batch:
  - Start: batch_cutoff_time (또는 lookback_days 전)
  - End: realtime_cutoff_time (또는 현재)
  - Update: 각 chunk 완료 시마다 batch_cutoff_time 갱신

Realtime:
  - Start: realtime_cutoff_time (또는 5분 전)
  - 첫 실행 시 realtime_cutoff_time 기록 → Batch의 종료점
  - Update: 데이터 처리 시마다 realtime_cutoff_time 갱신
```

**중복/누락 방지**:
- Batch는 `realtime_cutoff_time` 전까지만 처리
- Realtime은 `realtime_cutoff_time` 이후부터 처리
- 명확한 경계선

### 5.2 병렬 처리 구조

```
ParallelBatchMigrator
  ↓ ThreadPoolExecutor (8 workers)
Ship 1: Thread 1 → 3 tables
Ship 2: Thread 2 → 3 tables
...
Ship 8: Thread 8 → 3 tables
  ↓
DB Connection Pool (8-24 connections)
  - Multi-table: threads × 3
  - Single-table: threads × 2
```

---

## 6. Realtime Processing

### 6.1 아키텍처

```
Every 1 minute:
  ↓
For each ship (parallel):
  1. Get cutoff_time from {ship_id}_realtime.txt
  2. Query: created_time >= cutoff_time
  3. Group by timestamp
  4. Route channels to 3 tables
  5. Prepare wide rows
  6. INSERT (with ON CONFLICT DO NOTHING)
  7. Update cutoff_time
```

### 6.2 Processed Timestamps Cache

**목적**: 중복 처리 방지 (≥ 연산자로 인한 마지막 timestamp 재조회)

**메커니즘**:
```python
processed_timestamps: Set[datetime]  # 메모리 캐시

- INSERT 성공 후에만 추가
- 2분 이상 오래된 timestamp 자동 제거
- DB verification으로 안전성 보장
```

**안전장치**:
1. has_data_to_insert: 데이터 있을 때만 캐싱
2. DB verification: Cache에 있는데 DB에 없으면 재처리
3. discard() 사용: KeyError 방지

---

## 7. CSV Migration Data Upsert

### 7.1 목적
Batch 완료 후 보정 데이터를 CSV에서 import

### 7.2 처리 흐름

```
migration_data/
  ├── H2546/ (IMO9976903)
  │   ├── H2546_2024-12.csv
  │   └── H2546_2025-01.csv
  └── ...

CSV Format:
  timestamp,/hs4sd_v1/ch1,/hs4sd_v1/ch2,...
  2024-12-07 00:00:00,15.01,7.55,...

Processing:
  1. Read CSV header → classify channels
  2. Channel mapping: normalized ↔ original
  3. Batch UPSERT (1000 rows)
  4. ON CONFLICT (created_time) DO UPDATE
```

### 7.3 UPSERT 동작

```sql
INSERT INTO tenant.tbl_data_timeseries_{ship_id}_{table_type}
    (created_time, "/ch1", "/ch2", ...)
VALUES (timestamp, val1, val2, ...)
ON CONFLICT (created_time)
DO UPDATE SET
    "/ch1" = EXCLUDED."/ch1",
    "/ch2" = EXCLUDED."/ch2"
```

**동작**:
- **기존 row**: CSV의 컬럼만 업데이트, 나머지 유지
- **신규 row**: CSV의 컬럼 채움, 나머지 NULL

---

## 8. 성능 최적화

### 8.1 Thread & Connection Pool

```python
# 동적 계산 (config.py)
ship_count = 8
optimal_threads = 8  # 1:1 매핑

# Multi-table mode
maxconn = threads × 3 = 24  # 각 선박이 3개 테이블 사용

# Connection pool
minconn = threads = 8
maxconn = 24
```

### 8.2 배치 처리 최적화

**고정 시간 범위 (Lookback)**:
```python
# 기존: MIN/MAX 쿼리 (매우 느림!)
# 신규: 고정 lookback period
batch_lookback_days = 365  # 1년

start_time = batch_cutoff_time (또는 now - 365일)
end_time = realtime_cutoff_time (또는 now)
```

**채널 필터링**:
```python
# 기존: WHERE data_channel_id IN (...) (1,037개 - 느림!)
# 신규: 전체 조회 후 application에서 필터링
all_channels = channel_router.get_all_channels()
filtered = [row for row in data if row['data_channel_id'] in all_channels]
```

### 8.3 로그 최적화

**Thread-specific Logging**:
```
logs/ship_{ship_id}_{mode}.log
- ship_IMO9976903_batch.log
- ship_IMO9976903_realtime.log

Format: [IMO9976903:Thread-12345] message
```

**Log Levels**:
- DEBUG: 상세 디버깅 정보
- INFO: 핵심 진행 상황
- WARNING: 주의 필요 상황
- ERROR: 에러 및 예외

---

## 9. 데이터 정합성

### 9.1 PRIMARY KEY
```sql
CONSTRAINT {table_name}_pk PRIMARY KEY (created_time)
```
- 타임스탬프당 1 row 보장
- ON CONFLICT 기반 UPSERT

### 9.2 Column Naming
```python
# Channel ID를 그대로 컬럼명으로 사용
channel_id = '/hs4sd_v1/me01/fuel/oil//in_c/temp'
column_name = '"/hs4sd_v1/me01/fuel/oil//in_c/temp"'  # Quoted
```

**Quoting 필수**:
- `/` 포함 (경로)
- 연속 `//` (빈 세그먼트)
- 특수문자

### 9.3 NULL 처리
- 데이터 없음: NULL
- 값 변환 실패: NULL
- CSV 부분 업데이트: NULL (신규 row), 기존값 유지 (기존 row)

---

## 10. 모듈 아키텍처

### 10.1 핵심 모듈

```
config.py
  ├── MigrationConfig: use_multi_table, threads, pool
  └── get_optimal_pool_config()

channel_router.py
  ├── ChannelRouter: 채널 → 테이블 타입 매핑
  ├── get_table_type(channel_id) → '1'/'2'/'3'
  └── get_all_channels() → Set[str]

multi_table_generator.py
  ├── MultiTableGenerator: 테이블 생성
  └── ensure_all_tables_exist(ship_id)

multi_table_chunked_strategy.py
  ├── get_data_chunks(ship_id, cutoff_time)
  └── migrate_chunk(ship_id, start, end)

parallel_batch_migrator.py
  ├── ParallelBatchMigrator: 선박 병렬 처리
  └── migrate_all_ships_parallel()

realtime_processor.py
  ├── RealtimeProcessor: 실시간 데이터 처리
  ├── _process_batch_multi_table()
  └── processed_timestamps 관리

cutoff_time_manager.py
  ├── save_batch_cutoff_time(ship_id, time)
  ├── load_batch_cutoff_time(ship_id)
  ├── save_realtime_cutoff_time(ship_id, time)
  └── load_realtime_cutoff_time(ship_id)

upsert_migration_data.py
  ├── CSVMigrationUpserter: CSV 데이터 import
  ├── Channel mapping: normalized ↔ original
  └── Batch UPSERT with coverage check
```

### 10.2 데이터 흐름

```
Database (Narrow Table)
  ↓ (Batch/Realtime)
ExtractData → TransformData → LoadData
  ↓ (ChannelRouter)
Table 1 / Table 2 / Table 3
  ↓ (BRIN Index)
Fast time-range queries
```

---

## 11. 성능 특성

### 11.1 처리 속도

**Batch Migration**:
- 추출: 1M rows in 30-90s (≥ 연산 + BRIN)
- 변환: ~5s (in-memory)
- 삽입: 1M rows in 10-20s (executemany)
- **총합**: ~100s/chunk (2시간 데이터)

**Realtime Processing**:
- 조회: ~0.2s (15K rows)
- 처리: ~0.05s
- 삽입: ~0.03s
- **총합**: <1s (1분 데이터)

### 11.2 메모리 사용

- Chunk 단위 처리: 2시간 데이터만 메모리 로딩
- Streaming 방식: 배치별 commit
- Connection pooling: 최대 24 connections
- **예상 메모리**: <500MB per ship thread

### 11.3 I/O 최적화

- BRIN index: 순차 쓰기 최적화
- Batch INSERT: Network round-trip 감소
- 채널 필터링: Application 레벨 (DB 부하 감소)

---

## 12. 확장성 및 유지보수

### 12.1 새 선박 추가
```python
# config.py
target_ship_ids = [
    'IMO9976903',
    'IMO9976915',
    # ... add new ship
]
```
- 코드 변경 없음
- 테이블 자동 생성

### 12.2 새 채널 추가
1. 채널 분류 파일 업데이트:
   - `column_list_auxiliary_systems.txt`
   - `column_list_engine_generator.txt`
   - `column_list_navigation_ship.txt`
2. 테이블 재생성 또는 ALTER TABLE

### 12.3 설정 변경
```python
# config.py
chunk_size_hours = 2       # 청크 크기
batch_size = 50000         # INSERT 배치 크기
batch_lookback_days = 365  # Batch 처리 범위
```

---

## 13. 에러 처리 및 복구

### 13.1 Batch Migration 에러
- Chunk 단위 try-catch
- 실패한 chunk는 skip, 다음 chunk 계속
- Cutoff time은 성공한 chunk까지만 갱신
- 재실행 시 실패한 chunk부터 재처리

### 13.2 Realtime Processing 에러
- Ship별 독립 처리 (에러 전파 차단)
- Timestamp cache에 추가 안 함 (재시도)
- DB verification으로 데이터 유실 방지

### 13.3 Logging & Debugging
```
logs/
├── parallel_batch.log      - 전체 batch 로그
├── realtime.log           - 전체 realtime 로그
├── ship_IMO9976903_batch.log
├── ship_IMO9976903_realtime.log
└── csv_upsert.log         - CSV import 로그
```

---

## 14. 보안 및 권한

### 14.1 Database 권한
```sql
GRANT SELECT ON tenant.tbl_data_timeseries TO migration_user;
GRANT ALL ON tenant.tbl_data_timeseries_* TO migration_user;
GRANT USAGE ON SCHEMA tenant TO migration_user;
```

### 14.2 파일 시스템 권한
```bash
chmod 600 cutoff_times/*.txt  # Cutoff time 파일
chmod 755 *.sh                # 실행 스크립트
```

---

## 15. 테스트 전략

### 15.1 단위 테스트
- `channel_router`: 채널 매핑 정확성
- `multi_table_generator`: 테이블 생성 검증
- `cutoff_time_manager`: 파일 I/O 검증

### 15.2 통합 테스트
- 전체 migration 플로우
- Batch + Realtime 동시 실행
- 데이터 정합성 검증

### 15.3 성능 테스트
- 대용량 데이터 처리 (1M+ rows)
- 병렬 처리 효율성
- 메모리 사용량 모니터링

---

## 16. 버전 히스토리

### Version 1.0.0 (Current)
- ✅ Multi-Table 아키텍처
- ✅ Batch/Realtime 분리
- ✅ Cutoff time 관리 개선
- ✅ Channel Router 기반 분산
- ✅ CSV Migration Upserter
- ✅ processed_timestamps 안전성 강화
- ✅ 로그 간소화
- ✅ 성능 최적화 (고정 lookback, 채널 필터링)

---

## 17. 참고 자료

### 17.1 PostgreSQL 문서
- BRIN Indexes: https://www.postgresql.org/docs/current/brin.html
- ON CONFLICT: https://www.postgresql.org/docs/current/sql-insert.html
- psycopg2 execute_batch: https://www.psycopg.org/docs/extras.html

### 17.2 프로젝트 파일
- `config.py`: 전체 설정
- `README.md`: 사용자 가이드
- `requirements.txt`: 의존성

