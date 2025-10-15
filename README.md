# PostgreSQL Narrow-to-Wide Table Migration
Version 1.0.0

PostgreSQL Narrow 테이블을 선박별 Wide 테이블로 변환하는 고성능 마이그레이션 시스템

---

## 🚀 Quick Start

### 1. 설치
```bash
pip install -r requirements.txt
```

### 2. 설정
`config.py`에서 DB 설정 및 대상 선박 확인:
```python
use_multi_table: bool = True  # Multi-Table 모드 (권장)
target_ship_ids = ['IMO9976903', 'IMO9976915', ...]
```

### 3. 실행

#### 병렬 배치 마이그레이션 (과거 데이터)
```bash
./start_parallel_batch.sh
```

#### 실시간 처리 (신규 데이터)
```bash
./start_realtime.sh
```

---

## 📋 시스템 구성

### Multi-Table 모드 (권장)
선박당 **3개 테이블**로 분산 저장:

| 테이블 | 설명 | 채널 수 | 예시 |
|--------|------|---------|------|
| Table 1 | 보조 시스템 (Cargo, Boiler 등) | 347 | `tbl_data_timeseries_imo9976903_1` |
| Table 2 | 엔진/발전기 | 650 | `tbl_data_timeseries_imo9976903_2` |
| Table 3 | 항해/선박정보 | 40 | `tbl_data_timeseries_imo9976903_3` |

**장점**:
- ✅ 쿼리 성능 향상 (필요한 테이블만 조회)
- ✅ 테이블 크기 감소
- ✅ 병렬 처리 최적화

---

## 🎯 사용 가이드

### Batch Migration (과거 데이터 처리)

#### 시작
```bash
./start_parallel_batch.sh
```

#### 로그 확인
```bash
# 전체 batch 로그
tail -f logs/parallel_batch.log

# 선박별 로그
tail -f logs/ship_IMO9976903_batch.log
```

#### 중지
```bash
./stop_parallel_batch.sh
```

#### 재시작
- Cutoff time에서 자동으로 재개
- 중복 처리 없음

**처리 과정**:
```
1. 과거 1년치 데이터 (기본값)
2. 2시간 단위 chunk로 분할
3. 선박별 병렬 처리 (8 threads)
4. 3개 테이블에 분산 저장
5. Progress 및 ETA 표시
```

---

### Realtime Processing (실시간 데이터)

#### 시작
```bash
./start_realtime.sh
```

#### 로그 확인
```bash
# 전체 realtime 로그
tail -f logs/realtime.log

# 선박별 로그
tail -f logs/ship_IMO9976903_realtime.log
```

#### 중지
```bash
./stop_realtime.sh
```

**처리 과정**:
```
1. 1분 간격 실행
2. Cutoff time 이후 신규 데이터 조회
3. 채널별로 3개 테이블에 분산
4. UPSERT 방식 (중복 방지)
5. Cutoff time 자동 갱신
```

---

### CSV Data Upsert (보정 데이터)

#### 폴더 구조
```
migration_data/
├── H2546/  (→ IMO9976903)
│   ├── H2546_2024-12.csv
│   ├── H2546_2025-01.csv
│   └── ...
├── H2547/  (→ IMO9976915)
└── ...
```

#### CSV 형식
```csv
timestamp,/hs4sd_v1/ch1,/hs4sd_v1/ch2,...
2024-12-07 00:00:00,15.01,7.55,...
2024-12-07 00:15:00,15.02,7.56,...
```

#### 실행

**1단계: Dry-run (필수!)**
```bash
python upsert_migration_data.py --dry-run
```

**확인 사항**:
- ✅ 파일 읽기 성공
- ✅ 채널 매칭 확인
- ✅ Coverage 확인

**2단계: 특정 선박 테스트**
```bash
python upsert_migration_data.py --ship H2546
```

**3단계: 전체 실행**
```bash
python upsert_migration_data.py
```

**로그**:
```bash
tail -f logs/csv_upsert.log
```

**선박 매핑**:
| 폴더명 | IMO 번호 |
|--------|----------|
| H2546  | IMO9976903 |
| H2547  | IMO9976915 |
| H2548  | IMO9976927 |
| H2549  | IMO9976939 |
| H2559  | IMO9986051 |
| H2560  | IMO9986087 |

---

## ⚙️ 설정

### config.py 주요 설정

```python
# Multi-Table 모드
use_multi_table: bool = True

# 대상 선박
target_ship_ids = [
    'IMO9976903', 'IMO9976915', 'IMO9976927', 'IMO9976939',
    'IMO9986051', 'IMO9986063', 'IMO9986087', 'IMO9986104'
]

# Batch 설정
chunk_size_hours: int = 2           # Chunk 크기 (2시간)
batch_size: int = 50000             # INSERT 배치 크기
batch_lookback_days: int = 365      # 과거 1년치 처리

# Thread 설정 (자동 최적화)
parallel_workers: int = 8           # 선박별 병렬 처리
max_parallel_workers: int = 16

# DB Connection Pool (자동 계산)
# Multi-table: threads × 3 = 24
# Single-table: threads × 2 = 16

# Database
host: str = "DB_HOST"
port: int = 5432
    database: str = "tenant_builder"
user: str = "DB_USER"
password: str = "DB_PASSWORD"
```

---

## 📊 모니터링

### 진행 상황 확인

#### Batch Migration
```bash
# 실시간 로그
tail -f logs/parallel_batch.log | grep "Chunk"

# 예시 출력:
# 🔄 Chunk 1000/4380 (22.8%)
# 📅 Date range: 2024-10-05 12:00 to 2024-10-05 14:00
# ⏱️ Speed: 2.5s/chunk, Avg: 3.2s/chunk
# 📊 ETA: 180.5 minutes (3380 chunks remaining)
# 📊 Total: 1,234,567 narrow → 450,000 wide records
```

#### Realtime Processing
```bash
tail -f logs/realtime.log | grep "IMO9976903"

# 예시 출력:
# 🚢 Starting processing for ship: IMO9976903
# 📊 New records found: 3,023
# 🔍 Processing 3,023 records → 3 tables
# 📊 Prepared 3 rows: T1=1, T2=1, T3=1
# 💾 Inserted: T1:1, T2:1, T3:1
# ✅ Completed processing in 0.35s
```

### 선박별 로그
```bash
# Batch
tail -f logs/ship_IMO9976903_batch.log

# Realtime
tail -f logs/ship_IMO9976903_realtime.log
```

### Cutoff Time 확인
```bash
cat cutoff_times/imo9976903_batch.txt
cat cutoff_times/imo9976903_realtime.txt
```

---

## 🔧 문제 해결

### Batch가 멈춘 것 같아요
```bash
# 진행 상황 확인
tail -100 logs/parallel_batch.log | grep "Chunk"

# Cutoff time 확인
cat cutoff_times/imo9976903_batch.txt

# 특정 선박 로그 확인
tail -100 logs/ship_IMO9976903_batch.log
```

### Realtime에서 데이터가 안 들어가요
```bash
# Realtime 로그 확인
tail -100 logs/ship_IMO9976903_realtime.log | grep -E "Prepared|Inserted"

# 예상 출력:
# 📊 Prepared X rows: T1=Y, T2=Z, T3=W
# 💾 Inserted: T1:Y, T2:Z, T3:W

# 0 rows라면:
# - 채널 매칭 문제 확인
# - DB에 데이터 있는지 확인
```

### Coverage가 0% 또는 너무 낮아요 (CSV Upsert)
```bash
# Dry-run으로 확인
python upsert_migration_data.py --dry-run --ship H2546

# 예상 출력:
# - Table 1: 9/347 channels (2.6% coverage)
# - Table 2: 67/650 channels (10.3% coverage)
# - Table 3: 14/40 channels (35.0% coverage)

# 0%라면:
# - CSV 헤더 형식 확인
# - 채널 ID 공백 확인
# - channel_list 파일 확인
```

### 테이블이 존재하지 않아요
```bash
# Batch 또는 Realtime을 먼저 실행하면 테이블 자동 생성
./start_parallel_batch.sh

# 또는 수동 생성
python -c "
from multi_table_generator import multi_table_generator
multi_table_generator.ensure_all_tables_exist('IMO9976903')
"
```

### DB Connection 에러
```python
# config.py 확인
host: str = "올바른_호스트"
port: int = 5432
database: str = "올바른_DB"
user: str = "올바른_사용자"
password: str = "올바른_비밀번호"
```

---

## 📁 프로젝트 구조

```
postgres-narrow-to-wide-migrator/
├── config.py                        # ⚙️ 설정 파일
├── main.py                          # 🎯 메인 실행 스크립트
│
├── # 핵심 모듈
├── database.py                      # DB 연결 및 Connection Pool
├── channel_router.py                # 채널 → 테이블 매핑
├── multi_table_generator.py         # 테이블 생성
├── multi_table_chunked_strategy.py  # Chunk 기반 마이그레이션
├── parallel_batch_migrator.py       # 병렬 Batch 처리
├── realtime_processor.py            # 실시간 처리
├── cutoff_time_manager.py           # Cutoff time 관리
├── upsert_migration_data.py         # CSV 데이터 import
│
├── # 유틸리티
├── thread_logger.py                 # 선박별 로그
├── monitoring.py                    # 모니터링
├── simple_log_rotation.py           # 로그 로테이션
│
├── # 채널 정의
├── column_list_auxiliary_systems.txt    # Table 1 채널 (347)
├── column_list_engine_generator.txt     # Table 2 채널 (650)
├── column_list_navigation_ship.txt      # Table 3 채널 (40)
│
├── # 실행 스크립트
├── start_parallel_batch.sh          # Batch 시작
├── stop_parallel_batch.sh           # Batch 중지
├── start_realtime.sh                # Realtime 시작
├── stop_realtime.sh                 # Realtime 중지
│
├── # 로그 및 데이터
├── logs/                            # 로그 파일들
│   ├── parallel_batch.log
│   ├── realtime.log
│   ├── ship_IMO9976903_batch.log
│   ├── ship_IMO9976903_realtime.log
│   └── csv_upsert.log
├── cutoff_times/                    # Cutoff time 파일
│   ├── imo9976903_batch.txt
│   └── imo9976903_realtime.txt
├── migration_data/                  # CSV import 데이터
│
├── # 문서
├── README.md                        # 📖 사용자 가이드 (이 파일)
└── DESIGN.md                        # 🏗️ 기술 설계서
```

---

## 📖 상세 사용법

### Batch Migration (병렬 배치)

**목적**: 과거 데이터를 Narrow → Wide 테이블로 마이그레이션

#### 시작
```bash
./start_parallel_batch.sh
```

**내부 동작**:
1. 8개 선박 병렬 처리 (ThreadPool)
2. 선박별로 과거 1년치 데이터 처리 (기본값)
3. 2시간 단위 chunk로 분할
4. 3개 테이블에 분산 저장
5. Chunk 완료 시마다 cutoff_time 저장

#### 진행 상황
```bash
# 전체 로그
tail -f logs/parallel_batch.log

# 특정 선박
tail -f logs/ship_IMO9976903_batch.log | grep "Chunk\|ETA"

# 예시 출력:
# [IMO9976903] 🔄 Chunk 1000/4380 (22.8%)
# [IMO9976903] 📊 ETA: 180.5 minutes
# [IMO9976903] 📊 Total: 1,234,567 narrow → 450,000 wide records
```

#### 중지
```bash
./stop_parallel_batch.sh
```

#### 재시작
```bash
# Cutoff time에서 자동 재개
./start_parallel_batch.sh
```

**Cutoff time 확인**:
```bash
cat cutoff_times/imo9976903_batch.txt
# 2025-10-05 14:00:00
```

---

### Realtime Processing

**목적**: 실시간으로 생성되는 데이터를 Wide 테이블로 처리

#### 시작
```bash
./start_realtime.sh
```

**내부 동작**:
1. 1분 간격으로 실행
2. Cutoff time 이후 신규 데이터 조회
3. 3개 테이블에 분산 저장
4. UPSERT (중복 방지)
5. Cutoff time 갱신

#### 로그 확인
```bash
# 전체 로그
tail -f logs/realtime.log

# 특정 선박
tail -f logs/ship_IMO9976903_realtime.log

# 예시 출력:
# 🚢 Starting processing for ship: IMO9976903
# 📊 New records found: 3,023
# 🔍 Processing 3,023 records → 3 tables
# 💾 Inserted: T1:1, T2:1, T3:1
# ✅ Completed in 0.35s
```

#### 중지
```bash
./stop_realtime.sh
```

---

### CSV Data Import (보정 데이터)

**목적**: 과거 CSV 파일의 보정 데이터를 Wide 테이블에 UPSERT

#### 준비
1. `migration_data` 폴더에 CSV 파일 배치:
   ```
   migration_data/H2546/H2546_2025-01.csv
   ```

2. CSV 형식 확인:
   ```csv
   timestamp,/hs4sd_v1/channel1,/hs4sd_v1/channel2,...
   2025-01-01 00:00:00,value1,value2,...
   ```

#### 실행

**Step 1: Dry-run (필수!)**
```bash
python upsert_migration_data.py --dry-run
```

**확인**:
```
📊 Columns: 90 channels
- Table 1: 9/347 channels (2.6% coverage)
- Table 2: 67/650 channels (10.3% coverage)
- Table 3: 14/40 channels (35.0% coverage)

🔍 [DRY-RUN] Would upsert 143,697 rows...
```

**Step 2: 특정 선박 테스트**
```bash
python upsert_migration_data.py --ship H2546
```

**Step 3: 전체 실행**
```bash
python upsert_migration_data.py
```

#### 로그 확인
```bash
tail -f logs/csv_upsert.log

# Summary 예시:
# 📊 CSV Rows: 6,834,697
# 💾 DB Rows Upserted:
#    Table 1: 6,500,000
#    Table 2: 6,800,000
#    Table 3: 6,372,712
#    Total: 19,672,712
```

**Note**:
- 1 CSV row → 최대 3 DB rows (테이블별)
- 기존 row: CSV 컬럼만 업데이트, 나머지 유지
- 신규 row: CSV 컬럼 채움, 나머지 NULL

---

## 🔄 운영 시나리오

### 시나리오 1: 초기 셋업 (처음 시작)

```bash
# 1. Batch로 과거 데이터 처리 (시간 오래 걸림)
./start_parallel_batch.sh

# 2. Batch 진행 중 Realtime 시작 (동시 실행)
./start_realtime.sh

# 3. Batch 완료 후 CSV 보정 데이터 import
python upsert_migration_data.py --dry-run
python upsert_migration_data.py
```

### 시나리오 2: 정상 운영 (Realtime만)

```bash
# Realtime만 실행 (Batch는 완료됨)
./start_realtime.sh

# 모니터링
tail -f logs/realtime.log
```

### 시나리오 3: 재처리 (Batch 재시작)

```bash
# Batch 재시작 (Cutoff time에서 자동 재개)
./start_parallel_batch.sh

# 로그 확인
tail -f logs/parallel_batch.log | grep "Cutoff\|Chunk"
```

### 시나리오 4: 특정 기간 재처리

```bash
# Cutoff time 파일 수정
echo "2024-01-01 00:00:00" > cutoff_times/imo9976903_batch.txt

# Batch 재시작
./start_parallel_batch.sh
```

---

## ⚠️ 중요 사항

### 1. Batch와 Realtime 동시 실행
- ✅ **안전**: Cutoff time 기반 명확한 경계
- ✅ Batch는 `realtime_cutoff_time` 전까지만 처리
- ✅ Realtime은 `realtime_cutoff_time` 이후부터 처리
- ✅ 중복/누락 없음

### 2. CSV Upsert 주의사항
- ⚠️ **Batch 완료 후** 실행 권장
- ⚠️ Dry-run으로 먼저 테스트
- ⚠️ Coverage 확인 (특히 < 10%)
- ✅ 중복 실행 안전 (UPSERT)

### 3. 성능 고려사항
- Thread 수: 선박 수와 동일 (1:1 매핑)
- DB Pool: Multi-table 시 threads × 3
- Chunk 크기: 2시간 (메모리 효율)
- Batch 크기: 50K rows (INSERT 효율)

### 4. 로그 관리
- 로그 로테이션: 자동 (100MB 단위)
- 보관 기간: 30일
- 선박별 로그: 모드별 분리 (batch/realtime)

---

## 🆘 지원

### 문제 발생 시

1. **로그 확인**:
   ```bash
   tail -200 logs/parallel_batch.log
   tail -200 logs/realtime.log
   tail -200 logs/csv_upsert.log
   ```

2. **에러 검색**:
   ```bash
   grep ERROR logs/*.log
   grep WARNING logs/*.log
   ```

3. **프로세스 확인**:
   ```bash
   ps aux | grep python
   ps aux | grep realtime
   ps aux | grep batch
   ```

4. **재시작**:
   ```bash
   ./stop_realtime.sh && ./start_realtime.sh
   ./stop_parallel_batch.sh && ./start_parallel_batch.sh
   ```

### 데이터 검증

```sql
-- 테이블 존재 확인
SELECT tablename 
FROM pg_tables 
WHERE schemaname = 'tenant' 
  AND tablename LIKE 'tbl_data_timeseries_imo%';

-- 데이터 개수 확인
SELECT COUNT(*) 
FROM tenant.tbl_data_timeseries_imo9976903_1;

-- 최신 데이터 확인
SELECT created_time, COUNT(*) 
FROM tenant.tbl_data_timeseries_imo9976903_1 
WHERE created_time >= NOW() - INTERVAL '1 hour'
GROUP BY created_time 
ORDER BY created_time DESC 
LIMIT 10;

-- NULL 비율 확인 (낮아야 정상)
SELECT 
    COUNT(*) as total,
    COUNT("/hs4sd_v1/me01/fuel/oil//in_c/temp") as non_null,
    ROUND(100.0 * COUNT("/hs4sd_v1/me01/fuel/oil//in_c/temp") / COUNT(*), 2) as fill_rate
FROM tenant.tbl_data_timeseries_imo9976903_2;
```

---

## 📚 추가 문서

- **DESIGN.md**: 상세 기술 설계서 (아키텍처, 성능, 최적화)
- **requirements.txt**: Python 의존성
- **config.py**: 전체 설정 옵션

---

## 📊 성능 지표

### Batch Migration
- **처리 속도**: ~3s/chunk (2시간 데이터)
- **예상 시간**: 8개 선박 × 4,380 chunks = ~3.6시간
- **메모리**: <500MB per thread

### Realtime Processing
- **처리 속도**: <1s (1분 데이터, ~3K rows)
- **지연 시간**: <1분
- **CPU**: Low (<10%)

### CSV Upsert
- **처리 속도**: ~10s/1K rows
- **예상 시간**: 143K rows = ~25분/파일

---

## 🎯 권장 사항

### 초기 셋업
1. ✅ config.py 설정 확인
2. ✅ Batch migration 시작 (과거 데이터)
3. ✅ Realtime 시작 (동시 실행 가능)
4. ✅ Batch 완료 확인
5. ✅ CSV 보정 데이터 import (필요시)

### 정상 운영
1. ✅ Realtime만 실행
2. ✅ 로그 모니터링
3. ✅ Cutoff time 확인

### 문제 해결
1. ✅ 로그 확인 (선박별, 모드별)
2. ✅ 재시작 (cutoff time에서 재개)
3. ✅ DB 데이터 검증

---

## 📞 문의

로그 파일 및 에러 메시지와 함께 문의하세요.

## 라이선스

MIT License
