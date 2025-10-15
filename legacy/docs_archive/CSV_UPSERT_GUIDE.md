# CSV Migration Data Upsert Guide

`migration_data` 폴더의 CSV 파일들을 3개의 wide 테이블에 upsert하는 가이드

---

## 📋 개요

### 목적
- `migration_data` 폴더에 있는 과거 데이터 CSV 파일들을 wide 테이블로 import
- 3개 테이블 (auxiliary_systems, engine_generator, navigation_ship)로 자동 분산
- UPSERT 방식으로 중복 시 업데이트

### 지원 선박
| 폴더명 | IMO 번호 |
|--------|----------|
| H2546  | IMO9976903 |
| H2547  | IMO9976915 |
| H2548  | IMO9976927 |
| H2549  | IMO9976939 |
| H2559  | IMO9986051 |
| H2560  | IMO9986087 |

---

## 📁 폴더 구조

```
migration_data/
├── H2546/
│   ├── H2546_2025-01.csv
│   ├── H2546_2025-02.csv
│   └── ...
├── H2547/
│   ├── H2547_2025-01.csv
│   └── ...
└── ...
```

### CSV 파일 형식

**파일명**: `{선박코드}_{년도}-{월}.csv`
- 예: `H2546_2025-01.csv`

**CSV 구조**:
```csv
timestamp,/hs4sd_v1/me01/fuel/oil//in_c/temp,/hs4sd_v1/ship////fwd_m/draft,...
2024-12-07 00:00:00,15.015889167785645,7.552000045776367,...
2024-12-07 00:15:00,15.022246360778809,7.553000045776368,...
```

- **첫 번째 컬럼**: `timestamp` (YYYY-MM-DD HH:MM:SS)
- **나머지 컬럼**: 채널 ID (컬럼명 그대로 사용)

---

## 🚀 실행 방법

### 1. 전체 선박 처리

```bash
python upsert_migration_data.py
```

**동작**:
- `migration_data` 폴더의 모든 선박 폴더 스캔
- 각 선박의 모든 CSV 파일 처리
- 3개 테이블에 자동 분산 upsert

### 2. 특정 선박만 처리

```bash
python upsert_migration_data.py --ship H2546
```

**동작**:
- `H2546` (IMO9976903) 선박만 처리

### 3. 다른 폴더 지정

```bash
python upsert_migration_data.py --dir /path/to/data
```

**동작**:
- 기본 `migration_data` 대신 지정된 경로 사용

---

## 📊 처리 과정

### 1단계: CSV 파일 읽기
```
📄 Processing: H2546_2025-01.csv
   📊 Columns: 93 channels
```

### 2단계: 채널 분류
```
   - Table 1 (auxiliary_systems): 35 channels
   - Table 2 (engine_generator): 50 channels
   - Table 3 (navigation_ship): 8 channels
```

### 3단계: 배치 UPSERT
```
   ⏳ Processed 1000 rows...
   ⏳ Processed 2000 rows...
   ✅ Completed: 2500 rows processed
```

### 4단계: 최종 요약
```
📊 UPSERT SUMMARY
📁 Total files found: 12
✅ Successfully processed: 12
❌ Failed: 0
📊 Total rows processed: 30,000
💾 Total rows upserted: 30,000
```

---

## 🔧 UPSERT 로직

### SQL 구조

```sql
INSERT INTO tenant.tbl_data_timeseries_{ship_id}_{table_type} 
    (created_time, "/hs4sd_v1/me01/...", "/hs4sd_v1/ship/...", ...)
VALUES 
    ('2024-12-07 00:00:00', 15.01, 7.55, ...),
    ('2024-12-07 00:15:00', 15.02, 7.56, ...)
ON CONFLICT (created_time) 
DO UPDATE SET
    "/hs4sd_v1/me01/..." = EXCLUDED."/hs4sd_v1/me01/...",
    "/hs4sd_v1/ship/..." = EXCLUDED."/hs4sd_v1/ship/...",
    ...
```

### 특징
- ✅ **UPSERT**: 같은 `created_time`이 있으면 UPDATE
- ✅ **배치 처리**: 1000개씩 묶어서 처리 (성능 최적화)
- ✅ **NULL 처리**: 빈 값이나 변환 실패 시 NULL
- ✅ **트랜잭션**: 배치 단위로 commit

---

## 📝 로그

### 로그 파일
```
logs/csv_upsert.log
```

### 로그 내용 예시

```
2025-10-15 10:00:00 | INFO | 🚀 Starting CSV migration data upsert
2025-10-15 10:00:00 | INFO | 📂 Base directory: /path/to/migration_data

================================================================================
🚢 Processing ship: H2546 → IMO9976903
================================================================================
   📊 Found 3 CSV files
   
   📄 Processing: H2546_2025-01.csv
      📊 Columns: 93 channels
      - Table 1: 35 channels
      - Table 2: 50 channels
      - Table 3: 8 channels
      ⏳ Processed 1000 rows...
      ⏳ Processed 2000 rows...
      ✅ Completed: 2500 rows processed
   
   📄 Processing: H2546_2025-02.csv
      ...

================================================================================
📊 UPSERT SUMMARY
================================================================================
📁 Total files found: 18
✅ Successfully processed: 18
❌ Failed: 0
📊 Total rows processed: 45,000
💾 Total rows upserted: 45,000
```

---

## ⚠️ 주의사항

### 1. 테이블 존재 확인
- UPSERT 전에 테이블이 존재해야 합니다
- Realtime이나 Batch 실행으로 테이블 자동 생성 가능
- 또는 `multi_table_generator.ensure_all_tables_exist()` 호출

### 2. 중복 실행
- 같은 CSV 파일을 여러 번 실행해도 안전 (UPSERT)
- 기존 데이터는 UPDATE됨

### 3. 성능
- 대용량 CSV: 시간이 걸릴 수 있음
- 배치 크기: 1000개 (필요시 코드 수정)

### 4. 채널 매핑
- `channel_router`가 모르는 채널은 자동 제외
- 로그에 warning으로 표시됨

---

## 🔍 문제 해결

### 에러: "Directory not found"
```bash
# migration_data 폴더 생성
mkdir -p migration_data/H2546
```

### 에러: "Unknown ship code"
- `SHIP_MAPPING`에 정의된 선박만 처리 가능
- 새 선박 추가: `upsert_migration_data.py` 수정 필요

### 에러: "Invalid CSV format"
- CSV 첫 번째 컬럼이 `timestamp`인지 확인
- 날짜 형식: `YYYY-MM-DD HH:MM:SS`

### 에러: "Table does not exist"
```bash
# 테이블 생성
python -c "from multi_table_generator import multi_table_generator; multi_table_generator.ensure_all_tables_exist('IMO9976903')"
```

---

## 📈 성능 예상

### 처리 속도
- **1개 파일** (2,500 rows): ~5초
- **1개월 데이터** (5,000 rows): ~10초
- **1년 데이터** (60,000 rows): ~2분

### DB 부하
- UPSERT는 INSERT보다 느림
- 배치 처리로 최적화
- 네트워크 대역폭: 적음 (로컬 처리)

---

## 🎯 활용 시나리오

### 시나리오 1: 초기 데이터 로딩
```bash
# 전체 과거 데이터 import
python upsert_migration_data.py

# Realtime 시작
./start_realtime.sh
```

### 시나리오 2: 특정 월 재처리
```bash
# H2546 선박의 2025-01 데이터만 재처리
python upsert_migration_data.py --ship H2546
```

### 시나리오 3: 데이터 보정
```bash
# CSV 수정 후 재 upsert (기존 데이터 UPDATE됨)
python upsert_migration_data.py --ship H2546
```

---

## 📚 관련 파일

- **스크립트**: `upsert_migration_data.py`
- **로그**: `logs/csv_upsert.log`
- **채널 매핑**: `channel_router.py`
- **테이블 생성**: `multi_table_generator.py`
- **설정**: `config.py`

---

## 🆘 지원

문제 발생 시:
1. 로그 확인: `logs/csv_upsert.log`
2. 데이터 확인: CSV 형식, 경로
3. 테이블 확인: 존재 여부, 스키마

