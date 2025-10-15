# Multi-Table Migration Guide

## 🎯 개요

선박당 하나의 대형 테이블 대신 **3개의 시스템별 테이블**로 데이터를 분산 저장하는 Multi-Table 모드입니다.

### 기존 vs 신규

| 구분 | 기존 (Single-Table) | 신규 (Multi-Table) |
|------|---------------------|---------------------|
| 테이블 수 | 선박당 1개 | 선박당 3개 |
| 테이블명 | `tbl_data_timeseries_{ship_id}` | `auxiliary_systems_{ship_id}`<br>`engine_generator_{ship_id}`<br>`navigation_ship_{ship_id}` |
| 총 컬럼 수 | 1,037개 | 348개 + 651개 + 41개 |
| 쿼리 성능 | 전체 스캔 필요 | 필요한 테이블만 스캔 |

---

## 📊 테이블 구성

### 1. auxiliary_systems_{ship_id} (보조 시스템)
- **채널 수:** 347개
- **주요 시스템:**
  - 카고 탱크 (ct01~ct04): 207개
  - APRS (압축 릴리퀘팩션): 25개
  - 보일러 (ab01~ab02): 20개
  - 연료 가스 압축기 (fgc01~fgc02): 22개

### 2. engine_generator_{ship_id} (엔진/발전기)
- **채널 수:** 650개
- **주요 시스템:**
  - 주기관 (me01~me02): 298개
  - 발전기 (ge01~ge04): 342개

### 3. navigation_ship_{ship_id} (항해/선박정보)
- **채널 수:** 40개
- **주요 시스템:**
  - VAP (Vapour 시스템): 18개
  - VDR (Voyage Data Recorder): 17개
  - Ship (선박 정보): 5개

---

## 🚀 사용법

### Multi-Table 모드 활성화

```python
# config.py
use_multi_table: bool = True  # Multi-Table 모드 활성화
```

### 병렬 배치 마이그레이션 실행

```bash
# Multi-Table 모드로 병렬 배치 마이그레이션
./start_parallel_batch.sh

# 또는 직접 실행
python main.py --mode parallel-batch

# Cutoff time 지정
python main.py --mode parallel-batch --cutoff-time "2024-10-01 00:00:00"
```

### 로그 확인

```bash
# 병렬 배치 로그 확인
tail -f logs/parallel_batch.log

# 또는
./view_logs.sh -f parallel_batch
```

---

## ⚙️ 설정

### config.py 주요 설정

```python
# Multi-Table 모드
use_multi_table: bool = True

# 청크 크기 (2시간)
chunk_size_hours: int = 2

# 배치 크기 (50,000개/배치)
batch_size: int = 50000

# Thread 설정
parallel_workers: int = 8  # 8개 선박 동시 처리
max_parallel_workers: int = 16

# DB Pool 설정 (자동 계산)
# Multi-table mode: maxconn = threads * 3 (24개)
# Single-table mode: maxconn = threads * 2 (16개)
```

### Thread 및 DB Pool 최적화

| 선박 수 | Threads | DB Pool (Multi) | DB Pool (Single) |
|---------|---------|-----------------|------------------|
| 4개 | 4 | 12 | 8 |
| 8개 | 8 | 24 | 16 |
| 12개 | 9 | 27 | 18 |
| 16개 | 16 | 48 | 32 |

**Multi-Table 모드에서는 각 thread가 3개 테이블에 순차적으로 INSERT하므로 DB Pool을 3배로 설정**

---

## 📈 성능 비교

### 쿼리 성능

```sql
-- Before: 1,037개 컬럼 테이블 스캔
SELECT created_time, hs4sd_v1_me01_rpm_speed
FROM tbl_data_timeseries_imo9976903
WHERE created_time BETWEEN '2024-01-01' AND '2024-01-02';
-- 스캔: 1,037 컬럼

-- After: 651개 컬럼 테이블 스캔 (37% 감소)
SELECT created_time, hs4sd_v1_me01_rpm_speed
FROM engine_generator_imo9976903
WHERE created_time BETWEEN '2024-01-01' AND '2024-01-02';
-- 스캔: 651 컬럼
```

### 마이그레이션 성능

| 항목 | Single-Table | Multi-Table |
|------|--------------|-------------|
| Thread당 INSERT 횟수 | 1회 | 3회 (순차) |
| DB 연결 사용 | thread * 2 | thread * 3 |
| 테이블 락 경합 | 높음 | 낮음 (분산) |
| 전체 처리 시간 | 기준 | 약 10-15% 증가 |

**Note:** 마이그레이션 시간은 약간 증가하지만, 쿼리 성능과 관리 효율성이 크게 향상됩니다.

---

## 🔧 트러블슈팅

### 1. 테이블이 생성되지 않음

```bash
# 채널 파일 확인
ls -lh column_list_*.txt

# 수동으로 테이블 생성 테스트
python3 multi_table_generator.py --create
```

### 2. 채널 라우팅 오류

```bash
# 채널 라우터 테스트
python3 channel_router.py
```

### 3. DB Pool 부족

```python
# config.py에서 maxconn 증가
# 또는 thread 수 감소
parallel_workers: int = 4  # 8 → 4로 감소
```

---

## 📚 관련 문서

- `docs/MULTI_TABLE_DESIGN.md` - 상세 설계서
- `IMPLEMENTATION_STATUS.md` - 구현 현황 및 성능 분석
- `MULTI_TABLE_MIGRATION_SUMMARY.md` - 요약 문서

---

## 🔄 마이그레이션 전환

### Legacy → Multi-Table 전환 절차

1. **백업 생성**
   ```bash
   pg_dump -h 20.249.68.82 -U tapp -d tenant_builder -t 'tenant.tbl_data_timeseries_*' > backup.sql
   ```

2. **Multi-Table 모드 활성화**
   ```python
   # config.py
   use_multi_table: bool = True
   ```

3. **테스트 실행**
   ```bash
   # 단일 선박으로 테스트
   python main.py --mode parallel-batch --ship-id IMO9976903
   ```

4. **전체 마이그레이션**
   ```bash
   ./start_parallel_batch.sh
   ```

5. **검증**
   ```bash
   # 레코드 수 비교
   python -c "
   from database import db_manager
   # 각 테이블 레코드 수 확인
   "
   ```

---

## ⚠️ 주의사항

1. **데이터 백업 필수**
   - Multi-Table로 전환 전 반드시 백업

2. **Cutoff Time 관리**
   - 기존 cutoff time이 유지되는지 확인

3. **실시간 처리**
   - realtime_processor는 아직 Multi-Table 미지원
   - 배치 모드만 사용 권장

4. **디스크 공간**
   - 3개 테이블로 분산되므로 약간의 추가 공간 필요

---

## 💡 FAQ

**Q: Single-Table과 Multi-Table을 동시에 사용할 수 있나요?**
A: 네, `use_multi_table` 설정으로 전환 가능합니다.

**Q: 기존 데이터는 어떻게 되나요?**
A: 기존 single-table 데이터는 그대로 유지되며, 새로 multi-table로 마이그레이션됩니다.

**Q: 성능은 얼마나 향상되나요?**
A: 쿼리 성능은 30-60% 향상, 마이그레이션 시간은 약 10-15% 증가합니다.

**Q: Realtime 처리는 지원되나요?**
A: 현재는 배치 모드만 완전 지원되며, realtime은 추후 업데이트 예정입니다.

