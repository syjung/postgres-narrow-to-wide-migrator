# Multi-Table Migration Design Document

## 1. 개요

### 1.1 목적
기존 단일 wide 테이블 구조를 시스템 특성별로 3개의 테이블로 분리하여 성능 최적화 및 관리 효율성 향상

### 1.2 변경 범위
- **기존:** 선박당 1개 테이블 (`tbl_data_timeseries_{ship_id}`)
- **신규:** 선박당 3개 테이블 (시스템별 분리)

---

## 2. 테이블 구조 설계

### 2.1 테이블 명명 규칙

| 구분 | 테이블명 패턴 | 예시 |
|------|--------------|------|
| 보조 시스템 | `auxiliary_systems_{ship_id}` | `auxiliary_systems_imo9976903` |
| 엔진/발전기 | `engine_generator_{ship_id}` | `engine_generator_imo9976903` |
| 항해/선박정보 | `navigation_ship_{ship_id}` | `navigation_ship_imo9976903` |

### 2.2 채널 분류 및 컬럼 수

#### 2.2.1 보조 시스템 테이블 (Auxiliary Systems)
- **파일:** `column_list_auxiliary_systems.txt`
- **채널 수:** 347개
- **총 컬럼 수:** 348개 (created_time + 347개 채널)
- **주요 시스템:**
  - 카고 탱크 (ct01~ct04): 207개
  - APRS (압축 릴리퀘팩션): 25개
  - 보일러 (ab01~ab02): 20개
  - 연료 가스 압축기 (fgc01~fgc02): 22개
  - 기타: bwts, cd, cg_mach, dfge, fgp, fv, gc, gcu 등

#### 2.2.2 엔진/발전기 테이블 (Engine Generator)
- **파일:** `column_list_engine_generator.txt`
- **채널 수:** 650개
- **총 컬럼 수:** 651개 (created_time + 650개 채널)
- **주요 시스템:**
  - 주기관 (me01~me02): 298개
  - 발전기 (ge01~ge04): 342개
  - 가스 메인 시스템 (gms): 2개
  - 발전기 라인 (ge_lin01~ge_lin02): 16개

#### 2.2.3 항해/선박정보 테이블 (Navigation Ship)
- **파일:** `column_list_navigation_ship.txt`
- **채널 수:** 40개
- **총 컬럼 수:** 41개 (created_time + 40개 채널)
- **주요 시스템:**
  - VAP (Vapour 시스템): 18개
  - VDR (Voyage Data Recorder): 17개
  - Ship (선박 기본정보): 5개

### 2.3 테이블 스키마

```sql
-- 1. Auxiliary Systems Table
CREATE TABLE IF NOT EXISTS tenant.auxiliary_systems_{ship_id} (
    created_time TIMESTAMP NOT NULL,
    -- 347 data channels from column_list_auxiliary_systems.txt
    hs4sd_v1_ab_fuel_oil_use DOUBLE PRECISION,
    hs4sd_v1_ab_fuel_oil_in_c_temp DOUBLE PRECISION,
    hs4sd_v1_ab_fuel_oil_kgph_flow DOUBLE PRECISION,
    -- ... (347 columns total)
);

-- 2. Engine Generator Table
CREATE TABLE IF NOT EXISTS tenant.engine_generator_{ship_id} (
    created_time TIMESTAMP NOT NULL,
    -- 650 data channels from column_list_engine_generator.txt
    hs4sd_v1_ge_gas_in_bar_press DOUBLE PRECISION,
    hs4sd_v1_ge_gas_in_s_bar_press DOUBLE PRECISION,
    -- ... (650 columns total)
);

-- 3. Navigation Ship Table
CREATE TABLE IF NOT EXISTS tenant.navigation_ship_{ship_id} (
    created_time TIMESTAMP NOT NULL,
    -- 40 data channels from column_list_navigation_ship.txt
    hs4sd_v1_ship_aft_m_draft DOUBLE PRECISION,
    hs4sd_v1_ship_fwd_m_draft DOUBLE PRECISION,
    hs4sd_v1_ship_m_trim DOUBLE PRECISION,
    -- ... (40 columns total)
);
```

---

## 3. 인덱스 설계

### 3.1 BRIN 인덱스 적용

각 테이블에 `created_time`을 기준으로 BRIN 인덱스 생성

```sql
-- Auxiliary Systems 인덱스
CREATE INDEX IF NOT EXISTS idx_auxiliary_systems_{ship_id}_created_time 
ON tenant.auxiliary_systems_{ship_id} 
USING BRIN (created_time) 
WITH (pages_per_range = 128);

-- Engine Generator 인덱스
CREATE INDEX IF NOT EXISTS idx_engine_generator_{ship_id}_created_time 
ON tenant.engine_generator_{ship_id} 
USING BRIN (created_time) 
WITH (pages_per_range = 128);

-- Navigation Ship 인덱스
CREATE INDEX IF NOT EXISTS idx_navigation_ship_{ship_id}_created_time 
ON tenant.navigation_ship_{ship_id} 
USING BRIN (created_time) 
WITH (pages_per_range = 128);
```

### 3.2 인덱스 특성

- **타입:** BRIN (Block Range Index)
- **적용 컬럼:** `created_time`
- **pages_per_range:** 128
- **장점:**
  - 시계열 데이터에 최적화
  - 인덱스 크기 최소화 (일반 B-tree 대비 ~1/1000)
  - 순차적 INSERT 성능 우수

---

## 4. 데이터 마이그레이션 전략

### 4.1 마이그레이션 흐름

```
┌─────────────────────────────────────────────────────────────┐
│  Source: tbl_data_narrow (Narrow Format)                    │
│  - ship_id, created_time, data_channel_id, data_value       │
└─────────────────────────────────────────────────────────────┘
                            ↓
                    ┌───────────────┐
                    │  Transform     │
                    │  (Narrow→Wide) │
                    └───────────────┘
                            ↓
        ┌───────────────────┴───────────────────┐
        ↓                   ↓                   ↓
┌───────────────┐  ┌───────────────┐  ┌───────────────┐
│ auxiliary_    │  │ engine_       │  │ navigation_   │
│ systems_      │  │ generator_    │  │ ship_         │
│ {ship_id}     │  │ {ship_id}     │  │ {ship_id}     │
│               │  │               │  │               │
│ 347 channels  │  │ 650 channels  │  │ 40 channels   │
└───────────────┘  └───────────────┘  └───────────────┘
```

### 4.2 채널 라우팅 로직

```python
def route_channel_to_table(channel_id: str) -> str:
    """
    채널 ID를 기준으로 어느 테이블에 저장할지 결정
    
    Returns:
        'auxiliary_systems' | 'engine_generator' | 'navigation_ship'
    """
    if channel_id in auxiliary_channels:
        return 'auxiliary_systems'
    elif channel_id in engine_generator_channels:
        return 'engine_generator'
    elif channel_id in navigation_ship_channels:
        return 'navigation_ship'
    else:
        raise ValueError(f"Unknown channel: {channel_id}")
```

### 4.3 배치 처리 전략

#### 4.3.1 시간 기반 청크 처리
- **청크 크기:** 2시간 단위 (`chunk_size_hours = 2`)
- 각 청크를 3개 테이블로 분산 저장
- 적응형 청크 크기 조정 가능 (`adaptive_chunking = True`)

#### 4.3.2 병렬 처리
```
Time Chunk: 2024-01-01 00:00 ~ 02:00 (2시간)
    ↓
┌─────────────────────────────────────┐
│  Extract from tbl_data_narrow       │
└─────────────────────────────────────┘
    ↓
┌─────────────────────────────────────┐
│  Transform: Group by timestamp      │
└─────────────────────────────────────┘
    ↓
┌─────────────┬─────────────┬─────────────┐
│ Filter for  │ Filter for  │ Filter for  │
│ Auxiliary   │ Engine/Gen  │ Navigation  │
│ Channels    │ Channels    │ Channels    │
└─────────────┴─────────────┴─────────────┘
    ↓               ↓               ↓
┌─────────────┬─────────────┬─────────────┐
│ INSERT INTO │ INSERT INTO │ INSERT INTO │
│ auxiliary_  │ engine_gen_ │ navigation_ │
│ systems     │ erator      │ ship        │
│ (50K/batch) │ (50K/batch) │ (50K/batch) │
└─────────────┴─────────────┴─────────────┘
```

#### 4.3.3 INSERT 방식

**현재 구현 (executemany):**
```python
# database.py - execute_batch()
cursor.executemany(insert_sql, data)  # 50,000개/배치
conn.commit()
```

**특징:**
- ✅ ON CONFLICT DO UPDATE 지원 (upsert)
- ✅ 구현 단순, 안정적
- ✅ 배치 크기: 50,000개/배치
- ⚠️ 성능: 중상위 (COPY 대비 3-10배 느림)

**향후 최적화 옵션 (COPY - 선택적):**
```python
# 고성능이 필요한 경우
cursor.copy_expert(copy_sql, csv_buffer)  # 더 빠름
```

**장단점 비교:**

| 방식 | 성능 | 중복처리 | 구현 난이도 | 권장 시나리오 |
|------|------|----------|-------------|---------------|
| executemany | 중상 | ✅ | 쉬움 | 일반 마이그레이션 |
| COPY | 최고 | ❌ | 중간 | 대용량 초기 로드 |

**Phase 1에서는 executemany 사용 (현재 방식 유지)**

---

## 5. 구현 계획

### 5.1 수정 대상 모듈

#### 5.1.1 `table_generator.py`
- **현재:** 단일 테이블 생성 (`tbl_data_timeseries_{ship_id}`)
- **변경:** 3개 테이블 생성 함수 추가
  - `create_auxiliary_systems_table(ship_id)`
  - `create_engine_generator_table(ship_id)`
  - `create_navigation_ship_table(ship_id)`

#### 5.1.2 `chunked_migration_strategy.py`
- **현재:** 단일 테이블로 INSERT
- **변경:** 채널별 라우팅 후 3개 테이블로 분산 INSERT
  - 채널 필터링 로직 추가
  - 테이블별 wide 포맷 변환

#### 5.1.3 `realtime_processor.py`
- **현재:** 단일 테이블로 실시간 INSERT
- **변경:** 실시간 데이터를 3개 테이블로 분산

#### 5.1.4 `config.py`
- 새로운 설정 추가:
  ```python
  # 채널 그룹 파일 경로
  AUXILIARY_CHANNELS_FILE = "column_list_auxiliary_systems.txt"
  ENGINE_GENERATOR_CHANNELS_FILE = "column_list_engine_generator.txt"
  NAVIGATION_SHIP_CHANNELS_FILE = "column_list_navigation_ship.txt"
  
  # 테이블명 패턴
  TABLE_NAME_PATTERNS = {
      'auxiliary': 'auxiliary_systems_{ship_id}',
      'engine': 'engine_generator_{ship_id}',
      'navigation': 'navigation_ship_{ship_id}'
  }
  ```

### 5.2 신규 모듈

#### 5.2.1 `channel_router.py`
```python
"""
채널 ID를 기반으로 적절한 테이블로 라우팅하는 모듈
"""

class ChannelRouter:
    def __init__(self):
        self.auxiliary_channels = set()
        self.engine_generator_channels = set()
        self.navigation_ship_channels = set()
        self._load_channel_definitions()
    
    def get_table_type(self, channel_id: str) -> str:
        """채널이 속한 테이블 타입 반환"""
        pass
    
    def filter_channels_by_table(
        self, 
        channels: List[str], 
        table_type: str
    ) -> List[str]:
        """특정 테이블에 속한 채널만 필터링"""
        pass
```

#### 5.2.2 `multi_table_generator.py`
```python
"""
3개 테이블 생성 및 관리 모듈
"""

class MultiTableGenerator:
    def ensure_all_tables_exist(self, ship_id: str) -> bool:
        """선박의 3개 테이블 모두 생성 확인"""
        pass
    
    def create_auxiliary_systems_table(self, ship_id: str):
        """보조 시스템 테이블 생성"""
        pass
    
    def create_engine_generator_table(self, ship_id: str):
        """엔진/발전기 테이블 생성"""
        pass
    
    def create_navigation_ship_table(self, ship_id: str):
        """항해/선박정보 테이블 생성"""
        pass
    
    def create_indexes(self, ship_id: str):
        """3개 테이블 모두에 BRIN 인덱스 생성"""
        pass
```

### 5.3 구현 단계

#### Phase 1: 준비 단계
- [ ] `channel_router.py` 구현
- [ ] `multi_table_generator.py` 구현
- [ ] 채널 정의 파일 로딩 테스트
- [ ] 테이블 생성 로직 테스트

#### Phase 2: 마이그레이션 로직 수정
- [ ] `chunked_migration_strategy.py` 수정
  - 채널 필터링 로직 추가
  - 3개 테이블로 분산 INSERT
- [ ] `realtime_processor.py` 수정
  - 실시간 데이터 라우팅

#### Phase 3: 테스트
- [ ] 단위 테스트 작성
- [ ] 통합 테스트 (소규모 데이터)
- [ ] 성능 테스트

#### Phase 4: 배포
- [ ] 기존 데이터 백업
- [ ] 전체 데이터 재마이그레이션
- [ ] 실시간 프로세스 전환

---

## 6. 성능 예상 효과

### 6.1 테이블 크기 분산

| 테이블 | 컬럼 수 | 예상 크기 (비율) |
|--------|---------|------------------|
| Auxiliary Systems | 348 | ~33% |
| Engine Generator | 651 | ~63% |
| Navigation Ship | 41 | ~4% |

### 6.2 쿼리 성능 개선

#### 6.2.1 Before (단일 테이블)
```sql
-- 1,037개 컬럼 중 일부만 조회해도 전체 테이블 스캔
SELECT created_time, hs4sd_v1_me01_rpm_speed
FROM tbl_data_timeseries_imo9976903
WHERE created_time BETWEEN '2024-01-01' AND '2024-01-02';
```

#### 6.2.2 After (분리 테이블)
```sql
-- 필요한 테이블만 접근 (651개 컬럼)
SELECT created_time, hs4sd_v1_me01_rpm_speed
FROM engine_generator_imo9976903
WHERE created_time BETWEEN '2024-01-01' AND '2024-01-02';
```

### 6.3 예상 이점

1. **I/O 효율성 향상**
   - 필요한 데이터만 읽음
   - 테이블 크기 감소로 캐시 효율 증가

2. **INSERT 성능 안정화**
   - 테이블별 병렬 INSERT 가능
   - 락 경합 감소

3. **유지보수성 향상**
   - 시스템별 독립적 관리
   - 문제 발생 시 영향 범위 최소화

4. **확장성 개선**
   - 시스템별 별도 파티셔닝 전략 적용 가능
   - 테이블별 독립적 백업/복구

---

## 7. 마이그레이션 리스크 및 대응

### 7.1 리스크

| 리스크 | 영향도 | 대응 방안 |
|--------|--------|-----------|
| 채널 분류 오류 | 높음 | 철저한 테스트 및 검증 |
| 마이그레이션 중 데이터 손실 | 높음 | 백업 필수, 단계별 검증 |
| 성능 저하 | 중간 | 성능 테스트 후 조정 |
| 실시간 처리 중단 | 높음 | Blue-Green 배포 전략 |

### 7.2 롤백 계획

1. **즉시 롤백 가능:**
   - 기존 단일 테이블 유지
   - 새 테이블로만 데이터 복제

2. **검증 후 전환:**
   - 두 시스템 동시 운영 (일정 기간)
   - 데이터 정합성 확인 후 전환

---

## 8. 모니터링 및 검증

### 8.1 검증 항목

- [ ] 총 레코드 수 일치
- [ ] 각 테이블 레코드 수 합계 = 원본 레코드 수
- [ ] 시간 범위별 데이터 누락 확인
- [ ] 채널별 통계값 비교 (min, max, avg)

### 8.2 모니터링 지표

- 테이블별 INSERT 속도
- 테이블별 크기 증가율
- 쿼리 응답 시간
- 인덱스 효율성

---

## 9. 참고 자료

- `column_list_auxiliary_systems.txt`: 347개 채널
- `column_list_engine_generator.txt`: 650개 채널
- `column_list_navigation_ship.txt`: 40개 채널
- `brinindex.sql`: BRIN 인덱스 설정
- 기존 구현: `table_generator.py`, `chunked_migration_strategy.py`

---

## 10. 결론

이 설계를 통해:
1. ✅ 시스템별 데이터 분리로 관리 효율성 향상
2. ✅ 쿼리 성능 개선 (필요한 테이블만 접근)
3. ✅ 확장성 및 유지보수성 향상
4. ✅ 시스템별 독립적 최적화 가능

**다음 단계:** Phase 1 구현 시작 (channel_router, multi_table_generator)

