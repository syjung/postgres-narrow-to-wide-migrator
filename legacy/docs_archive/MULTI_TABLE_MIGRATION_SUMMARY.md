# Multi-Table Migration 요약

## 📋 변경 개요

### Before (현재)
```
선박당 1개 테이블
└── tbl_data_timeseries_{ship_id}
    └── 1,037개 컬럼 (created_time + 1,036 channels)
```

### After (신규)
```
선박당 3개 테이블
├── auxiliary_systems_{ship_id}        (348 컬럼)
├── engine_generator_{ship_id}         (651 컬럼)
└── navigation_ship_{ship_id}          (41 컬럼)
```

---

## 🎯 목표

1. **성능 최적화**: 쿼리 시 필요한 테이블만 접근
2. **관리 효율성**: 시스템별 독립적 관리
3. **확장성**: 테이블별 최적화 전략 적용 가능

---

## 📊 테이블 구성

| 테이블명 | 채널 수 | 주요 시스템 | 비율 |
|---------|---------|------------|------|
| **auxiliary_systems** | 347개 | 카고탱크, APRS, 보일러, 연료가스압축기 | 33% |
| **engine_generator** | 650개 | 주기관, 발전기 | 63% |
| **navigation_ship** | 40개 | VAP, VDR, 선박정보 | 4% |

---

## 🔧 구현 단계

### Phase 1: 준비 (1-2일)
- [ ] `channel_router.py` - 채널 라우팅 로직
- [ ] `multi_table_generator.py` - 3개 테이블 생성

### Phase 2: 마이그레이션 로직 (2-3일)
- [ ] `chunked_migration_strategy.py` 수정
- [ ] `realtime_processor.py` 수정

### Phase 3: 테스트 (2-3일)
- [ ] 단위 테스트
- [ ] 통합 테스트
- [ ] 성능 테스트

### Phase 4: 배포 (1-2일)
- [ ] 백업 생성
- [ ] 데이터 재마이그레이션
- [ ] 실시간 프로세스 전환

**예상 소요 시간: 1-2주**

---

## 📈 예상 효과

### 쿼리 성능
```sql
-- Before: 1,037개 컬럼 테이블 스캔
SELECT created_time, me01_rpm FROM tbl_data_timeseries_imo9976903;

-- After: 651개 컬럼 테이블 스캔 (37% 절감)
SELECT created_time, me01_rpm FROM engine_generator_imo9976903;
```

### 이점
- ✅ I/O 효율성 향상 (필요한 데이터만 읽음)
- ✅ 캐시 효율 증가 (테이블 크기 감소)
- ✅ INSERT 성능 안정화 (병렬 처리)
- ✅ 유지보수 용이 (시스템별 관리)

---

## 🔍 주요 파일

### 설정 파일
- `column_list_auxiliary_systems.txt` - 보조 시스템 채널 목록
- `column_list_engine_generator.txt` - 엔진/발전기 채널 목록
- `column_list_navigation_ship.txt` - 항해/선박정보 채널 목록

### 구현 파일
- `channel_router.py` - **신규** 채널 라우팅
- `multi_table_generator.py` - **신규** 테이블 생성
- `chunked_migration_strategy.py` - **수정** 마이그레이션 로직
- `realtime_processor.py` - **수정** 실시간 처리
- `config.py` - **수정** 설정 추가

---

## ⚠️ 리스크 및 대응

| 리스크 | 대응 방안 |
|--------|-----------|
| 채널 분류 오류 | 철저한 검증 및 테스트 |
| 데이터 손실 | 백업 필수, 단계별 검증 |
| 성능 저하 | 성능 테스트 후 조정 |

---

## 📚 상세 문서

전체 설계서: `docs/MULTI_TABLE_DESIGN.md`

