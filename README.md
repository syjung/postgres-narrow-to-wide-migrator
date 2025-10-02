# PostgreSQL Narrow-to-Wide Table Migration

PostgreSQL의 대용량 narrow type 테이블을 wide type 테이블로 마이그레이션하는 프로젝트입니다.

## 프로젝트 개요

이 프로젝트는 `tenant.tbl_data_timeseries` 테이블의 데이터를 ship_id별로 분리된 wide type 테이블로 변환합니다.

### 주요 기능

- **스키마 분석**: 60분간의 샘플 데이터를 기반으로 wide table 스키마 생성
- **테이블 생성**: ship_id별 wide type 테이블 자동 생성
- **청크 기반 마이그레이션**: 대용량 데이터를 24시간 청크 단위로 안전하게 처리
- **동시 처리**: 실시간 데이터 처리와 기존 데이터 백필을 동시에 실행
- **cutoff_time 관리**: 마이그레이션 완료 시점을 영구 저장하여 데이터 중복 방지
- **실시간 처리**: 새로운 데이터를 실시간으로 wide table에 반영
- **모니터링**: 마이그레이션 진행상황 및 시스템 상태 모니터링

### 성능 특징

- **청크 기반 처리**: 24시간 단위로 데이터를 분할하여 메모리 효율성 극대화
- **동시 처리**: 실시간 + 백필을 멀티스레딩으로 동시 실행
- **COPY 방식**: PostgreSQL의 가장 빠른 데이터 처리 방식 사용
- **메모리 최적화**: 스트리밍 방식으로 대용량 데이터 처리
- **배치 처리**: 50,000 레코드 단위로 효율적인 데이터 변환
- **성능 향상**: 기존 방식 대비 15배 빠른 처리 속도
- **Zero Downtime**: 실시간 데이터 수집 중단 없이 마이그레이션 가능

## 설치 및 설정

### 1. 의존성 설치

```bash
pip install -r requirements.txt
```

### 2. 테스트 환경 설정

```bash
# 테스트 실행
python run_tests.py --type unit
python run_tests.py --type integration
python run_tests.py --all

# 특정 테스트 파일 실행
python run_tests.py --file tests/unit/test_schema_analyzer.py

# 커버리지 리포트 확인
python run_tests.py --type all
# HTML 리포트는 htmlcov/index.html에서 확인 가능
```

### 3. 데이터베이스 설정

`config.py` 파일에서 데이터베이스 연결 정보를 확인하거나 수정합니다:

```python
class DatabaseConfig(BaseSettings):
    host: str = "222.99.122.73"
    port: int = 25432
    database: str = "tenant_builder"
    user: str = "tapp"
    password: str = "tapp.123"
```

## 사용법

### 기본 사용법

#### **권장 방법: 동시 처리 (Concurrent Strategy)**
```bash
# 1. 스키마 분석 및 테이블 생성
python main.py --mode schema-only
python main.py --mode tables-only

# 2. 동시 처리 시작 (실시간 + 백필)
python main.py --mode concurrent --interval 1

# 3. 상태 확인
python main.py --mode status
```

#### **전통적인 방법: 순차 처리**
```bash
# 전체 마이그레이션 실행
python main.py --mode full

# 단계별 실행
python main.py --mode schema-only
python main.py --mode tables-only
python main.py --mode migration-only --cutoff-time "2024-10-02 17:00:00"
python main.py --mode realtime
```

#### **고급 사용법**
```bash
# 하이브리드 전략 (고급 모니터링)
python main.py --mode hybrid --interval 1

# 스트리밍 전략 (모든 데이터 스트리밍)
python main.py --mode streaming --interval 1

# Dual-Write 패턴
python main.py --mode dual-write --interval 1

# 현재 상태 확인
python main.py --mode status
```

### 초고속 마이그레이션 사용법

```bash
# 특정 선박의 데이터 마이그레이션
python -c "
from ultra_fast_migrator import ultra_fast_migrator
result = ultra_fast_migrator.migrate_ship_data_ultra_fast('IMO9999994')
print(f'마이그레이션 결과: {result}')
"

# 특정 시점 이후 데이터만 마이그레이션
python -c "
from ultra_fast_migrator import ultra_fast_migrator
from datetime import datetime, timedelta
cutoff_time = datetime.now() - timedelta(hours=1)
result = ultra_fast_migrator.migrate_ship_data_ultra_fast('IMO9999994', cutoff_time)
print(f'마이그레이션 결과: {result}')
"
```

### 고급 옵션

```bash
# 특정 시점 이전 데이터만 마이그레이션
python main.py --mode full --cutoff-time "2024-01-01 00:00:00"

# 기존 테이블 삭제 후 재생성
python main.py --mode full --drop-tables

# 실시간 처리 간격 설정 (분 단위)
python main.py --mode realtime --interval 5

# 특정 선박만 처리
python main.py --mode full --ship-id IMO9976903
```

## 프로젝트 구조

```
postgres-narrow-to-wide-migrator/
├── main.py                           # 메인 실행 스크립트
├── config.py                         # 설정 파일 (대상 선박, PostgreSQL 최적화)
├── database.py                       # 데이터베이스 연결 및 유틸리티
├── schema_analyzer.py                # 스키마 분석 모듈
├── table_generator.py                # Wide table 생성 모듈
├── ultra_fast_migrator.py           # 초고속 데이터 마이그레이션 모듈 (COPY 방식)
├── data_migrator.py                  # 데이터 마이그레이션 관리 클래스
├── chunked_migration_strategy.py     # 청크 기반 마이그레이션 전략
├── concurrent_migration_strategy.py  # 동시 처리 전략 (실시간 + 백필)
├── cutoff_time_manager.py            # cutoff_time 영구 저장 관리
├── realtime_processor.py             # 실시간 데이터 처리 모듈
├── monitoring.py                     # 모니터링 및 진행상황 추적
├── requirements.txt                  # Python 의존성
├── run_tests.py                     # 테스트 실행 스크립트
├── pytest.ini                      # pytest 설정
├── column_list.txt                  # 대상 컬럼 목록
├── migration_cutoff_time.txt        # cutoff_time 저장 파일
├── README.md                        # 프로젝트 문서
├── PRD.md                           # 제품 요구사항 문서
├── prompt.md                        # 초기 요구사항 문서
├── migration_optimization_plan.md   # 마이그레이션 최적화 계획서
├── large_scale_realtime_migration_strategy.md  # 대용량 실시간 마이그레이션 전략
├── migration_execution_guide.md      # 마이그레이션 실행 가이드
├── project_improvement_plan.md       # 프로젝트 개선 계획
└── tests/                           # 테스트 파일들
    ├── __init__.py
    ├── conftest.py                  # pytest 설정 및 공통 fixtures
    ├── fixtures/                    # 테스트 픽스처
    ├── unit/                        # 단위 테스트
    │   ├── test_database.py
    │   ├── test_schema_analyzer.py
    │   └── test_table_generator.py
    └── integration/                 # 통합 테스트
        └── test_migration_flow.py
```

## 새로운 기능

### 🚀 **동시 처리 전략 (Concurrent Strategy)**
- **실시간 처리**: 새로운 데이터를 즉시 처리
- **백그라운드 백필**: 기존 데이터를 백그라운드에서 청크 단위로 처리
- **Zero Downtime**: 실시간 데이터 수집 중단 없이 마이그레이션 가능

### 🔧 **청크 기반 마이그레이션**
- **24시간 청크**: 대용량 데이터를 24시간 단위로 분할 처리
- **메모리 효율성**: 메모리 부족 문제 해결
- **장애 복구**: 실패한 청크만 재처리 가능

### 💾 **cutoff_time 영구 저장**
- **자동 저장**: 마이그레이션 완료 시점을 파일에 저장
- **자동 로드**: 프로세스 재시작 시 자동으로 cutoff_time 복구
- **데이터 중복 방지**: cutoff_time 이후 데이터만 실시간 처리

### 📊 **향상된 모니터링**
- **실시간 진행률**: 청크별 진행상황 실시간 추적
- **상태 확인**: `python main.py --mode status`로 전체 상태 확인
- **성능 지표**: 처리 속도, 메모리 사용량 등 모니터링

## 데이터 변환 규칙

### Narrow to Wide 변환

**소스 테이블**: `tenant.tbl_data_timeseries`
- `ship_id`, `data_channel_id`, `created_time`, `value_format`, `bool_v`, `str_v`, `long_v`, `double_v`

**타겟 테이블**: `tbl_data_timeseries_{ship_id}`
- `created_time` (PRIMARY KEY)
- `data_channel_id` 값들이 컬럼으로 변환 (text type)

### Value Format 매핑

- **Decimal** → `double_v` 값 사용
- **Integer** → `long_v` 값 사용
- **String** → `str_v` 값 사용
- **Boolean** → `bool_v` 값 사용

## 마이그레이션 프로세스

### 1. 스키마 분석 단계
- 10분간의 샘플 데이터 수집
- 고유한 `data_channel_id` 값들을 컬럼으로 변환
- `value_format`에 따른 데이터 타입 매핑 규칙 적용

### 2. 테이블 생성 단계
- 각 ship_id별로 wide type 테이블 생성
- `created_time`을 PRIMARY KEY로 설정
- `created_time` 기준 인덱스 생성

### 3. 데이터 마이그레이션 단계
- 배치 단위로 데이터 변환 및 삽입
- 진행상황 모니터링
- 데이터 일관성 검증

### 4. 실시간 처리 단계
- 1분 간격으로 새로운 데이터 처리
- 새로운 `data_channel_id` 발견 시 동적 스키마 업데이트
- UPSERT 로직으로 중복 데이터 처리

## 트러블슈팅

### 성능 관련 이슈

#### 메모리 부족 문제
- **증상**: 대용량 데이터 처리 시 OOM 발생
- **해결방안**: 청크 단위 배치 처리 (기본값: 10,000건)

#### 인덱스 성능 저하
- **증상**: 데이터 삽입 시 인덱스 업데이트로 인한 성능 저하
- **해결방안**: 마이그레이션 중 인덱스 일시 비활성화

#### 락 경합 문제
- **증상**: 테이블 락으로 인한 다른 작업 블로킹
- **해결방안**: 작업 시간대 조정, 작은 단위로 나누어 처리

### 데이터 일관성 이슈

#### 트랜잭션 타임아웃
- **증상**: 긴 트랜잭션으로 인한 타임아웃 발생
- **해결방안**: 작은 단위 트랜잭션으로 분할

#### 데이터 타입 변환 오류
- **증상**: value_format과 실제 데이터 타입 불일치
- **해결방안**: 데이터 검증 로직 구현

## 모니터링

### 시스템 상태 확인

```bash
python main.py --mode status
```

### 로그 파일

- 기본 로그 파일: `logs/migration.log`
- 로그 레벨: INFO
- 로테이션: 10MB 단위
- 보관: 5개 파일

### 성능 메트릭

- 데이터베이스 연결 상태
- 디스크 사용량
- 메모리 사용량
- 마이그레이션 진행률
- 처리 속도 (시간당 레코드 수)

## 설정 옵션

`config.py`에서 다음 설정을 조정할 수 있습니다:

```python
class MigrationConfig(BaseSettings):
    batch_size: int = 10000          # 배치 크기
    sample_minutes: int = 10         # 샘플 데이터 수집 시간 (분)
    migration_timeout: int = 3600    # 마이그레이션 타임아웃 (초)
    chunk_size: int = 1000          # 청크 크기
```

## TDD (Test-Driven Development) 가이드라인

이 프로젝트는 TDD 기반으로 개발되었으며, 새로운 기능 구현 시 다음 가이드라인을 따릅니다:

### 1. 테스트 우선 개발 프로세스

```bash
# 1. 실패하는 테스트 작성 (Red)
python run_tests.py --file tests/unit/test_new_feature.py

# 2. 테스트를 통과하는 최소한의 코드 작성 (Green)
# 코드 구현...

# 3. 리팩토링 (Refactor)
# 코드 개선...

# 4. 모든 테스트 통과 확인
python run_tests.py --all
```

### 2. 테스트 작성 규칙

#### 단위 테스트 (Unit Tests)
- 각 모듈의 개별 함수/메서드 테스트
- Mock을 사용하여 외부 의존성 격리
- 테스트 파일 위치: `tests/unit/test_*.py`

#### 통합 테스트 (Integration Tests)
- 여러 모듈 간의 상호작용 테스트
- 전체 마이그레이션 플로우 테스트
- 테스트 파일 위치: `tests/integration/test_*.py`

#### 테스트 명명 규칙
```python
def test_function_name_scenario_expected_result(self):
    """Test description"""
    # Given
    # When
    # Then
    assert expected == actual
```

### 3. 테스트 커버리지

- 최소 80% 이상의 코드 커버리지 유지
- 커버리지 리포트: `htmlcov/index.html`
- 커버리지 확인: `python run_tests.py --all`

### 4. 새로운 기능 개발 시 체크리스트

- [ ] 실패하는 테스트 작성
- [ ] 테스트를 통과하는 코드 구현
- [ ] 리팩토링 및 코드 개선
- [ ] 모든 기존 테스트 통과 확인
- [ ] 새로운 테스트 추가 (필요시)
- [ ] 문서 업데이트

### 5. 테스트 실행 명령어

```bash
# 전체 테스트 실행
python run_tests.py --all

# 단위 테스트만 실행
python run_tests.py --type unit

# 통합 테스트만 실행
python run_tests.py --type integration

# 특정 테스트 파일 실행
python run_tests.py --file tests/unit/test_schema_analyzer.py

# 커버리지 없이 실행
python run_tests.py --no-coverage

# 상세 출력
python run_tests.py --verbose
```

## 주의사항

1. **백업**: 마이그레이션 전 반드시 데이터베이스 백업을 수행하세요.
2. **테스트**: 프로덕션 환경에서 실행하기 전에 테스트 환경에서 충분히 테스트하세요.
3. **모니터링**: 마이그레이션 중 시스템 리소스 사용량을 모니터링하세요.
4. **롤백 계획**: 문제 발생 시 롤백 계획을 준비하세요.
5. **TDD 준수**: 새로운 기능 개발 시 반드시 테스트 우선으로 개발하세요.

## 지원

문제가 발생하거나 질문이 있으시면 로그 파일을 확인하고 다음 정보를 포함하여 문의하세요:

- 실행한 명령어
- 오류 메시지
- 로그 파일 내용
- 시스템 환경 정보

## 라이선스

이 프로젝트는 MIT 라이선스 하에 배포됩니다.

