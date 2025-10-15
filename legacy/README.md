# 레거시 스크립트 (Legacy Scripts)

이 폴더는 이전 버전의 통합 마이그레이션 스크립트들을 포함합니다.

## 📋 포함된 스크립트들

- `run_migration.sh` - 기존 통합 마이그레이션 실행 스크립트
- `stop_migration.sh` - 기존 마이그레이션 중지 스크립트  
- `restart_migration.sh` - 기존 마이그레이션 재시작 스크립트

## ⚠️ 주의사항

이 스크립트들은 **더 이상 권장되지 않습니다**. 새로운 분리된 스크립트들을 사용하세요:

### 새로운 스크립트 사용법

#### 실시간 데이터 처리
```bash
# 실시간 데이터 처리 시작
./start_realtime.sh

# 실시간 처리 중지
./stop_realtime.sh
```

#### 배치 마이그레이션
```bash
# 배치 마이그레이션 시작 (과거 데이터 처리 후 종료)
./start_batch.sh batch

# 동시 실행 (배치 + 실시간, 계속 실행)
./start_batch.sh concurrent

# 배치 마이그레이션 중지
./stop_batch.sh
```

#### 모니터링
```bash
# 로그 확인
./view_logs.sh -f realtime
./view_logs.sh -f batch
./view_logs.sh -f all

# 상태 확인
./check_status.sh
```

## 🔄 마이그레이션 가이드

기존 통합 스크립트에서 새로운 분리된 스크립트로 마이그레이션하는 방법:

### 기존 방식
```bash
# 기존 통합 실행
./run_migration.sh

# 기존 중지
./stop_migration.sh

# 기존 재시작
./restart_migration.sh
```

### 새로운 방식
```bash
# 실시간과 배치를 분리하여 실행
./start_realtime.sh    # 실시간 처리 시작
./start_batch.sh batch # 배치 마이그레이션 시작

# 각각 독립적으로 중지
./stop_realtime.sh     # 실시간 처리 중지
./stop_batch.sh        # 배치 마이그레이션 중지
```

## 📊 장점

새로운 분리된 스크립트의 장점:

1. **독립적 실행**: 실시간과 배치를 각각 독립적으로 실행/중지 가능
2. **별도 로그**: `logs/realtime.log`, `logs/batch.log`로 분리된 로그 관리
3. **상세한 로그**: 처리된 레코드 수, 컬럼 수, 시간 범위 등 상세 정보 제공
4. **유연한 제어**: 필요에 따라 실시간만 또는 배치만 실행 가능
5. **자동 종료**: 배치 마이그레이션은 완료 시 자동 종료

## 🗑️ 제거 예정

이 레거시 스크립트들은 향후 버전에서 제거될 예정입니다. 새로운 스크립트로 마이그레이션하시기 바랍니다.
