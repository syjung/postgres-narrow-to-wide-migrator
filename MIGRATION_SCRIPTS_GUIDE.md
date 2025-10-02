# 🚀 PostgreSQL Narrow-to-Wide Migration 관리 스크립트

서버에서 백그라운드로 마이그레이션을 실행하고 상태를 확인할 수 있는 쉘 스크립트들입니다.

## 📋 스크립트 목록

| 스크립트 | 기능 | 사용법 |
|----------|------|--------|
| `run_migration.sh` | 마이그레이션 시작 | `./run_migration.sh [mode] [interval]` |
| `check_status.sh` | 상태 확인 | `./check_status.sh` |
| `view_logs.sh` | 로그 보기 | `./view_logs.sh [options]` |
| `stop_migration.sh` | 마이그레이션 중지 | `./stop_migration.sh [options]` |
| `restart_migration.sh` | 마이그레이션 재시작 | `./restart_migration.sh [mode] [interval]` |

## 🚀 기본 사용법

### 1. 마이그레이션 시작
```bash
# 동시 처리 전략으로 시작 (권장)
./run_migration.sh

# 하이브리드 전략으로 시작
./run_migration.sh hybrid

# 5분 간격으로 시작
./run_migration.sh concurrent 5

# 전체 마이그레이션 시작
./run_migration.sh full
```

### 2. 상태 확인
```bash
# 현재 상태 확인
./check_status.sh
```

### 3. 로그 보기
```bash
# 최근 50줄 보기
./view_logs.sh

# 실시간 로그 보기
./view_logs.sh -f

# 에러만 보기
./view_logs.sh -e

# 특정 키워드 검색
./view_logs.sh -g "chunk"
```

### 4. 마이그레이션 중지
```bash
# 정상 중지
./stop_migration.sh

# 강제 중지
./stop_migration.sh -f
```

### 5. 마이그레이션 재시작
```bash
# 현재 설정으로 재시작
./restart_migration.sh

# 다른 모드로 재시작
./restart_migration.sh hybrid 2
```

## 🔧 고급 사용법

### 로그 관리
```bash
# 로그 통계 보기
./view_logs.sh -s

# 마지막 100줄 보기
./view_logs.sh -n 100

# 경고 메시지만 보기
./view_logs.sh -w

# 정보 메시지만 보기
./view_logs.sh -i
```

### 상태 모니터링
```bash
# 상태 확인 (상세 정보 포함)
./check_status.sh

# 실시간 로그 모니터링
./view_logs.sh -f
```

## 📊 마이그레이션 모드

| 모드 | 설명 | 사용 시기 |
|------|------|-----------|
| `concurrent` | 실시간 + 백필 동시 처리 | **권장** - Zero Downtime |
| `hybrid` | 고급 동시 처리 | 고급 모니터링 필요시 |
| `streaming` | 모든 데이터 스트리밍 | 간단한 처리 필요시 |
| `full` | 전체 마이그레이션 | 처음 실행시 |
| `schema-only` | 스키마 분석만 | 스키마 확인시 |
| `tables-only` | 테이블 생성만 | 테이블 생성시 |
| `migration-only` | 데이터 마이그레이션만 | 데이터만 마이그레이션시 |
| `realtime` | 실시간 처리만 | 실시간 처리만 필요시 |
| `dual-write` | Dual-Write 모드 | 양방향 동기화시 |

## 🎯 서버 운영 시나리오

### 시나리오 1: 처음 마이그레이션 시작
```bash
# 1. 전체 마이그레이션 시작
./run_migration.sh full

# 2. 상태 확인
./check_status.sh

# 3. 로그 모니터링
./view_logs.sh -f
```

### 시나리오 2: 실시간 서비스 중 마이그레이션
```bash
# 1. 동시 처리 전략으로 시작 (Zero Downtime)
./run_migration.sh concurrent

# 2. 주기적으로 상태 확인
./check_status.sh

# 3. 문제 발생시 로그 확인
./view_logs.sh -e
```

### 시나리오 3: 마이그레이션 중단 및 재시작
```bash
# 1. 현재 상태 확인
./check_status.sh

# 2. 마이그레이션 중지
./stop_migration.sh

# 3. 재시작
./restart_migration.sh concurrent
```

## 📁 파일 구조

```
postgres-narrow-to-wide-migrator/
├── run_migration.sh          # 마이그레이션 시작
├── check_status.sh           # 상태 확인
├── view_logs.sh              # 로그 보기
├── stop_migration.sh         # 마이그레이션 중지
├── restart_migration.sh      # 마이그레이션 재시작
├── logs/                     # 로그 디렉토리
│   └── migration.log         # 마이그레이션 로그
├── migration.pid             # 프로세스 ID 파일
└── migration_cutoff_time.txt # cutoff_time 저장 파일
```

## ⚠️ 주의사항

1. **PID 파일**: `migration.pid` 파일이 프로세스 관리를 위해 사용됩니다.
2. **로그 파일**: `logs/migration.log`에 모든 로그가 저장됩니다.
3. **cutoff_time**: `migration_cutoff_time.txt`에 마이그레이션 완료 시점이 저장됩니다.
4. **백그라운드 실행**: 모든 마이그레이션은 백그라운드에서 실행됩니다.
5. **정상 종료**: 가능하면 `./stop_migration.sh`로 정상 종료하세요.

## 🆘 문제 해결

### 마이그레이션이 시작되지 않을 때
```bash
# PID 파일 확인
ls -la migration.pid

# 프로세스 확인
ps aux | grep python

# 강제 정리
./stop_migration.sh -f
```

### 로그가 보이지 않을 때
```bash
# 로그 파일 확인
ls -la logs/

# 로그 파일 생성
mkdir -p logs
touch logs/migration.log
```

### 상태 확인이 안 될 때
```bash
# Python 모듈 확인
python3 -c "from main import MigrationManager; print('OK')"

# 데이터베이스 연결 확인
python3 -c "from database import db_manager; print('OK')"
```

## 🎉 성공적인 운영을 위한 팁

1. **정기적인 상태 확인**: `./check_status.sh`를 주기적으로 실행
2. **로그 모니터링**: `./view_logs.sh -f`로 실시간 모니터링
3. **백업**: 중요한 데이터는 미리 백업
4. **테스트**: 운영 환경 적용 전 테스트 환경에서 충분히 테스트
5. **문서화**: 마이그레이션 진행상황을 문서화

이제 서버에서 안전하고 효율적으로 마이그레이션을 운영할 수 있습니다! 🚀
