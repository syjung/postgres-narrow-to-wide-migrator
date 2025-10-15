# 운영 가이드 (Operation Guide)

## 📋 개요

PostgreSQL Narrow-to-Wide 마이그레이션 시스템의 운영 가이드입니다. 실시간 데이터 처리와 배치 데이터 마이그레이션을 별도의 프로세스로 분리하여 독립적으로 실행할 수 있습니다.

## 🚀 빠른 시작

### 1. 실시간 데이터 처리

#### 시작
```bash
./start_realtime.sh
```

#### 중지
```bash
./stop_realtime.sh
```

#### 로그 확인
```bash
# 실시간 로그 팔로우
./view_logs.sh -f realtime

# 실시간 로그 마지막 100줄
./view_logs.sh -t realtime 100
```

### 2. 배치 데이터 마이그레이션

#### 배치만 실행 (과거 데이터 처리 후 종료)
```bash
./start_batch.sh batch
```

#### 동시 실행 (배치 + 실시간, 계속 실행)
```bash
./start_batch.sh concurrent
```

#### 중지
```bash
./stop_batch.sh
```

#### 로그 확인
```bash
# 배치 로그 팔로우
./view_logs.sh -f batch

# 배치 로그 마지막 100줄
./view_logs.sh -t batch 100
```

### 3. 통합 로그 확인

#### 모든 로그 팔로우
```bash
./view_logs.sh -f all
```

#### 에러 메시지만 확인
```bash
./view_logs.sh -e
```

#### 성공 메시지만 확인
```bash
./view_logs.sh -s
```

#### 로그 통계
```bash
./view_logs.sh -c
```

## 📊 상세 로그 정보

### 실시간 처리 로그
- **파일**: `logs/realtime.log`
- **PID 파일**: `logs/realtime.pid`
- **중요 정보**:
  ```
  ✅ REALTIME INSERT SUCCESS: tbl_data_timeseries_imo9999994
     📊 Records: 2 rows inserted
     📊 Columns: 1037 data columns (total: 1038)
     📊 Time Range: 2025-10-03 00:24:08 ~ 2025-10-03 00:24:08
     📊 Affected Rows: 2
  ```

### 배치 마이그레이션 로그
- **파일**: `logs/batch.log`
- **PID 파일**: `logs/batch.pid`
- **중요 정보**:
  ```
  ✅ BATCH INSERT SUCCESS: tbl_data_timeseries_imo9999994
     📊 Records: 1000 rows inserted
     📊 Columns: 1037 data columns (total: 1038)
     📊 Time Range: 2025-10-02 00:00:00 ~ 2025-10-02 23:59:59
     📊 Affected Rows: 1000
  ```

### 청크 마이그레이션 로그
- **중요 정보**:
  ```
  ✅ CHUNK MIGRATION SUCCESS: IMO9999994
     📊 Records processed: 50000
     📊 Columns: 1037 data columns
     📊 Time Range: 2025-10-02 00:00:00 ~ 2025-10-02 23:59:59
     📊 Method: Chunked migration (24-hour chunks)
  ```

### Ultra-Fast 마이그레이션 로그
- **중요 정보**:
  ```
  ✅ ULTRA-FAST INSERT SUCCESS: tbl_data_timeseries_imo9999994
     📊 Records: 100000 rows inserted
     📊 Columns: 1037 data columns (total: 1038)
     📊 Time Range: 2025-10-02 00:00:00 ~ 2025-10-02 23:59:59
     📊 Method: PostgreSQL COPY FROM (optimized)
  ```

## 🔄 프로세스 관리

### 프로세스 상태 확인
```bash
# 실시간 프로세스 확인
ps aux | grep realtime

# 배치 프로세스 확인
ps aux | grep batch
```

### 강제 종료 (필요시)
```bash
# PID로 직접 종료
kill -TERM <PID>

# 강제 종료
kill -KILL <PID>
```

## 📈 모니터링

### 실시간 모니터링
```bash
# 실시간 로그 팔로우
./view_logs.sh -f realtime

# 성공 메시지만 확인
./view_logs.sh -s

# 에러 메시지만 확인
./view_logs.sh -e
```

### 배치 모니터링
```bash
# 배치 로그 팔로우
./view_logs.sh -f batch

# 로그 통계 확인
./view_logs.sh -c
```

## 🛠️ 문제 해결

### 프로세스가 시작되지 않는 경우
1. PID 파일 확인: `ls -la logs/*.pid`
2. 로그 파일 확인: `./view_logs.sh -e`
3. 의존성 확인: `pip install -r requirements.txt`

### 프로세스가 예상대로 작동하지 않는 경우
1. 로그 확인: `./view_logs.sh -f <type>`
2. 에러 확인: `./view_logs.sh -e`
3. 프로세스 상태 확인: `ps aux | grep <type>`

### 로그 파일이 너무 큰 경우
- 로그는 자동으로 100MB마다 회전됩니다
- 7일 이상 된 로그는 자동으로 삭제됩니다
- 수동으로 정리: `rm logs/*.log`

## 📝 예시 시나리오

### 시나리오 1: 순차 실행
```bash
# 1. 배치 마이그레이션 먼저 실행
./start_batch.sh batch

# 2. 배치 완료 후 실시간 처리 시작
./start_realtime.sh
```

### 시나리오 2: 동시 실행
```bash
# 배치와 실시간을 동시에 실행
./start_batch.sh concurrent
```

### 시나리오 3: 실시간만 실행
```bash
# 실시간 데이터만 처리
./start_realtime.sh
```

## 🔧 설정

### PostgreSQL 최적화
- **실시간**: `work_mem=16MB`, `maintenance_work_mem=64MB`
- **배치**: `work_mem=32MB`, `maintenance_work_mem=128MB`

### 로그 설정
- **회전 크기**: 100MB
- **보관 기간**: 7일
- **레벨**: INFO

## 📋 스크립트 목록

### 활성 스크립트
- `start_realtime.sh` - 실시간 데이터 처리 시작
- `start_batch.sh` - 배치 마이그레이션 시작
- `stop_realtime.sh` - 실시간 데이터 처리 중지
- `stop_batch.sh` - 배치 마이그레이션 중지
- `check_status.sh` - 마이그레이션 상태 확인
- `view_logs.sh` - 로그 확인 및 모니터링

### 레거시 스크립트 (legacy/ 폴더)
- `legacy/run_migration.sh` - 기존 통합 마이그레이션 (더 이상 권장되지 않음)
- `legacy/stop_migration.sh` - 기존 마이그레이션 중지 (더 이상 권장되지 않음)
- `legacy/restart_migration.sh` - 기존 마이그레이션 재시작 (더 이상 권장되지 않음)

### 마이그레이션 가이드
기존 통합 스크립트에서 새로운 분리된 스크립트로 마이그레이션하는 방법:

#### 기존 방식 (레거시)
```bash
# 기존 통합 실행
./legacy/run_migration.sh

# 기존 중지
./legacy/stop_migration.sh

# 기존 재시작
./legacy/restart_migration.sh
```

#### 새로운 방식 (권장)
```bash
# 실시간과 배치를 분리하여 실행
./start_realtime.sh    # 실시간 처리 시작
./start_batch.sh batch # 배치 마이그레이션 시작

# 각각 독립적으로 중지
./stop_realtime.sh     # 실시간 처리 중지
./stop_batch.sh        # 배치 마이그레이션 중지
```
