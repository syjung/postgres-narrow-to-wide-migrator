# 실패한 청크 재처리 가이드

## 개요

패러럴 배치 처리 중 connection timeout 등의 이유로 실패한 청크들을 재처리하는 도구입니다.

## 파일 구성

- `post_proc.csv`: 실패한 청크 목록 (IMO번호, 시작시간, 종료시간)
- `reprocess_failed_chunks.py`: 재처리 Python 스크립트
- `reprocess_failed_chunks.sh`: 실행용 쉘 스크립트
- `logs/reprocess_YYYYMMDD_HHMMSS.log`: 재처리 전용 로그 파일
- `logs/reprocess_latest.log`: 최신 재처리 로그 (심볼릭 링크)

## CSV 파일 형식

```csv
IMO9986063,2025-06-15 14:40:40.388958,2025-06-15 20:40:40.388958
IMO9986104,2025-07-16 02:40:40.394745,2025-07-16 08:40:40.394745
IMO9986087,2025-07-15 14:40:40.391527,2025-07-15 20:40:40.391527
```

각 라인은:
- 첫 번째 컬럼: 선박 ID (예: IMO9986063)
- 두 번째 컬럼: 청크 시작 시간 (ISO 8601 형식)
- 세 번째 컬럼: 청크 종료 시간 (ISO 8601 형식)

## 사용 방법

### 방법 1: 쉘 스크립트 사용 (권장)

```bash
./reprocess_failed_chunks.sh
```

또는 다른 CSV 파일을 지정:

```bash
./reprocess_failed_chunks.sh my_failed_chunks.csv
```

### 방법 2: Python 직접 실행

```bash
python3 reprocess_failed_chunks.py
```

또는:

```bash
python3 reprocess_failed_chunks.py my_failed_chunks.csv
```

## 처리 과정

1. **CSV 파일 로드**: `post_proc.csv` 파일에서 실패한 청크 목록을 읽습니다.
2. **청크 재처리**: 각 청크에 대해:
   - 대상 테이블 존재 확인 (없으면 생성)
   - narrow format 데이터 추출
   - wide format으로 변환
   - 대상 테이블에 삽입
3. **결과 로깅**: 성공/실패/스킵 통계를 출력합니다.
4. **실패 청크 저장**: 여전히 실패한 청크가 있으면 `failed_chunks_after_reprocess.csv`에 저장합니다.

## 출력 예시

```
================================================================================
🚀 Starting failed chunk reprocessing
================================================================================
📂 Loading failed chunks from post_proc.csv
✅ Loaded 90 failed chunks
📊 Total chunks to reprocess: 90

================================================================================
📦 Processing chunk 1/90
   Ship: IMO9986063
   Range: 2025-06-15 14:40:40.388958 to 2025-06-15 20:40:40.388958
================================================================================
🔄 Reprocessing chunk: IMO9986063 [2025-06-15 14:40:40.388958 to 2025-06-15 20:40:40.388958]
✅ Chunk 1/90 completed: 1234 records
📊 Progress: 1.1% (1/90)
📊 Stats: ✅ 1 | ❌ 0 | ⏭️ 0

...

================================================================================
🏁 Reprocessing completed
================================================================================
📊 Total chunks: 90
✅ Successful: 85
❌ Failed: 3
⏭️ Skipped: 2
📈 Total records processed: 125,467
⏱️ Total time: 450.23 seconds
📊 Success rate: 94.4%
⚡ Average time per chunk: 5.00 seconds
```

## 주요 기능

### 1. 안전한 재처리
- 각 청크를 독립적으로 처리
- 한 청크 실패가 다른 청크에 영향을 주지 않음
- 트랜잭션 단위로 처리

### 2. 상세한 로깅
- 각 청크의 처리 상태 실시간 출력
- 성공/실패/스킵 통계
- 처리된 레코드 수
- 처리 시간

### 3. 실패 추적
- 여전히 실패한 청크를 새 CSV 파일에 저장
- 실패 원인 로깅

### 4. 성능 최적화
- 청크 사이에 0.5초 지연으로 DB 부하 조절
- 배치 삽입으로 성능 향상

## 설정

### 지연 시간 조정

`reprocess_failed_chunks.py`에서 `delay_seconds` 파라미터를 조정할 수 있습니다:

```python
# 지연 시간을 1초로 증가 (DB 부하가 높을 때)
result = reprocessor.reprocess_all(delay_seconds=1.0)

# 지연 시간 없음 (최대 속도)
result = reprocessor.reprocess_all(delay_seconds=0)
```

### 배치 크기

`config.py`의 `batch_size` 설정을 통해 조정:

```python
batch_size: int = 50000  # 한 번에 삽입할 레코드 수
```

## 문제 해결

### CSV 파일을 찾을 수 없음

```
❌ Error: CSV file not found: post_proc.csv
```

해결: `post_proc.csv` 파일이 현재 디렉토리에 있는지 확인하세요.

### 데이터베이스 연결 실패

```
❌ Failed to reprocess chunk: connection already closed
```

해결:
1. 데이터베이스가 실행 중인지 확인
2. `config.py`의 데이터베이스 설정 확인
3. 네트워크 연결 확인
4. 타임아웃 설정 증가 고려

### 테이블 생성 실패

```
❌ Failed to ensure table exists: tenant.imo9986063_wide
```

해결:
1. 데이터베이스 권한 확인
2. `column_list.txt` 파일 존재 확인
3. 스키마(`tenant`) 존재 확인

## 재실행

실패한 청크가 있는 경우:

```bash
# 실패한 청크만 다시 처리
./reprocess_failed_chunks.sh failed_chunks_after_reprocess.csv
```

## 모니터링

### 로그 파일 구조

재처리 스크립트는 기존 마이그레이션 로그(`logs/migration.log`)와 **분리된 전용 로그 파일**을 사용합니다:

- `logs/reprocess_YYYYMMDD_HHMMSS.log`: 타임스탬프가 포함된 개별 로그 파일
- `logs/reprocess_latest.log`: 최신 로그 파일을 가리키는 심볼릭 링크

### 실시간 로그 확인

```bash
# 최신 재처리 로그 보기
tail -f logs/reprocess_latest.log

# 특정 선박의 로그 필터링
tail -f logs/reprocess_latest.log | grep IMO9986063

# 에러만 보기
tail -f logs/reprocess_latest.log | grep ERROR

# 성공만 보기
tail -f logs/reprocess_latest.log | grep "✅"
```

### 로그 파일 목록

```bash
# 모든 재처리 로그 파일 확인
ls -lt logs/reprocess_*.log

# 최신 로그 파일 확인
ls -l logs/reprocess_latest.log
```

### 로그 파일 특징

- **자동 로테이션**: 100MB 이상 시 자동으로 새 파일 생성
- **자동 압축**: 오래된 로그는 자동으로 zip 압축
- **보관 기간**: 30일간 보관 후 자동 삭제
- **타임스탬프**: 각 실행마다 별도의 타임스탬프 로그 파일 생성

## 성능 팁

1. **DB 연결 풀 크기 조정**: `config.py`에서 설정
2. **배치 크기 증가**: 메모리가 충분하면 `batch_size` 증가
3. **병렬 처리**: 여러 프로세스로 나누어 실행 가능
4. **청크 크기 조정**: `config.py`의 `chunk_size_hours` 조정

## 주의사항

1. **중복 데이터**: ON CONFLICT DO NOTHING으로 중복 삽입 방지
2. **트랜잭션**: 각 청크는 독립적인 트랜잭션으로 처리
3. **타임아웃**: 큰 청크는 타임아웃 발생 가능 - chunk_size_hours 감소 고려
4. **리소스**: DB 부하를 고려하여 적절한 delay_seconds 설정

## 지원

문제가 있거나 질문이 있으면 프로젝트 담당자에게 문의하세요.

