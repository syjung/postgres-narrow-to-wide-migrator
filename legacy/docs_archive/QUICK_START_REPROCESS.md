# 실패한 청크 재처리 - 빠른 시작 가이드

## 📋 개요

패러럴 배치 마이그레이션 중 connection timeout 등으로 실패한 청크들을 재처리하는 도구입니다.

## 🚀 빠른 시작

### 1. Dry-run으로 미리 확인 (권장)

```bash
# 전체 청크 확인
./reprocess_failed_chunks.sh --dry-run

# 또는 Python으로
python3 reprocess_failed_chunks.py --dry-run
```

### 2. 특정 선박만 테스트

```bash
# Dry-run으로 특정 선박 확인
./reprocess_failed_chunks.sh --dry-run --ship IMO9986063

# 실제 처리
./reprocess_failed_chunks.sh --ship IMO9986063
```

### 3. 전체 재처리 실행

```bash
# 쉘 스크립트로 실행 (권장)
./reprocess_failed_chunks.sh

# 또는 Python으로 직접 실행
python3 reprocess_failed_chunks.py
```

## 📊 사용 예제

### 예제 1: 기본 실행
```bash
./reprocess_failed_chunks.sh
```

### 예제 2: 특정 선박만 처리
```bash
./reprocess_failed_chunks.sh --ship IMO9986063
```

### 예제 3: 지연 시간 조정 (DB 부하가 높을 때)
```bash
./reprocess_failed_chunks.sh -d 1.0
```

### 예제 4: 다른 CSV 파일 사용
```bash
./reprocess_failed_chunks.sh my_failed_chunks.csv
```

### 예제 5: 조합 사용
```bash
./reprocess_failed_chunks.sh --ship IMO9986063 -d 1.0 --dry-run
```

## 📁 파일 구조

```
post_proc.csv                          # 입력: 실패한 청크 목록
reprocess_failed_chunks.py             # 재처리 Python 스크립트
reprocess_failed_chunks.sh             # 실행용 쉘 스크립트
failed_chunks_after_reprocess.csv      # 출력: 여전히 실패한 청크 (생성 시)
logs/reprocess_YYYYMMDD_HHMMSS.log     # 재처리 전용 로그 파일
logs/reprocess_latest.log              # 최신 로그 (심볼릭 링크)
```

**중요**: 재처리 로그는 기존 `logs/migration.log`와 **분리**되어 저장됩니다.

## 📝 CSV 파일 형식

```csv
IMO9986063,2025-06-15 14:40:40.388958,2025-06-15 20:40:40.388958
IMO9986104,2025-07-16 02:40:40.394745,2025-07-16 08:40:40.394745
```

- 첫 번째 컬럼: 선박 ID
- 두 번째 컬럼: 청크 시작 시간 (ISO 8601 형식)
- 세 번째 컬럼: 청크 종료 시간 (ISO 8601 형식)

## 🎯 추천 워크플로우

### 단계 1: 확인
```bash
# Dry-run으로 무엇이 처리될지 확인
./reprocess_failed_chunks.sh --dry-run
```

### 단계 2: 테스트
```bash
# 한 선박만 먼저 테스트
./reprocess_failed_chunks.sh --ship IMO9986063
```

### 단계 3: 전체 실행
```bash
# 모든 실패한 청크 재처리
./reprocess_failed_chunks.sh
```

### 단계 4: 재시도 (필요시)
```bash
# 여전히 실패한 청크가 있다면
./reprocess_failed_chunks.sh failed_chunks_after_reprocess.csv
```

## ⚙️ 주요 옵션

| 옵션 | 설명 | 예제 |
|------|------|------|
| `--dry-run` | 실제 처리 없이 확인만 | `--dry-run` |
| `--ship SHIP_ID` | 특정 선박만 처리 | `--ship IMO9986063` |
| `-d, --delay N` | 청크 사이 지연 시간 (초) | `-d 1.0` |
| `-h, --help` | 도움말 표시 | `-h` |

## 📊 출력 예시

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
✅ Chunk 1/90 completed: 12,345 records
📊 Progress: 1.1% (1/90)
📊 Stats: ✅ 1 | ❌ 0 | ⏭️ 0

...

================================================================================
🏁 Reprocessing completed
================================================================================
📊 Total chunks: 90
✅ Successful: 87
❌ Failed: 2
⏭️ Skipped: 1
📈 Total records processed: 1,234,567
⏱️ Total time: 450.23 seconds
📊 Success rate: 96.7%
⚡ Average time per chunk: 5.00 seconds
```

## ❓ 문제 해결

### CSV 파일을 찾을 수 없음
```
❌ Error: CSV file not found: post_proc.csv
```
**해결**: 파일이 현재 디렉토리에 있는지 확인하세요.

### 데이터베이스 연결 실패
```
❌ Failed to reprocess chunk: connection already closed
```
**해결**: 
- 데이터베이스가 실행 중인지 확인
- `config.py`의 데이터베이스 설정 확인
- 지연 시간 증가: `./reprocess_failed_chunks.sh -d 2.0`

### 실패한 청크가 계속 발생
**해결**:
1. 로그 확인: `tail -f logs/migration.log`
2. 지연 시간 증가
3. DB 부하 확인
4. chunk_size_hours 감소 고려

## 💡 팁

1. **항상 dry-run부터**: 실제 처리 전에 dry-run으로 확인하세요
2. **한 선박씩 테스트**: 전체 실행 전에 한 선박만 먼저 테스트
3. **DB 부하 조절**: 지연 시간을 적절히 조정하세요
4. **로그 모니터링**: 별도 터미널에서 로그를 실시간으로 확인
   ```bash
   tail -f logs/reprocess_latest.log
   ```
5. **재시도 전략**: 실패한 청크는 CSV로 저장되므로 쉽게 재시도 가능

## 📝 로그 확인

재처리 로그는 **별도의 파일**에 저장됩니다:

```bash
# 실시간 로그 확인
tail -f logs/reprocess_latest.log

# 특정 선박 필터링
tail -f logs/reprocess_latest.log | grep IMO9986063

# 모든 재처리 로그 목록
ls -lt logs/reprocess_*.log
```

## 📚 상세 문서

더 자세한 정보는 [REPROCESS_GUIDE.md](./REPROCESS_GUIDE.md)를 참고하세요.

## 🆘 지원

문제가 있거나 질문이 있으면 프로젝트 담당자에게 문의하세요.

