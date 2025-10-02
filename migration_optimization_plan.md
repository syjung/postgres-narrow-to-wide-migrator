# PostgreSQL 대용량 데이터 마이그레이션 최적화 방안

## 현재 문제점
- 800만+ 레코드를 개별 INSERT로 처리
- 배치 크기: 10,000 (기본값)
- 매 배치마다 트랜잭션 커밋
- 인덱스가 활성화된 상태에서 INSERT

## 최적화 방안

### 1. PostgreSQL COPY 명령어 사용
```sql
COPY table_name FROM '/path/to/file.csv' WITH CSV HEADER;
```
- 가장 빠른 대량 삽입 방법
- 네트워크 오버헤드 최소화
- 단일 트랜잭션으로 처리

### 2. 배치 크기 최적화
- 현재: 10,000 → 권장: 50,000-100,000
- 메모리 사용량과 성능의 균형점

### 3. 인덱스 비활성화
```sql
-- 마이그레이션 전
DROP INDEX IF EXISTS idx_name;

-- 마이그레이션 후
CREATE INDEX idx_name ON table_name (column_name);
```

### 4. 병렬 처리
- 여러 프로세스로 동시 마이그레이션
- 시간 범위별로 분할 처리

### 5. 트랜잭션 최적화
- 큰 배치 단위로 커밋
- WAL(Write-Ahead Log) 설정 최적화

### 6. 메모리 기반 처리
- pandas를 활용한 메모리 내 변환
- numpy 배열로 벡터화된 연산

## 구현 우선순위
1. **COPY 명령어 구현** (가장 효과적)
2. **배치 크기 증가**
3. **인덱스 비활성화**
4. **병렬 처리**
