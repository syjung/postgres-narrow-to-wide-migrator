# Connection Timeout 근본 원인 분석 및 해결 방안

**작성일**: 2025-10-13  
**분석 대상**: a.log (90건의 "connection already closed" 에러)

---

## 📊 에러 패턴 분석

### 실행 시간 통계
- **최소**: 950.27초 (15.8분)
- **최대**: 8159.23초 (2.27시간)
- **평균**: 7963.72초 (2.21시간)
- **중간값**: 8122.89초

### 시간대별 분포
- 2시간(7200초) 이상: **89건 (98.9%)**
- 2.2시간(7900초) 이상: **88건 (97.8%)**
- 정확히 8126.46초: **42건 (46.7%)** ⚠️

### 에러 메시지
```
Query failed after 8126.46 seconds: connection already closed
Failed to migrate chunk [start_time] to [end_time]: connection already closed
```

---

## 🎯 근본 원인 (Root Cause)

### 1. PostgreSQL 서버 측 타임아웃 ⭐⭐⭐⭐⭐
**증거**:
- 42건(46.7%)이 정확히 **8126.46초**(2시간 15분 26초)에서 실패
- 이는 서버에 설정된 특정 타임아웃의 명백한 증거

**가능한 타임아웃 설정**:
- `statement_timeout`: 단일 SQL 문 실행 제한 시간
- `idle_in_transaction_session_timeout`: 트랜잭션 유휴 시간 제한
- `tcp_keepalives_idle/interval/count`: TCP 연결 유지 설정

### 2. 대용량 데이터 처리 시간 ⭐⭐⭐⭐
**현재 상황**:
- `chunk_size_hours`: 6시간 → 평균 2.21시간 소요
- narrow 테이블에서 대량의 데이터를 읽고 변환하는 과정이 매우 느림
- `ORDER BY created_time`으로 인한 추가 성능 저하
- 93개 `data_channel_id`에 대한 IN 절 처리

### 3. 네트워크 연결 유지 문제 ⭐⭐⭐
**현재 코드**:
- psycopg2 연결 시 keepalive 설정 없음
- 장시간 쿼리 실행 중 TCP 연결이 끊어질 수 있음
- 로드 밸런서나 방화벽의 idle timeout
- Azure PostgreSQL의 기본 연결 제한

---

## 💡 해결 방안

### 방안 1: 청크 크기 축소 ⭐⭐⭐⭐⭐
**가장 효과적이고 즉시 적용 가능**

**현재 상황**:
```python
# config.py
chunk_size_hours: int = 6  # 6시간 → 평균 2.21시간 소요 → 타임아웃
```

**해결책**:
```python
# config.py
chunk_size_hours: int = 2  # 6시간 → 2시간으로 축소
# 또는
chunk_size_hours: int = 1  # 더 안전하게 1시간
```

**장점**:
- ✅ 가장 안전하고 확실한 방법
- ✅ 코드 수정이 간단 (config.py 한 줄)
- ✅ 타임아웃 발생 전에 완료
- ✅ 실패 시 재처리 범위 축소 (복구 빠름)
- ✅ 메모리 사용량도 감소

**단점**:
- ⚠️ 전체 처리 시간이 약간 증가할 수 있음 (오버헤드)
- ⚠️ 더 많은 청크 처리 필요

**예상 효과**:
- 청크당 처리 시간: 2.21시간 → 40~50분 이내
- 타임아웃 여유: 1.5시간 이상

---

### 방안 2: Connection Keepalive 설정 ⭐⭐⭐⭐

**현재 상황**:
```python
# config.py
@property
def connection_string(self) -> str:
    return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
```

**해결책**:
```python
# config.py
@property
def connection_string(self) -> str:
    return (
        f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        f"?keepalives=1"
        f"&keepalives_idle=30"      # 30초마다 keepalive 확인
        f"&keepalives_interval=10"   # 10초 간격으로 재시도
        f"&keepalives_count=5"       # 5번 실패 시 연결 끊김
    )
```

**장점**:
- ✅ 장시간 쿼리에서도 연결 유지
- ✅ 네트워크 문제 조기 감지
- ✅ Azure PostgreSQL과 호환

**단점**:
- ⚠️ 근본 원인(쿼리 시간)을 해결하지는 않음
- ⚠️ Azure PostgreSQL에서 효과가 제한적일 수 있음

---

### 방안 3: 서버 측 타임아웃 증가 ⭐⭐⭐
**DBA 협업 필요**

**현재 설정 확인**:
```sql
-- PostgreSQL에서 실행
SHOW statement_timeout;
SHOW idle_in_transaction_session_timeout;
```

**타임아웃 증가**:
```sql
-- 세션별 설정 (재처리 스크립트에 추가)
SET statement_timeout = '4h';
SET idle_in_transaction_session_timeout = '4h';

-- 또는 데이터베이스 전체 설정 (DBA 권한 필요)
ALTER DATABASE tenant_builder SET statement_timeout = '4h';
```

**장점**:
- ✅ 장시간 쿼리 실행 가능

**단점**:
- ⚠️ DBA 권한 필요
- ⚠️ 다른 쿼리에도 영향 (전역 설정 시)
- ⚠️ Azure PostgreSQL은 제한적일 수 있음
- ⚠️ 근본 문제(쿼리 성능) 미해결

---

### 방안 4: 쿼리 최적화 ⭐⭐⭐

**인덱스 최적화**:
```sql
-- 마이그레이션 쿼리에 최적화된 인덱스
CREATE INDEX IF NOT EXISTS idx_timeseries_migration 
ON tenant.tbl_data_timeseries(ship_id, created_time, data_channel_id);
```

**쿼리 개선**:
- ORDER BY 제거 (필요한 경우만 사용)
- LIMIT 추가로 메모리 제어
- 병렬 쿼리 활용

**장점**:
- ✅ 쿼리 실행 시간 단축
- ✅ 근본적인 성능 개선

**단점**:
- ⚠️ 인덱스 생성 시간 소요
- ⚠️ 인덱스로 인한 INSERT 성능 저하 가능
- ⚠️ 효과가 제한적일 수 있음

---

### 방안 5: 재시도 로직 강화 ⭐⭐

**자동 재시도 메커니즘**:
- Exponential backoff
- 타임아웃 발생 시 청크 분할
- 부분 성공 처리

**장점**:
- ✅ 일시적 네트워크 문제 대응
- ✅ 자동 복구

**단점**:
- ⚠️ 복잡도 증가
- ⚠️ 근본 원인 미해결

---

## 🎯 권장 해결 방안

### 최우선 (즉시 적용): 방안 1 + 방안 2

#### 1단계: chunk_size_hours 축소
```python
# config.py
chunk_size_hours: int = 2  # 6 → 2로 축소
```

#### 2단계: Connection Keepalive 추가
```python
# config.py
@property
def connection_string(self) -> str:
    return (
        f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        f"?keepalives=1"
        f"&keepalives_idle=30"
        f"&keepalives_interval=10"
        f"&keepalives_count=5"
    )
```

#### 예상 효과:
- ✅ 청크당 처리 시간: 2시간 → 40분 이내
- ✅ 타임아웃 발생 전에 완료
- ✅ 연결 안정성 향상
- ✅ 90건의 실패 대부분 해결

---

## 📋 실행 계획

### 1. 즉시 조치 (코드 수정 없이 테스트)
- [ ] DBA에게 요청:
  - `SHOW statement_timeout;` 실행
  - `SHOW idle_in_transaction_session_timeout;` 실행
  - 현재 설정값 확인

- [ ] 임시 재처리 테스트:
  - post_proc.csv를 2~3개 청크로 분할
  - 작은 규모로 먼저 테스트

### 2. 단기 조치 (1-2일)
- [ ] `config.py` 수정:
  - `chunk_size_hours: 6 → 2`
  - connection_string에 keepalives 추가

- [ ] 테스트 실행:
  - 1개 선박으로 검증
  - 로그 확인

- [ ] 전체 재처리:
  - `./reprocess_failed_chunks.sh`

### 3. 중장기 조치 (1-2주)
- [ ] 쿼리 성능 분석
- [ ] 인덱스 최적화
- [ ] 자동 재시도 로직 추가

---

## 📊 예상 효과

### 현재 (chunk_size_hours: 6)
- 청크당 처리 시간: 평균 2.21시간
- 타임아웃 발생: 8126초 (2.26시간)
- 실패율: 매우 높음 (90건 전부 실패)
- 재처리 범위: 6시간치 데이터

### 방안 적용 후 (chunk_size_hours: 2)
- 청크당 처리 시간: 예상 40~50분
- 타임아웃 여유: 충분 (1.5시간 여유)
- 실패율: 대폭 감소 예상
- 재처리 범위: 2시간치로 축소

---

## ✨ 결론

**가장 효과적이고 빠른 해결책**:

1. `chunk_size_hours`를 6 → 2로 축소 (config.py 한 줄 수정)
2. Connection keepalives 추가 (config.py connection_string 수정)
3. 테스트 후 전체 재처리

이 조합으로 **90건의 실패를 대부분 해결**할 수 있을 것으로 예상됩니다.

---

## 📚 참고 자료

- [PostgreSQL Statement Timeout](https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-STATEMENT-TIMEOUT)
- [psycopg2 Connection Parameters](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-KEEPALIVES)
- [Azure PostgreSQL Timeouts](https://learn.microsoft.com/en-us/azure/postgresql/)

