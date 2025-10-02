# 올바른 마이그레이션 실행 가이드

## 🚨 중요: 마이그레이션 순서

**절대 실시간 처리를 먼저 시작하지 마세요!** 올바른 순서를 반드시 지켜야 합니다.

## 📋 권장 마이그레이션 순서

### **방법 1: 동시 처리 전략 (Concurrent Strategy) - 권장**

```bash
# 1단계: 스키마 분석
python main.py --mode schema-only

# 2단계: 테이블 생성
python main.py --mode tables-only

# 3단계: 동시 처리 시작 (실시간 + 백필)
python main.py --mode concurrent --interval 1

# 4단계: 상태 확인
python main.py --mode status
```

### **방법 2: 하이브리드 전략 (Hybrid Strategy)**

```bash
# 1단계: 스키마 분석
python main.py --mode schema-only

# 2단계: 테이블 생성
python main.py --mode tables-only

# 3단계: 하이브리드 전략 시작
python main.py --mode hybrid --interval 1

# 4단계: 상태 확인
python main.py --mode status
```

### **방법 3: 스트리밍 전략 (Streaming Strategy)**

```bash
# 1단계: 스키마 분석
python main.py --mode schema-only

# 2단계: 테이블 생성
python main.py --mode tables-only

# 3단계: 스트리밍 전략 시작
python main.py --mode streaming --interval 1

# 4단계: 상태 확인
python main.py --mode status
```

### **방법 4: 안전한 순차 마이그레이션 (전통적 방법)**

```bash
# 1단계: 스키마 분석
python main.py --mode schema-only

# 2단계: 테이블 생성
python main.py --mode tables-only

# 3단계: 기존 데이터 마이그레이션 (cutoff_time 설정)
python main.py --mode migration-only --cutoff-time "2024-10-02 17:00:00"

# 4단계: 실시간 처리 시작 (cutoff_time 이후 데이터만 처리)
python main.py --mode realtime
```

## ⚠️ 각 단계별 주의사항

### **1단계: 스키마 분석**
- **목적**: 기존 데이터를 분석하여 wide table 스키마 생성
- **시간**: 약 1-2분
- **주의**: 데이터베이스 연결만 필요

```bash
python main.py --mode schema-only
```

### **2단계: 테이블 생성**
- **목적**: 분석된 스키마로 wide table 생성
- **시간**: 약 30초
- **주의**: 테이블 생성 권한 필요

```bash
python main.py --mode tables-only
```

### **3단계: 동시 처리 시작 (Concurrent Strategy)**
- **목적**: 실시간 데이터 처리와 기존 데이터 백필을 동시에 실행
- **시간**: 지속적 실행 (백필 완료까지)
- **주의**: **가장 효율적인 방법!** 실시간 데이터 수집 중단 없음

```bash
python main.py --mode concurrent --interval 1
```

### **3단계: 하이브리드 전략 (Hybrid Strategy)**
- **목적**: 동시 처리의 고도화 버전 (더 정교한 모니터링)
- **시간**: 지속적 실행
- **주의**: 고급 사용자용, 더 정교한 진행률 추적

```bash
python main.py --mode hybrid --interval 1
```

### **3단계: 스트리밍 전략 (Streaming Strategy)**
- **목적**: 모든 데이터를 스트리밍으로 처리
- **시간**: 지속적 실행
- **주의**: 간단한 구조, 복잡한 백필 로직 없음

```bash
python main.py --mode streaming --interval 1
```

## 🚀 새로운 기능

### **동시 처리 전략 (Concurrent Strategy)**
- **실시간 처리**: 새로운 데이터를 즉시 처리
- **백그라운드 백필**: 기존 데이터를 백그라운드에서 청크 단위로 처리
- **멀티스레딩**: 실시간과 백필을 동시에 실행
- **Zero Downtime**: 실시간 데이터 수집 중단 없이 마이그레이션

### **청크 기반 마이그레이션**
- **24시간 청크**: 대용량 데이터를 24시간 단위로 분할 처리
- **메모리 효율성**: 메모리 부족 문제 해결
- **장애 복구**: 실패한 청크만 재처리 가능
- **진행률 추적**: 청크별 진행상황 실시간 모니터링

### **cutoff_time 영구 저장**
- **자동 저장**: 마이그레이션 완료 시점을 파일에 저장
- **자동 로드**: 프로세스 재시작 시 자동으로 cutoff_time 복구
- **데이터 중복 방지**: cutoff_time 이후 데이터만 실시간 처리
- **일관성 보장**: 마이그레이션과 실시간 처리 간 데이터 중복 방지

## 🔧 고급 옵션

### **특정 선박만 처리**
```bash
# 특정 선박의 데이터만 마이그레이션
python main.py --mode migration-only --ship-id IMO9999993 --cutoff-time "2024-10-02 17:00:00"
```

### **기존 테이블 재생성**
```bash
# 기존 테이블 삭제 후 재생성
python main.py --mode tables-only --drop-tables
```

### **실시간 처리 간격 조정**
```bash
# 5분 간격으로 실시간 처리
python main.py --mode realtime --interval 5
```

## 📊 진행상황 모니터링

### **현재 상태 확인**
```bash
python main.py --mode status
```

### **마이그레이션 통계 확인**
```python
from data_migrator import data_migrator
stats = data_migrator.get_migration_statistics()
print(stats)
```

## 🚨 문제 해결

### **데이터 중복 문제**
- **원인**: 실시간 처리와 마이그레이션을 동시에 실행
- **해결**: 마이그레이션 완료 후 실시간 처리 시작

### **메모리 부족 문제**
- **원인**: 대용량 데이터 처리 시 메모리 부족
- **해결**: 배치 크기 조정 (config.py에서 batch_size 수정)

### **데이터베이스 연결 문제**
- **원인**: 네트워크 불안정 또는 권한 문제
- **해결**: 연결 정보 확인 및 권한 확인

## ✅ 성공 확인

### **마이그레이션 성공 확인**
```python
from data_migrator import data_migrator

# 각 선박별 상태 확인
for ship_id in ["IMO9999993", "IMO9999994"]:
    status = data_migrator.get_migration_status(ship_id)
    print(f"{ship_id}: {status['status']} - {status['record_count']} records")
```

### **데이터 일관성 검증**
```python
from data_migrator import data_migrator

# 각 선박별 검증
for ship_id in ["IMO9999993", "IMO9999994"]:
    validation = data_migrator.validate_migration(ship_id)
    print(f"{ship_id}: {validation['status']} - {validation['message']}")
```

## 🎯 최종 체크리스트

- [ ] 스키마 분석 완료
- [ ] 테이블 생성 완료
- [ ] 데이터 마이그레이션 완료 (cutoff_time 설정)
- [ ] 실시간 처리 시작
- [ ] 데이터 일관성 검증 완료
- [ ] 성능 모니터링 설정

---

**⚠️ 중요**: 이 순서를 반드시 지켜야 데이터 손실이나 중복 없이 안전하게 마이그레이션할 수 있습니다!
