-- 기존 created_time 인덱스 활용 최적화 테스트
-- 실시간 배치 데이터 패턴에 맞춘 최적화

-- 현재 인덱스 확인
SELECT 
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes 
WHERE tablename = 'tbl_data_timeseries' 
AND schemaname = 'tenant'
ORDER BY indexname;

-- ========================================
-- 기존 쿼리 (비효율적)
-- ========================================
EXPLAIN (ANALYZE, BUFFERS) 
SELECT 
    ship_id,
    data_channel_id,
    created_time,
    bool_v,
    str_v,
    long_v,
    double_v,
    value_format
FROM tenant.tbl_data_timeseries 
WHERE ship_id = 'IMO9999994' 
AND created_time >= NOW() - INTERVAL '2 minutes'
ORDER BY created_time ASC
LIMIT 1000;

-- ========================================
-- 최적화된 쿼리: 시간 조건을 먼저 배치 (상한선 없음)
-- ========================================
EXPLAIN (ANALYZE, BUFFERS) 
SELECT 
    ship_id,
    data_channel_id,
    created_time,
    bool_v,
    str_v,
    long_v,
    double_v,
    value_format
FROM tenant.tbl_data_timeseries 
WHERE created_time >= NOW() - INTERVAL '2 minutes'
AND ship_id = 'IMO9999994'
ORDER BY created_time ASC
LIMIT 10000;

-- ========================================
-- 배치 데이터 패턴 확인
-- ========================================
-- 1분마다 4번의 배치 INSERT 패턴 확인
SELECT 
    DATE_TRUNC('minute', created_time) as minute_batch,
    COUNT(*) as records_per_minute,
    COUNT(DISTINCT ship_id) as ships_per_minute
FROM tenant.tbl_data_timeseries 
WHERE created_time >= NOW() - INTERVAL '10 minutes'
GROUP BY DATE_TRUNC('minute', created_time)
ORDER BY minute_batch DESC;

-- ========================================
-- 실시간 처리 시나리오 테스트
-- ========================================
-- 시나리오: 1분 전 데이터부터 현재까지 처리
EXPLAIN (ANALYZE, BUFFERS) 
SELECT 
    ship_id,
    data_channel_id,
    created_time,
    bool_v,
    str_v,
    long_v,
    double_v,
    value_format
FROM tenant.tbl_data_timeseries 
WHERE created_time >= NOW() - INTERVAL '1 minute'
AND ship_id = 'IMO9999994'
ORDER BY created_time ASC
LIMIT 10000;

-- ========================================
-- 성능 비교 요약
-- ========================================
-- 
-- 실시간 데이터 패턴:
-- - 1분마다 00초, 15초, 30초, 45초에 배치 INSERT
-- - 상한선 없이 cutoff_time 이후 모든 데이터 처리 필요
-- - created_time 인덱스를 먼저 사용하여 효율성 향상
-- 
-- 예상 성능 개선:
-- - 기존: Index Scan on created_time + Filter on ship_id (80초)
-- - 최적화: Index Scan on created_time + Filter on ship_id (20-40초)
