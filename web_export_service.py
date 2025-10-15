"""
Wide Table Data Export Web Service
선박별 wide table 데이터를 Excel로 추출하는 웹 서비스 (시트별 분리)
"""
from flask import Flask, render_template, request, send_file, jsonify
from datetime import datetime, timedelta
from pathlib import Path
import csv
import io
import time
import sys
from typing import Dict, List, Any, Optional
from loguru import logger

from database import db_manager
from channel_router import channel_router
from config import web_export_config

# 로깅 설정
logger.remove()
logger.add(
    "logs/web_export.log",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    level="INFO",
    rotation="10 MB"
)
logger.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | {message}",
    level="INFO"
)

app = Flask(__name__)

# Flask 기본 로거 비활성화
import logging
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

# 선박 매핑 (upsert_migration_data.py와 동일)
SHIP_MAPPING = {
    'H2546': 'IMO9976903',
    'H2547': 'IMO9976915',
    'H2548': 'IMO9976927',
    'H2549': 'IMO9976939',
    'H2559': 'IMO9986051',
    'H2560': 'IMO9986087',
}

# 역 매핑 (IMO -> H 코드)
IMO_TO_H_CODE = {v: k for k, v in SHIP_MAPPING.items()}


class DataExporter:
    """Wide table 데이터를 CSV로 추출하는 클래스"""
    
    def __init__(self):
        self.channel_router = channel_router
    
    def get_ship_data_range(self, ship_code: str) -> Dict[str, Any]:
        """
        선박의 데이터 범위 조회 (min/max created_time)
        
        Args:
            ship_code: 선박 코드 (H2546, etc.)
            
        Returns:
            {'min_date': datetime, 'max_date': datetime, 'has_data': bool}
        """
        if ship_code not in SHIP_MAPPING:
            logger.debug(f"Ship code {ship_code} not in SHIP_MAPPING")
            return {'has_data': False}
        
        imo_number = SHIP_MAPPING[ship_code]
        
        # Table 1에서 대표로 조회 (가장 빠름)
        table_name = f"tbl_data_timeseries_{imo_number.lower()}_1"
        logger.debug(f"Checking table: {table_name}")
        
        if not db_manager.check_table_exists(table_name):
            logger.debug(f"Table {table_name} does not exist")
            return {'has_data': False}
        
        try:
            query = f"""
                SELECT 
                    MIN(created_time) as min_date,
                    MAX(created_time) as max_date,
                    COUNT(*) as row_count
                FROM tenant.{table_name}
            """
            
            logger.debug(f"Executing query: {query}")
            result = db_manager.execute_query(query)
            logger.debug(f"Query result: {result}")
            
            if result and len(result) > 0 and result[0]['row_count'] > 0:
                logger.debug(f"Found data for {ship_code}: {result[0]['min_date']} ~ {result[0]['max_date']}, {result[0]['row_count']} rows")
                return {
                    'has_data': True,
                    'min_date': result[0]['min_date'],
                    'max_date': result[0]['max_date'],
                    'row_count': result[0]['row_count']
                }
            else:
                logger.debug(f"No data found for {ship_code} (result: {result})")
                return {'has_data': False}
                
        except Exception as e:
            logger.error(f"Could not get data range for {ship_code}: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return {'has_data': False}
    
    def export_data(self, ship_code: str, start_date: datetime, end_date: datetime, request_id: str = None) -> Dict[str, Any]:
        """
        데이터 추출 (3개 테이블을 created_time 기준으로 병합)
        
        Args:
            ship_code: 선박 코드 (H2546, etc.)
            start_date: 시작 일시
            end_date: 종료 일시
            request_id: 진행상황 추적용 ID
            
        Returns:
            추출 결과 정보
        """
        if ship_code not in SHIP_MAPPING:
            raise ValueError(f"Unknown ship code: {ship_code}")
        
        imo_number = SHIP_MAPPING[ship_code]
        
        logger.info(f"🚀 Starting export for {ship_code} ({imo_number})")
        logger.info(f"   Period: {start_date} ~ {end_date}")
        
        # 3개 테이블에서 데이터 조회
        export_info = {
            'ship_code': ship_code,
            'imo_number': imo_number,
            'start_date': start_date,
            'end_date': end_date,
            'tables': {}
        }
        
        # 진행상황 업데이트: 데이터 조회 시작
        if request_id:
            export_progress[request_id] = {
                'status': 'processing',
                'message': '데이터를 조회하는 중...',
                'progress': 20,
                'start_time': export_progress.get(request_id, {}).get('start_time', time.time())
            }
            logger.info(f"📊 Progress updated: {request_id} - processing (20%)")
        
        # 각 테이블별로 데이터 조회 (병렬 처리)
        table_data = {}  # {table_type: (data, columns)}
        
        # 병렬 조회를 위한 ThreadPoolExecutor
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        def fetch_table(table_type):
            table_name = f"tbl_data_timeseries_{imo_number.lower()}_{table_type}"
            
            # 테이블 존재 확인
            if not db_manager.check_table_exists(table_name):
                return table_type, None, {
                    'name': table_name,
                    'exists': False,
                    'rows': 0,
                    'columns': 0
                }
            
            # 데이터 조회
            data, columns = self.fetch_table_data(table_name, start_date, end_date)
            
            return table_type, (data, columns), {
                'name': table_name,
                'exists': True,
                'rows': len(data),
                'columns': len(columns),
                'column_list': columns[:10]  # 샘플 10개
            }
        
        # 3개 테이블 병렬 조회
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {executor.submit(fetch_table, t): t for t in ['1', '2', '3']}
            
            completed_tables = 0
            for future in as_completed(futures):
                table_type, data_tuple, table_info = future.result()
                
                if data_tuple:
                    table_data[table_type] = data_tuple
                
                export_info['tables'][table_type] = table_info
                completed_tables += 1
                
                # 진행상황 업데이트: 테이블 조회 완료
                if request_id:
                    progress = 20 + (completed_tables * 20)  # 20% -> 40% -> 60%
                    export_progress[request_id] = {
                        'status': 'processing',
                        'message': f'테이블 조회 중... ({completed_tables}/3)',
                        'progress': progress,
                        'start_time': export_progress.get(request_id, {}).get('start_time', time.time())
                    }
                    logger.info(f"📊 Progress updated: {request_id} - processing ({progress}%)")
        
        # 진행상황 업데이트: 병합 시작
        if request_id:
            export_progress[request_id] = {
                'status': 'processing',
                'message': '데이터를 병합하는 중...',
                'progress': 70,
                'start_time': export_progress.get(request_id, {}).get('start_time', time.time())
            }
            logger.info(f"📊 Progress updated: {request_id} - processing (70%)")
        
        # created_time 기준으로 병합
        merged_data, all_columns = self.merge_tables_by_timestamp(table_data)
        
        export_info['total_rows'] = len(merged_data)
        export_info['total_columns'] = len(all_columns)
        
        # 진행상황 업데이트: CSV 생성 시작
        if request_id:
            export_progress[request_id] = {
                'status': 'processing',
                'message': 'CSV 파일을 생성하는 중...',
                'progress': 90,
                'start_time': export_progress.get(request_id, {}).get('start_time', time.time())
            }
            logger.info(f"📊 Progress updated: {request_id} - processing (90%)")
        
        # CSV 파일 생성
        csv_content, file_size = self.create_merged_csv(merged_data, all_columns, ship_code, start_date, end_date)
        
        export_info['file_size'] = file_size
        export_info['csv_content'] = csv_content
        
        return export_info
    
    def fetch_table_data(self, table_name: str, start_date: datetime, end_date: datetime, chunk_size: int = 50000) -> tuple:
        """
        테이블에서 데이터 조회 (COPY 명령 사용으로 최적화)
        
        Returns:
            (data_rows, column_names)
        """
        logger.info(f"   📊 Fetching data from {table_name} using COPY...")
        fetch_start = time.time()
        
        # 컬럼 목록 조회
        col_query = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'tenant'
              AND table_name = %s
            ORDER BY ordinal_position
        """
        
        col_result = db_manager.execute_query(col_query, (table_name,))
        columns = [row['column_name'] for row in col_result] if col_result else []
        
        if not columns:
            logger.warning(f"      ⚠️ No columns found for {table_name}")
            return [], []
        
        logger.info(f"      Found {len(columns)} columns")
        
        # 테이블 통계 업데이트 (성능 최적화)
        try:
            stats_query = f"ANALYZE tenant.{table_name}"
            db_manager.execute_update(stats_query)
            logger.debug(f"      Updated table statistics for query optimization")
        except Exception as e:
            logger.debug(f"      Could not update statistics: {e}")
        
        # COPY 명령으로 데이터 조회 (훨씬 빠름)
        quoted_columns = [f'"{col}"' if col != 'created_time' else col for col in columns]
        columns_str = ', '.join(quoted_columns)
        
        copy_query = f"""
            COPY (
                SELECT {columns_str}
                FROM tenant.{table_name}
                WHERE created_time >= %s
                  AND created_time < %s
                ORDER BY created_time
            ) TO STDOUT WITH CSV HEADER
        """
        
        logger.info(f"      Executing COPY query for period {start_date.date()} ~ {end_date.date()}...")
        
        try:
            # 방법 1: 단일 COPY + 바이너리 압축 (최고 성능)
            conn = db_manager.get_connection()
            
            # 날짜를 PostgreSQL 형식으로 포맷팅
            start_date_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
            end_date_str = end_date.strftime('%Y-%m-%d %H:%M:%S')
            
            logger.info(f"      🔍 Executing single COPY with binary compression...")
            query_start = time.time()
            
            # 바이너리 압축 COPY 쿼리
            copy_query = f"""
                COPY (
                    SELECT {columns_str}
                    FROM tenant.{table_name}
                    WHERE created_time >= '{start_date_str}'
                      AND created_time < '{end_date_str}'
                    ORDER BY created_time
                ) TO STDOUT WITH (FORMAT binary)
            """
            
            # COPY 실행을 위한 커서 생성
            cursor = conn.cursor()
            
            # 바이너리 데이터를 받기 위한 BytesIO
            import io
            copy_buffer = io.BytesIO()
            
            # 바이너리 COPY 실행
            cursor.copy_expert(copy_query, copy_buffer)
            
            query_time = time.time() - query_start
            if query_time > 10:  # 10초 이상 걸리면 경고
                logger.warning(f"      ⚠️ Slow binary COPY detected: {query_time:.2f}s execution time")
            
            # 바이너리 데이터를 파싱하여 딕셔너리 리스트로 변환
            copy_buffer.seek(0)
            binary_data = copy_buffer.getvalue()
            
            logger.info(f"      🔍 Parsing binary COPY data ({len(binary_data):,} bytes)...")
            parse_start = time.time()
            
            all_data = self._parse_binary_copy_data(binary_data, columns)
            
            parse_time = time.time() - parse_start
            logger.info(f"      ✅ Binary parsing completed in {parse_time:.2f}s")
            
            cursor.close()
            db_manager.return_connection(conn)
            
            fetch_time = time.time() - fetch_start
            row_count = len(all_data)
            
            if row_count > 50000:  # 큰 테이블에 대한 특별 로그
                logger.info(f"      ✅ Large table query completed: {row_count:,} rows in {fetch_time:.2f}s")
            
            logger.info(f"      ✅ Fetched {row_count:,} rows from {table_name} in {fetch_time:.2f}s (BINARY COPY)")
            
            return all_data, columns
            
        except Exception as e:
            logger.warning(f"      ⚠️ Binary COPY failed, falling back to SELECT: {e}")
            
            # 연결 정리
            try:
                if 'conn' in locals():
                    conn.rollback()
                    db_manager.return_connection(conn)
            except Exception as cleanup_error:
                logger.error(f"      ⚠️ Error during cleanup: {cleanup_error}")
            
            # Fallback to SELECT with parameterized query
            data_query = f"""
                SELECT {columns_str}
                FROM tenant.{table_name}
                WHERE created_time >= %s
                  AND created_time < %s
                ORDER BY created_time
            """
            
            logger.info(f"      Executing SELECT query for period {start_date.date()} ~ {end_date.date()}...")
            
            # Execute with proper parameter binding and performance monitoring
            select_start = time.time()
            data = db_manager.execute_query(data_query, (start_date, end_date))
            select_time = time.time() - select_start
            
            if select_time > 10:  # 10초 이상 걸리면 경고
                logger.warning(f"      ⚠️ Slow SELECT query detected: {select_time:.2f}s execution time")
            
            fetch_time = time.time() - fetch_start
            row_count = len(data) if data else 0
            logger.info(f"      ✅ Fetched {row_count:,} rows from {table_name} in {fetch_time:.2f}s (SELECT)")
            
            return data if data else [], columns
    
    def _create_date_chunks(self, start_date: datetime, end_date: datetime, chunk_days: int = 30) -> List[tuple]:
        """
        날짜 범위를 청크로 분할
        
        Args:
            start_date: 시작 날짜
            end_date: 종료 날짜
            chunk_days: 청크 크기 (일 단위)
            
        Returns:
            [(chunk_start, chunk_end, chunk_idx), ...]
        """
        chunks = []
        current_start = start_date
        chunk_idx = 1
        
        while current_start < end_date:
            current_end = min(current_start + timedelta(days=chunk_days), end_date)
            chunks.append((current_start, current_end, chunk_idx))
            current_start = current_end
            chunk_idx += 1
        
        return chunks
    
    def _parse_binary_copy_data(self, binary_data: bytes, columns: List[str]) -> List[Dict]:
        """
        PostgreSQL 바이너리 COPY 데이터를 파싱
        
        Args:
            binary_data: 바이너리 COPY 데이터
            columns: 컬럼 목록
            
        Returns:
            딕셔너리 리스트
        """
        try:
            import struct
            
            if len(binary_data) < 11:  # 최소 헤더 크기
                logger.warning("      ⚠️ Binary data too short")
                return []
            
            # PostgreSQL 바이너리 COPY 헤더 확인
            header = b'PGCOPY\n\xff\r\n\x00'
            if binary_data[:11] != header:
                logger.warning("      ⚠️ Invalid binary COPY format")
                return []
            
            logger.debug("      🔍 Parsing PostgreSQL binary COPY format...")
            
            # 바이너리 데이터 파싱 시작
            pos = 11  # 헤더 이후부터 시작
            
            # 플래그 필드 읽기 (4바이트)
            if pos + 4 > len(binary_data):
                return []
            
            flags = struct.unpack('>I', binary_data[pos:pos+4])[0]
            pos += 4
            
            # 확장 헤더 길이 읽기 (4바이트)
            if pos + 4 > len(binary_data):
                return []
            
            header_length = struct.unpack('>I', binary_data[pos:pos+4])[0]
            pos += 4
            
            # 확장 헤더 건너뛰기
            pos += header_length
            
            rows = []
            
            # 행 데이터 파싱
            while pos < len(binary_data):
                # 행의 필드 수 읽기 (2바이트)
                if pos + 2 > len(binary_data):
                    break
                
                field_count = struct.unpack('>H', binary_data[pos:pos+2])[0]
                pos += 2
                
                if field_count == 0xFFFF:  # EOF 마커
                    break
                
                row = {}
                
                # 각 필드 파싱
                for i in range(field_count):
                    if i >= len(columns):
                        break
                    
                    # 필드 길이 읽기 (4바이트)
                    if pos + 4 > len(binary_data):
                        break
                    
                    field_length = struct.unpack('>I', binary_data[pos:pos+4])[0]
                    pos += 4
                    
                    if field_length == 0xFFFFFFFF:  # NULL 값
                        row[columns[i]] = None
                    else:
                        # 필드 데이터 읽기
                        if pos + field_length > len(binary_data):
                            break
                        
                        field_data = binary_data[pos:pos+field_length]
                        pos += field_length
                        
                        # 데이터 타입에 따른 변환
                        try:
                            # 문자열로 디코딩 시도
                            value = field_data.decode('utf-8')
                            
                            # 숫자 타입 변환 시도
                            if '.' in value:
                                try:
                                    value = float(value)
                                except ValueError:
                                    pass
                            else:
                                try:
                                    value = int(value)
                                except ValueError:
                                    pass
                            
                            row[columns[i]] = value
                        except UnicodeDecodeError:
                            # 바이너리 데이터인 경우 문자열로 유지
                            row[columns[i]] = field_data.decode('utf-8', errors='replace')
                
                if len(row) > 0:
                    rows.append(row)
            
            logger.info(f"      ✅ Parsed {len(rows):,} rows from binary data")
            return rows
            
        except Exception as e:
            logger.error(f"      ❌ Error parsing binary COPY data: {e}")
            logger.debug(f"      🔍 Binary data length: {len(binary_data):,} bytes")
            return []
    
    def merge_tables_by_timestamp(self, table_data: Dict[str, tuple]) -> tuple:
        """
        3개 테이블을 created_time 기준으로 병합
        
        Args:
            table_data: {table_type: (data_rows, columns)}
            
        Returns:
            (merged_data_list, all_columns_sorted)
        """
        logger.info(f"   🔄 Merging tables by created_time...")
        merge_start = time.time()
        
        # created_time -> merged_row 매핑
        merged_dict = {}
        
        # 모든 컬럼 수집
        all_columns = set()
        
        for table_type, (data, columns) in table_data.items():
            # created_time 제외한 컬럼들
            data_columns = [col for col in columns if col != 'created_time']
            all_columns.update(data_columns)
            
            logger.info(f"      Merging Table {table_type}: {len(data):,} rows, {len(data_columns)} columns")
            
            # 각 row를 created_time 기준으로 병합
            for row in data:
                created_time = row['created_time']
                
                if created_time not in merged_dict:
                    merged_dict[created_time] = {'created_time': created_time}
                
                # 이 테이블의 컬럼들을 병합
                for col in data_columns:
                    merged_dict[created_time][col] = row.get(col)
        
        # created_time 순서로 정렬
        logger.info(f"      Sorting {len(merged_dict):,} unique timestamps...")
        sorted_timestamps = sorted(merged_dict.keys())
        merged_data = [merged_dict[ts] for ts in sorted_timestamps]
        
        # 컬럼 정렬: created_time + 알파벳 순서
        logger.info(f"      Sorting {len(all_columns):,} columns...")
        sorted_columns = ['created_time'] + sorted(all_columns)
        
        merge_time = time.time() - merge_start
        logger.info(f"   ✅ Merge completed: {len(merged_data):,} rows, {len(sorted_columns):,} columns in {merge_time:.2f}s")
        
        return merged_data, sorted_columns
    
    def create_merged_csv(self, merged_data: List[Dict], all_columns: List[str], 
                          ship_code: str, start_date: datetime, end_date: datetime) -> tuple:
        """
        병합된 데이터로 CSV 파일 생성
        
        Returns:
            (csv_content, file_size)
        """
        if not merged_data:
            logger.warning("   ⚠️ No data to create CSV")
            return None, 0
        
        logger.info(f"   📝 Creating CSV: {len(merged_data):,} rows × {len(all_columns):,} columns")
        csv_start = time.time()
        
        # CSV 생성
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=all_columns, restval='')
        writer.writeheader()
        
        for row in merged_data:
            # created_time 포맷팅
            if 'created_time' in row and isinstance(row['created_time'], datetime):
                row['created_time'] = row['created_time'].strftime('%Y-%m-%d %H:%M:%S')
            writer.writerow(row)
        
        csv_content = output.getvalue()
        file_size = len(csv_content.encode('utf-8'))
        
        csv_time = time.time() - csv_start
        logger.info(f"   ✅ CSV created: {file_size / 1024 / 1024:.2f} MB in {csv_time:.2f}s")
        
        return csv_content, file_size


# Flask 라우트
@app.route('/')
def index():
    """메인 페이지"""
    logger.info(f"📄 Main page accessed from {request.remote_addr}")
    return render_template('index.html', ships=SHIP_MAPPING)


@app.route('/api/ships/data-range')
def get_ships_data_range():
    """모든 선박의 데이터 범위 조회"""
    try:
        exporter = DataExporter()
        ships_info = {}
        
        # 실제 테이블 목록 확인 (디버깅용)
        try:
            table_check_query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'tenant' 
                AND table_name LIKE 'tbl_data_timeseries_%_1'
                ORDER BY table_name
            """
            existing_tables = db_manager.execute_query(table_check_query)
            if existing_tables:
                logger.debug(f"Existing tables: {[t['table_name'] for t in existing_tables]}")
            else:
                logger.warning("No wide tables found in tenant schema")
        except Exception as e:
            logger.error(f"Failed to check existing tables: {e}")
        
        for ship_code, imo_number in SHIP_MAPPING.items():
            logger.debug(f"Checking data range for {ship_code} ({imo_number})")
            data_range = exporter.get_ship_data_range(ship_code)
            
            if data_range['has_data']:
                ships_info[ship_code] = {
                    'imo_number': imo_number,
                    'min_date': data_range['min_date'].strftime('%Y-%m-%d'),
                    'max_date': data_range['max_date'].strftime('%Y-%m-%d'),
                    'row_count': data_range['row_count'],
                    'has_data': True
                }
            else:
                ships_info[ship_code] = {
                    'imo_number': imo_number,
                    'has_data': False
                }
        
        return jsonify(ships_info)
        
    except Exception as e:
        logger.error(f"Failed to get ships data range: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/check-tables')
def check_tables():
    """테이블 생성 상태 확인 (디버깅용)"""
    try:
        # 모든 선박의 테이블 존재 여부 확인
        table_status = {}
        
        for ship_code, imo_number in SHIP_MAPPING.items():
            tables = {}
            for table_type in ['1', '2', '3']:
                table_name = f"tbl_data_timeseries_{imo_number.lower()}_{table_type}"
                exists = db_manager.check_table_exists(table_name)
                tables[table_name] = exists
                
                if exists:
                    # 행 수 확인
                    try:
                        count_query = f"SELECT COUNT(*) as cnt FROM tenant.{table_name}"
                        count_result = db_manager.execute_query(count_query)
                        row_count = count_result[0]['cnt'] if count_result else 0
                        tables[f"{table_name}_rows"] = row_count
                        
                        # MIN/MAX 쿼리도 테스트
                        if row_count > 0:
                            range_query = f"SELECT MIN(created_time) as min_date, MAX(created_time) as max_date FROM tenant.{table_name}"
                            range_result = db_manager.execute_query(range_query)
                            if range_result:
                                tables[f"{table_name}_min_date"] = str(range_result[0]['min_date'])
                                tables[f"{table_name}_max_date"] = str(range_result[0]['max_date'])
                    except Exception as e:
                        tables[f"{table_name}_rows"] = f"Error: {str(e)}"
            
            table_status[ship_code] = {
                'imo_number': imo_number,
                'tables': tables
            }
        
        return jsonify(table_status)
        
    except Exception as e:
        logger.error(f"Failed to check tables: {e}")
        return jsonify({'error': str(e)}), 500


# 전역 진행상황 저장
export_progress = {}
active_exports = set()  # 진행 중인 export 요청 추적

@app.route('/export', methods=['POST'])
def export():
    """데이터 추출"""
    try:
        # 입력값 파싱
        ship_code = request.form.get('ship_code')
        year = int(request.form.get('year'))
        month = int(request.form.get('month'))
        day = int(request.form.get('day'))
        period_type = request.form.get('period_type')  # 'day', 'week', 'month'
        period_value = int(request.form.get('period_value', 1))
        
        # 날짜 계산 (기준일자 = 종료일, 기간을 과거로 계산)
        end_date = datetime(year, month, day, 23, 59, 59)  # 기준일 끝까지
        
        if period_type == 'day':
            start_date = end_date - timedelta(days=period_value)
        elif period_type == 'week':
            start_date = end_date - timedelta(weeks=period_value)
        elif period_type == 'month':
            # 월 계산 (대략적)
            start_date = end_date - timedelta(days=period_value * 30)
        else:
            return jsonify({'error': 'Invalid period type'}), 400
        
        # 시작일은 00:00:00으로
        start_date = start_date.replace(hour=0, minute=0, second=0)
        
        logger.info(f"📥 Export request: {ship_code} ({SHIP_MAPPING[ship_code]}), {start_date.date()} ~ {end_date.date()}")
        
        # 진행상황 초기화
        request_id = f"{ship_code}_{int(time.time())}"
        
        # 중복 요청 방지 - 동일한 ship_code + 날짜 범위 체크
        request_key = f"{ship_code}_{start_date.date()}_{end_date.date()}"
        if request_key in active_exports:
            logger.warning(f"⚠️ Duplicate export request detected: {request_key}")
            return jsonify({
                'error': '이미 동일한 요청이 처리 중입니다. 잠시만 기다려주세요.',
                'request_key': request_key
            }), 409  # Conflict
        
        active_exports.add(request_key)
        
        export_progress[request_id] = {
            'status': 'starting',
            'message': '데이터 추출을 시작합니다...',
            'progress': 10,
            'start_time': time.time(),
            'ship_code': ship_code,
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'request_key': request_key
        }
        
        # 데이터 추출
        exporter = DataExporter()
        start_time = time.time()
        
        result = exporter.export_data(ship_code, start_date, end_date, request_id)
        
        extraction_time = time.time() - start_time
        result['extraction_time'] = f"{extraction_time:.2f}s"
        
        # CSV가 없으면 에러
        if not result['csv_content']:
            export_progress[request_id] = {
                'status': 'error',
                'message': '데이터를 찾을 수 없습니다.',
                'progress': 100,
                'error': 'No data found'
            }
            return jsonify({
                'error': 'No data found',
                'info': result
            }), 404
        
        # 파일명 생성
        start_str = start_date.strftime('%Y%m%d')
        end_str = end_date.strftime('%Y%m%d')
        filename = f"{ship_code}_{start_str}_to_{end_str}.csv"
        
        result['filename'] = filename
        
        # 진행상황 완료
        # request_key 미리 저장
        request_key = None
        if request_id in export_progress and 'request_key' in export_progress[request_id]:
            request_key = export_progress[request_id]['request_key']
        
        export_progress[request_id] = {
            'status': 'completed',
            'message': f'다운로드 완료: {filename}',
            'progress': 100,
            'result': result,
            'request_key': request_key,  # 유지
            'completed_time': time.time(),  # 완료 시간 추가
            'download_ready': True  # 다운로드 준비 완료 플래그
        }
        logger.info(f"📊 Progress updated: {request_id} - completed (100%)")
        
        # active_exports에서 제거 (안전하게 처리)
        if request_key:
            active_exports.discard(request_key)
        
        logger.success(f"✅ Export completed: {filename}, {result['total_rows']:,} rows, {result['file_size']/1024/1024:.2f}MB in {extraction_time:.2f}s")
        
        # CSV 파일로 응답 (Request ID를 헤더에 포함)
        csv_bytes = io.BytesIO(result['csv_content'].encode('utf-8-sig'))  # BOM for Excel
        csv_bytes.seek(0)
        
        response = send_file(
            csv_bytes,
            mimetype='text/csv',
            as_attachment=True,
            download_name=filename
        )
        
        # Request ID를 헤더에 추가 (디버깅용)
        response.headers['X-Request-ID'] = request_id
        
        return response
        
    except Exception as e:
        import traceback
        logger.error(f"❌ Export failed: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        
        # 에러 진행상황 업데이트
        if 'request_id' in locals():
            # request_key 미리 저장
            request_key = None
            if request_id in export_progress and 'request_key' in export_progress[request_id]:
                request_key = export_progress[request_id]['request_key']
            
            export_progress[request_id] = {
                'status': 'error',
                'message': f'오류 발생: {str(e)}',
                'progress': 100,
                'error': str(e),
                'request_key': request_key  # 유지
            }
            
            # active_exports에서 제거 (안전하게 처리)
            if request_key:
                active_exports.discard(request_key)
        
        return jsonify({
            'error': str(e),
            'traceback': traceback.format_exc()
        }), 500


@app.route('/api/export-progress/<request_id>')
def get_export_progress(request_id):
    """Export 진행상황 조회"""
    logger.debug(f"🔍 Progress check for request_id: {request_id}")
    logger.debug(f"📊 Available progress keys: {list(export_progress.keys())}")
    
    if request_id in export_progress:
        progress = export_progress[request_id].copy()
        logger.debug(f"✅ Found progress: {progress['status']}")
        
        # 완료된 요청은 30분 후 삭제 (다운로드 완료 후에도 충분히 유지)
        if progress['status'] in ['completed', 'error']:
            # 완료 시간 기준으로 삭제 (더 정확함)
            completed_time = progress.get('completed_time', progress.get('start_time', 0))
            if time.time() - completed_time > 1800:  # 30분
                logger.debug(f"🗑️ Removing old progress: {request_id}")
                if request_id in export_progress:
                    del export_progress[request_id]
                return jsonify({
                    'status': 'expired',
                    'message': '요청이 만료되었습니다.',
                    'progress': 100
                }), 410  # Gone
        
        # 완료된 경우 추가 정보 제공
        if progress['status'] == 'completed':
            progress['download_status'] = 'ready'
            progress['can_download'] = True
        
        return jsonify(progress)
    else:
        logger.warning(f"❌ Progress not found for request_id: {request_id}")
        return jsonify({
            'status': 'not_found',
            'message': '요청을 찾을 수 없습니다.',
            'progress': 0,
            'available_keys': list(export_progress.keys())
        }), 404


@app.route('/preview', methods=['POST'])
def preview():
    """미리보기 (다운로드 전 정보 확인)"""
    try:
        # 입력값 파싱
        ship_code = request.form.get('ship_code')
        year = int(request.form.get('year'))
        month = int(request.form.get('month'))
        day = int(request.form.get('day'))
        period_type = request.form.get('period_type')
        period_value = int(request.form.get('period_value', 1))
        
        # 날짜 계산 (기준일자 = 종료일, 기간을 과거로 계산)
        end_date = datetime(year, month, day, 23, 59, 59)  # 기준일 끝까지
        
        if period_type == 'day':
            start_date = end_date - timedelta(days=period_value)
        elif period_type == 'week':
            start_date = end_date - timedelta(weeks=period_value)
        elif period_type == 'month':
            start_date = end_date - timedelta(days=period_value * 30)
        else:
            return jsonify({'error': 'Invalid period type'}), 400
        
        # 시작일은 00:00:00으로
        start_date = start_date.replace(hour=0, minute=0, second=0)
        
        logger.info(f"🔍 Preview request: {ship_code} ({SHIP_MAPPING[ship_code]}), {start_date.date()} ~ {end_date.date()}")
        
        # 데이터 추출 정보만 (CSV 생성 안 함)
        exporter = DataExporter()
        start_time = time.time()
        
        result = exporter.export_data(ship_code, start_date, end_date)
        
        extraction_time = time.time() - start_time
        
        # 파일명 생성
        start_str = start_date.strftime('%Y%m%d')
        end_str = end_date.strftime('%Y%m%d')
        filename = f"{ship_code}_{start_str}_to_{end_str}.csv"
        
        # 응답 데이터
        file_size_mb = f"{result['file_size'] / 1024 / 1024:.2f} MB"
        
        response = {
            'filename': filename,
            'ship_code': ship_code,
            'imo_number': result['imo_number'],
            'period': f"{start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}",
            'total_rows': result['total_rows'],
            'total_columns': result['total_columns'],
            'file_size_mb': file_size_mb,
            'extraction_time': f"{extraction_time:.2f}s",
            'tables': result['tables']
        }
        
        logger.info(f"✅ Preview completed: {result['total_rows']:,} rows, {file_size_mb}, {extraction_time:.2f}s")
        
        return jsonify(response)
        
    except Exception as e:
        import traceback
        logger.error(f"❌ Preview failed: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return jsonify({
            'error': str(e),
            'traceback': traceback.format_exc()
        }), 500


if __name__ == '__main__':
    logger.info(f"🌐 Starting Wide Table Data Export Web Service")
    logger.info(f"📊 Host: {web_export_config.host}")
    logger.info(f"📊 Port: {web_export_config.port}")
    logger.info(f"📊 Debug: {web_export_config.debug}")
    logger.info(f"🚀 Service starting...")
    app.run(
        debug=web_export_config.debug,
        host=web_export_config.host,
        port=web_export_config.port
    )

