"""
Wide Table Data Export Web Service
선박별 wide table 데이터를 CSV로 추출하는 웹 서비스
"""
from flask import Flask, render_template, request, send_file, jsonify
from datetime import datetime, timedelta
from pathlib import Path
import csv
import io
import time
from typing import Dict, List, Any, Optional

from database import db_manager
from channel_router import channel_router

app = Flask(__name__)

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
    
    def export_data(self, ship_code: str, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """
        데이터 추출
        
        Args:
            ship_code: 선박 코드 (H2546, etc.)
            start_date: 시작 일시
            end_date: 종료 일시
            
        Returns:
            추출 결과 정보
        """
        if ship_code not in SHIP_MAPPING:
            raise ValueError(f"Unknown ship code: {ship_code}")
        
        imo_number = SHIP_MAPPING[ship_code]
        
        # 3개 테이블에서 데이터 조회
        export_info = {
            'ship_code': ship_code,
            'imo_number': imo_number,
            'start_date': start_date,
            'end_date': end_date,
            'tables': {}
        }
        
        total_rows = 0
        total_columns = 0
        all_data = []  # 통합 데이터
        
        for table_type in ['1', '2', '3']:
            table_name = f"tbl_data_timeseries_{imo_number.lower()}_{table_type}"
            
            # 테이블 존재 확인
            if not db_manager.check_table_exists(table_name):
                export_info['tables'][table_type] = {
                    'name': table_name,
                    'exists': False,
                    'rows': 0,
                    'columns': 0
                }
                continue
            
            # 데이터 조회
            data, columns = self.fetch_table_data(table_name, start_date, end_date)
            
            export_info['tables'][table_type] = {
                'name': table_name,
                'exists': True,
                'rows': len(data),
                'columns': len(columns),
                'column_list': columns[:10]  # 샘플 10개
            }
            
            total_rows += len(data)
            total_columns += len(columns)
            all_data.extend(data)
        
        export_info['total_rows'] = total_rows
        export_info['total_columns'] = total_columns
        
        # CSV 파일 생성
        csv_content, file_size = self.create_csv(all_data, ship_code, start_date, end_date)
        
        export_info['file_size'] = file_size
        export_info['csv_content'] = csv_content
        
        return export_info
    
    def fetch_table_data(self, table_name: str, start_date: datetime, end_date: datetime) -> tuple:
        """
        테이블에서 데이터 조회
        
        Returns:
            (data_rows, column_names)
        """
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
            return [], []
        
        # 데이터 조회
        quoted_columns = [f'"{col}"' if col != 'created_time' else col for col in columns]
        columns_str = ', '.join(quoted_columns)
        
        data_query = f"""
            SELECT {columns_str}
            FROM tenant.{table_name}
            WHERE created_time >= %s
              AND created_time < %s
            ORDER BY created_time
        """
        
        data = db_manager.execute_query(data_query, (start_date, end_date))
        
        return data if data else [], columns
    
    def create_csv(self, all_data: List[Dict], ship_code: str, start_date: datetime, 
                   end_date: datetime) -> tuple:
        """
        CSV 파일 생성
        
        Returns:
            (csv_content, file_size)
        """
        if not all_data:
            return None, 0
        
        # 모든 컬럼 수집 (created_time + 모든 채널)
        all_columns = set()
        for row in all_data:
            all_columns.update(row.keys())
        
        # created_time을 맨 앞으로
        all_columns.discard('created_time')
        sorted_columns = ['created_time'] + sorted(all_columns)
        
        # CSV 생성
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=sorted_columns)
        writer.writeheader()
        
        for row in all_data:
            # created_time 포맷팅
            if 'created_time' in row and isinstance(row['created_time'], datetime):
                row['created_time'] = row['created_time'].strftime('%Y-%m-%d %H:%M:%S')
            writer.writerow(row)
        
        csv_content = output.getvalue()
        file_size = len(csv_content.encode('utf-8'))
        
        return csv_content, file_size


# Flask 라우트
@app.route('/')
def index():
    """메인 페이지"""
    return render_template('index.html', ships=SHIP_MAPPING)


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
        
        # 날짜 계산
        start_date = datetime(year, month, day)
        
        if period_type == 'day':
            end_date = start_date + timedelta(days=period_value)
        elif period_type == 'week':
            end_date = start_date + timedelta(weeks=period_value)
        elif period_type == 'month':
            # 월 계산 (대략적)
            end_date = start_date + timedelta(days=period_value * 30)
        else:
            return jsonify({'error': 'Invalid period type'}), 400
        
        # 데이터 추출
        exporter = DataExporter()
        start_time = time.time()
        
        result = exporter.export_data(ship_code, start_date, end_date)
        
        extraction_time = time.time() - start_time
        result['extraction_time'] = f"{extraction_time:.2f}s"
        
        # CSV가 없으면 에러
        if not result['csv_content']:
            return jsonify({
                'error': 'No data found',
                'info': result
            }), 404
        
        # 파일명 생성
        start_str = start_date.strftime('%Y%m%d')
        end_str = end_date.strftime('%Y%m%d')
        filename = f"{ship_code}_{start_str}_to_{end_str}.csv"
        
        result['filename'] = filename
        
        # CSV 파일로 응답
        csv_bytes = io.BytesIO(result['csv_content'].encode('utf-8-sig'))  # BOM for Excel
        csv_bytes.seek(0)
        
        return send_file(
            csv_bytes,
            mimetype='text/csv',
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
        import traceback
        return jsonify({
            'error': str(e),
            'traceback': traceback.format_exc()
        }), 500


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
        
        # 날짜 계산
        start_date = datetime(year, month, day)
        
        if period_type == 'day':
            end_date = start_date + timedelta(days=period_value)
        elif period_type == 'week':
            end_date = start_date + timedelta(weeks=period_value)
        elif period_type == 'month':
            end_date = start_date + timedelta(days=period_value * 30)
        else:
            return jsonify({'error': 'Invalid period type'}), 400
        
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
        response = {
            'filename': filename,
            'ship_code': ship_code,
            'imo_number': result['imo_number'],
            'period': f"{start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}",
            'total_rows': result['total_rows'],
            'total_columns': result['total_columns'],
            'file_size_mb': f"{result['file_size'] / 1024 / 1024:.2f} MB",
            'extraction_time': f"{extraction_time:.2f}s",
            'tables': result['tables']
        }
        
        return jsonify(response)
        
    except Exception as e:
        import traceback
        return jsonify({
            'error': str(e),
            'traceback': traceback.format_exc()
        }), 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)

