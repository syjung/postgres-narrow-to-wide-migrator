"""
Improved Cutoff Time Management Strategy
실제 데이터 패턴을 고려한 cutoff_time 관리 전략
"""
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from loguru import logger
import threading


class CutoffTimeStrategy:
    """실제 데이터 패턴을 고려한 cutoff_time 관리"""
    
    def __init__(self):
        self.cutoff_lock = threading.Lock()
        self.last_processed_minute: Optional[datetime] = None
        
    def get_processing_window(self, current_time: Optional[datetime] = None) -> Dict[str, datetime]:
        """
        현재 시간을 기준으로 처리할 시간 윈도우 계산
        
        Args:
            current_time: 현재 시간 (None이면 DB 서버 시간 사용)
            
        Returns:
            Dict with 'start_time', 'end_time', 'target_minute'
        """
        if current_time is None:
            # DB 서버 시간 가져오기
            from database import db_manager
            try:
                db_time_result = db_manager.execute_query("SELECT NOW() as current_time")
                if db_time_result and len(db_time_result) > 0:
                    db_time = db_time_result[0]['current_time']
                    # Convert timezone-aware datetime to naive (UTC)
                    if db_time.tzinfo is not None:
                        current_time = db_time.replace(tzinfo=None)
                    else:
                        current_time = db_time
                else:
                    current_time = datetime.now()
            except Exception as e:
                logger.warning(f"Failed to get DB time: {e}, using local time")
                current_time = datetime.now()
        
        # 현재 시간을 분 단위로 정규화 (초를 0으로)
        current_minute = current_time.replace(second=0, microsecond=0)
        
        # 처리할 대상: 이전 분의 4세트 (00, 15, 30, 45초)
        target_minute = current_minute - timedelta(minutes=1)
        
        # 처리 윈도우: 이전 분의 시작부터 끝까지
        start_time = target_minute  # 12:49:00
        end_time = target_minute + timedelta(minutes=1)  # 12:50:00
        
        return {
            'start_time': start_time,
            'end_time': end_time,
            'target_minute': target_minute,
            'current_time': current_time
        }
    
    def should_process_minute(self, target_minute: datetime) -> bool:
        """
        해당 분을 처리해야 하는지 확인
        
        Args:
            target_minute: 처리 대상 분 (예: 12:49:00)
            
        Returns:
            True if should process, False otherwise
        """
        with self.cutoff_lock:
            if self.last_processed_minute is None:
                return True
            
            # 이미 처리된 분인지 확인
            if target_minute <= self.last_processed_minute:
                logger.debug(f"Minute {target_minute} already processed (last: {self.last_processed_minute})")
                return False
            
            return True
    
    def mark_minute_processed(self, target_minute: datetime):
        """
        해당 분이 처리되었음을 기록
        
        Args:
            target_minute: 처리 완료된 분
        """
        with self.cutoff_lock:
            if self.last_processed_minute is None or target_minute > self.last_processed_minute:
                self.last_processed_minute = target_minute
                logger.info(f"✅ Marked minute {target_minute} as processed")
    
    def get_cutoff_time_for_query(self, target_minute: datetime) -> datetime:
        """
        쿼리용 cutoff_time 계산
        
        Args:
            target_minute: 처리 대상 분 (예: 12:49:00)
            
        Returns:
            쿼리에서 사용할 cutoff_time
        """
        # 이전 분의 마지막 처리 시점을 cutoff로 사용
        # 예: 12:49:00을 처리할 때는 12:48:45를 cutoff로 사용
        cutoff_time = target_minute - timedelta(seconds=15)
        return cutoff_time
    
    def get_processing_status(self) -> Dict[str, Any]:
        """현재 처리 상태 반환"""
        with self.cutoff_lock:
            return {
                'last_processed_minute': self.last_processed_minute.isoformat() if self.last_processed_minute else None,
                'strategy': 'minute_based_batch_processing',
                'description': 'Processes 4 batches (00, 15, 30, 45s) from previous minute'
            }


# 전역 인스턴스
cutoff_strategy = CutoffTimeStrategy()


def get_optimal_cutoff_time() -> Dict[str, datetime]:
    """
    최적화된 cutoff_time 계산
    
    Returns:
        Dict with processing window information
    """
    return cutoff_strategy.get_processing_window()


def should_process_current_minute() -> bool:
    """
    현재 분을 처리해야 하는지 확인
    
    Returns:
        True if should process, False otherwise
    """
    window = cutoff_strategy.get_processing_window()
    return cutoff_strategy.should_process_minute(window['target_minute'])


def mark_current_minute_processed():
    """현재 처리 대상 분을 완료로 표시"""
    window = cutoff_strategy.get_processing_window()
    cutoff_strategy.mark_minute_processed(window['target_minute'])


def get_cutoff_for_query() -> datetime:
    """쿼리용 cutoff_time 반환"""
    window = cutoff_strategy.get_processing_window()
    return cutoff_strategy.get_cutoff_time_for_query(window['target_minute'])


# 사용 예시
if __name__ == "__main__":
    # 현재 처리 윈도우 확인
    window = get_optimal_cutoff_time()
    print(f"Processing window: {window['start_time']} to {window['end_time']}")
    print(f"Target minute: {window['target_minute']}")
    
    # 처리 여부 확인
    should_process = should_process_current_minute()
    print(f"Should process: {should_process}")
    
    # 쿼리용 cutoff_time
    cutoff = get_cutoff_for_query()
    print(f"Query cutoff time: {cutoff}")
    
    # 처리 완료 표시
    if should_process:
        mark_current_minute_processed()
        print("Marked as processed")
    
    # 상태 확인
    status = cutoff_strategy.get_processing_status()
    print(f"Status: {status}")
