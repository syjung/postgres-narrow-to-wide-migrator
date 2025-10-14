"""
Channel Router Module
채널 ID를 기반으로 적절한 테이블로 라우팅하는 모듈
"""
import os
from typing import Dict, List, Set, Optional
from loguru import logger


class ChannelRouter:
    """채널을 시스템 타입별로 라우팅하는 클래스"""
    
    # 테이블 타입 상수 (숫자로 간단화)
    TABLE_AUXILIARY = "1"
    TABLE_ENGINE = "2"
    TABLE_NAVIGATION = "3"
    
    def __init__(self, 
                 auxiliary_file: str = "column_list_auxiliary_systems.txt",
                 engine_file: str = "column_list_engine_generator.txt",
                 navigation_file: str = "column_list_navigation_ship.txt"):
        """
        Initialize ChannelRouter
        
        Args:
            auxiliary_file: 보조 시스템 채널 리스트 파일
            engine_file: 엔진/발전기 채널 리스트 파일
            navigation_file: 항해/선박정보 채널 리스트 파일
        """
        self.auxiliary_channels: Set[str] = set()
        self.engine_generator_channels: Set[str] = set()
        self.navigation_ship_channels: Set[str] = set()
        
        # 채널 → 테이블 타입 매핑 (빠른 조회용)
        self._channel_to_table: Dict[str, str] = {}
        
        # 파일 경로
        self.auxiliary_file = auxiliary_file
        self.engine_file = engine_file
        self.navigation_file = navigation_file
        
        # 채널 정의 로드
        self._load_channel_definitions()
        
        logger.info(f"✅ ChannelRouter initialized")
        logger.info(f"   📊 Auxiliary channels: {len(self.auxiliary_channels)}")
        logger.info(f"   📊 Engine/Generator channels: {len(self.engine_generator_channels)}")
        logger.info(f"   📊 Navigation/Ship channels: {len(self.navigation_ship_channels)}")
        logger.info(f"   📊 Total channels: {len(self._channel_to_table)}")
    
    def _load_channel_definitions(self):
        """채널 정의 파일 로드"""
        try:
            # 1. Auxiliary Systems
            self.auxiliary_channels = self._load_channel_file(self.auxiliary_file)
            for channel in self.auxiliary_channels:
                self._channel_to_table[channel] = self.TABLE_AUXILIARY
            
            # 2. Engine Generator
            self.engine_generator_channels = self._load_channel_file(self.engine_file)
            for channel in self.engine_generator_channels:
                self._channel_to_table[channel] = self.TABLE_ENGINE
            
            # 3. Navigation Ship
            self.navigation_ship_channels = self._load_channel_file(self.navigation_file)
            for channel in self.navigation_ship_channels:
                self._channel_to_table[channel] = self.TABLE_NAVIGATION
            
            # 중복 체크
            self._check_duplicates()
            
        except Exception as e:
            logger.error(f"❌ Failed to load channel definitions: {e}")
            raise
    
    def _load_channel_file(self, filename: str) -> Set[str]:
        """
        채널 파일 로드
        
        Args:
            filename: 채널 리스트 파일명
            
        Returns:
            채널 ID 세트
        """
        channels = set()
        
        if not os.path.exists(filename):
            logger.warning(f"⚠️ Channel file not found: {filename}")
            return channels
        
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):  # 빈 줄과 주석 제외
                        # 채널 ID를 파일에 있는 그대로 사용 (슬래시 포함)
                        channels.add(line)
            
            logger.debug(f"✅ Loaded {len(channels)} channels from {filename}")
            
        except Exception as e:
            logger.error(f"❌ Failed to load file {filename}: {e}")
            raise
        
        return channels
    
    def _check_duplicates(self):
        """채널 중복 체크"""
        all_channels = []
        all_channels.extend(self.auxiliary_channels)
        all_channels.extend(self.engine_generator_channels)
        all_channels.extend(self.navigation_ship_channels)
        
        if len(all_channels) != len(set(all_channels)):
            # 중복 찾기
            from collections import Counter
            counter = Counter(all_channels)
            duplicates = [ch for ch, count in counter.items() if count > 1]
            
            logger.warning(f"⚠️ Found {len(duplicates)} duplicate channels:")
            for dup in duplicates[:10]:  # 처음 10개만 표시
                logger.warning(f"   - {dup}")
    
    def get_table_type(self, channel_id: str) -> Optional[str]:
        """
        채널이 속한 테이블 타입 반환
        
        Args:
            channel_id: 채널 ID (예: /hs4sd_v1/ab/fuel/oil///use)
            
        Returns:
            테이블 타입 ('1', '2', '3')
            또는 None (알 수 없는 채널)
        """
        # 채널 ID를 파일에 저장된 그대로 사용 (슬래시 포함)
        return self._channel_to_table.get(channel_id)
    
    def get_table_name(self, channel_id: str, ship_id: str) -> Optional[str]:
        """
        채널과 선박 ID로 전체 테이블명 반환
        
        Args:
            channel_id: 채널 ID
            ship_id: 선박 ID
            
        Returns:
            테이블명 (예: 'tbl_data_timeseries_imo9976903_1')
        """
        table_type = self.get_table_type(channel_id)
        if table_type is None:
            return None
        
        return f"tbl_data_timeseries_{ship_id.lower()}_{table_type}"
    
    def filter_channels_by_table(self, channels: List[str], table_type: str) -> List[str]:
        """
        특정 테이블에 속한 채널만 필터링
        
        Args:
            channels: 채널 ID 리스트
            table_type: 테이블 타입
            
        Returns:
            필터링된 채널 리스트
        """
        filtered = []
        for channel in channels:
            if self.get_table_type(channel) == table_type:
                filtered.append(channel)
        return filtered
    
    def get_all_channels_by_table(self, table_type: str) -> Set[str]:
        """
        특정 테이블 타입의 모든 채널 반환
        
        Args:
            table_type: 테이블 타입
            
        Returns:
            채널 세트
        """
        if table_type == self.TABLE_AUXILIARY:
            return self.auxiliary_channels.copy()
        elif table_type == self.TABLE_ENGINE:
            return self.engine_generator_channels.copy()
        elif table_type == self.TABLE_NAVIGATION:
            return self.navigation_ship_channels.copy()
        else:
            return set()
    
    def get_all_table_types(self) -> List[str]:
        """모든 테이블 타입 반환"""
        return [self.TABLE_AUXILIARY, self.TABLE_ENGINE, self.TABLE_NAVIGATION]
    
    def get_all_channels(self) -> Set[str]:
        """모든 유효한 채널 ID 반환"""
        return set(self._channel_to_table.keys())
    
    def get_channel_count_by_table(self) -> Dict[str, int]:
        """테이블별 채널 수 반환"""
        return {
            self.TABLE_AUXILIARY: len(self.auxiliary_channels),
            self.TABLE_ENGINE: len(self.engine_generator_channels),
            self.TABLE_NAVIGATION: len(self.navigation_ship_channels)
        }
    
    def is_valid_channel(self, channel_id: str) -> bool:
        """채널이 유효한지 확인"""
        return self.get_table_type(channel_id) is not None
    
    def get_statistics(self) -> Dict[str, any]:
        """채널 라우터 통계 정보 반환"""
        return {
            'total_channels': len(self._channel_to_table),
            'by_table': self.get_channel_count_by_table(),
            'table_types': self.get_all_table_types()
        }


# Global instance
channel_router = ChannelRouter()


if __name__ == "__main__":
    """테스트 코드"""
    print("=" * 80)
    print("Channel Router Test")
    print("=" * 80)
    
    # 통계 출력
    stats = channel_router.get_statistics()
    print(f"\n📊 Statistics:")
    print(f"   Total channels: {stats['total_channels']}")
    print(f"   By table:")
    for table, count in stats['by_table'].items():
        print(f"      - {table}: {count}")
    
    # 샘플 채널 테스트
    test_channels = [
        "hs4sd_v1/ab/fuel/oil///use",
        "hs4sd_v1/me01/////run",
        "hs4sd_v1/ship////aft_m/draft",
        "hs4sd_v1/ge01////kw/power"
    ]
    
    print(f"\n🔍 Sample channel routing:")
    for channel in test_channels:
        table_type = channel_router.get_table_type(channel)
        table_name = channel_router.get_table_name(channel, "IMO9976903")
        print(f"   {channel}")
        print(f"      → Type: {table_type}")
        print(f"      → Table: {table_name}")

