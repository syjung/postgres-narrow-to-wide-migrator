"""
Multi-Table Generator Module
3개 테이블 생성 및 관리 모듈
"""
from typing import Dict, List, Any, Optional, Set
from loguru import logger
from database import db_manager
from channel_router import channel_router


class MultiTableGenerator:
    """선박당 3개 테이블(auxiliary, engine, navigation)을 생성하고 관리하는 클래스"""
    
    def __init__(self):
        self.channel_router = channel_router
    
    def ensure_all_tables_exist(self, ship_id: str) -> bool:
        """
        선박의 3개 테이블 모두 생성 확인
        
        Args:
            ship_id: 선박 ID
            
        Returns:
            성공 여부
        """
        logger.debug(f"Checking tables for ship: {ship_id}")
        
        try:
            # 3개 테이블 모두 생성 (이미 있으면 스킵)
            success = True
            tables_created = 0
            
            # 1. Auxiliary Systems
            created = self.create_auxiliary_systems_table(ship_id)
            if not created:
                logger.error(f"❌ Failed to create auxiliary_systems table for {ship_id}")
                success = False
            elif created == "created":  # 새로 생성됨
                tables_created += 1
            
            # 2. Engine Generator
            created = self.create_engine_generator_table(ship_id)
            if not created:
                logger.error(f"❌ Failed to create engine_generator table for {ship_id}")
                success = False
            elif created == "created":  # 새로 생성됨
                tables_created += 1
            
            # 3. Navigation Ship
            created = self.create_navigation_ship_table(ship_id)
            if not created:
                logger.error(f"❌ Failed to create navigation_ship table for {ship_id}")
                success = False
            elif created == "created":  # 새로 생성됨
                tables_created += 1
            
            if success:
                # 인덱스 생성 (조용히)
                self.create_indexes(ship_id)
                
                # 새로 생성된 테이블이 있을 때만 로그
                if tables_created > 0:
                    logger.success(f"✅ Created {tables_created} new tables for {ship_id}")
                else:
                    logger.debug(f"All tables already exist for {ship_id}")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Failed to ensure tables for {ship_id}: {e}")
            return False
    
    def create_auxiliary_systems_table(self, ship_id: str) -> bool:
        """
        보조 시스템 테이블 생성
        
        Args:
            ship_id: 선박 ID
            
        Returns:
            성공 여부
        """
        table_name = f"auxiliary_systems_{ship_id.lower()}"
        channels = self.channel_router.get_all_channels_by_table(
            self.channel_router.TABLE_AUXILIARY
        )
        
        return self._create_table(table_name, channels, "Auxiliary Systems")
    
    def create_engine_generator_table(self, ship_id: str) -> bool:
        """
        엔진/발전기 테이블 생성
        
        Args:
            ship_id: 선박 ID
            
        Returns:
            성공 여부
        """
        table_name = f"engine_generator_{ship_id.lower()}"
        channels = self.channel_router.get_all_channels_by_table(
            self.channel_router.TABLE_ENGINE
        )
        
        return self._create_table(table_name, channels, "Engine Generator")
    
    def create_navigation_ship_table(self, ship_id: str) -> bool:
        """
        항해/선박정보 테이블 생성
        
        Args:
            ship_id: 선박 ID
            
        Returns:
            성공 여부
        """
        table_name = f"navigation_ship_{ship_id.lower()}"
        channels = self.channel_router.get_all_channels_by_table(
            self.channel_router.TABLE_NAVIGATION
        )
        
        return self._create_table(table_name, channels, "Navigation Ship")
    
    def _create_table(self, table_name: str, channels: Set[str], description: str):
        """
        테이블 생성 (공통 로직)
        
        Args:
            table_name: 테이블명
            channels: 채널 세트
            description: 테이블 설명
            
        Returns:
            "created" if newly created, True if already exists, False if failed
        """
        try:
            # 테이블 존재 확인
            if db_manager.check_table_exists(table_name):
                logger.debug(f"Table {table_name} already exists")
                return True
            
            # 새로 생성
            logger.info(f"📋 Creating {description} table: {table_name} ({len(channels)} channels)")
            
            # CREATE TABLE SQL 생성
            create_sql = self._generate_create_table_sql(table_name, channels)
            
            # 실행
            db_manager.execute_update(create_sql)
            
            logger.success(f"✅ Successfully created table: {table_name}")
            return "created"
            
        except Exception as e:
            logger.error(f"❌ Failed to create table {table_name}: {e}")
            return False
    
    def _generate_create_table_sql(self, table_name: str, channels: Set[str]) -> str:
        """
        CREATE TABLE SQL 생성
        
        Args:
            table_name: 테이블명
            channels: 채널 세트
            
        Returns:
            CREATE TABLE SQL
        """
        sql_parts = [f"CREATE TABLE IF NOT EXISTS tenant.{table_name} ("]
        
        # created_time 컬럼 (Primary Key)
        column_definitions = ["    created_time TIMESTAMP NOT NULL"]
        
        # 데이터 채널 컬럼들
        for channel in sorted(channels):
            col_name = self._channel_to_column_name(channel)
            # Quote column name if needed
            if self._needs_quoting(col_name):
                quoted_name = f'"{col_name}"'
            else:
                quoted_name = col_name
            
            column_definitions.append(f"    {quoted_name} DOUBLE PRECISION")
        
        sql_parts.append(",\n".join(column_definitions))
        
        # Primary Key constraint
        sql_parts.append(f",\n    CONSTRAINT {table_name}_pk PRIMARY KEY (created_time)")
        
        sql_parts.append(");")
        
        return "\n".join(sql_parts)
    
    def _channel_to_column_name(self, channel: str) -> str:
        """
        채널 ID를 컬럼명으로 변환
        
        Args:
            channel: 채널 ID (예: hs4sd_v1/ab/fuel/oil///use)
            
        Returns:
            컬럼명 (예: hs4sd_v1_ab_fuel_oil_use)
        """
        # / 를 _ 로 변경
        col_name = channel.replace('/', '_')
        
        # 연속된 _ 제거
        while '__' in col_name:
            col_name = col_name.replace('__', '_')
        
        # 앞뒤 _ 제거
        col_name = col_name.strip('_')
        
        return col_name
    
    def _needs_quoting(self, col_name: str) -> bool:
        """컬럼명에 따옴표가 필요한지 확인"""
        special_chars = ['-', ' ', '.', '(', ')', '[', ']', '{', '}', '@', '#', '$', 
                        '%', '^', '&', '*', '+', '=', '|', '\\', ':', ';', '"', "'", 
                        '<', '>', ',', '?', '!', '~', '`']
        return any(char in col_name for char in special_chars)
    
    def create_indexes(self, ship_id: str):
        """
        3개 테이블 모두에 BRIN 인덱스 생성
        
        Args:
            ship_id: 선박 ID
        """
        logger.debug(f"Checking indexes for ship: {ship_id}")
        
        # 3개 테이블에 대해 인덱스 생성
        tables = [
            f"auxiliary_systems_{ship_id.lower()}",
            f"engine_generator_{ship_id.lower()}",
            f"navigation_ship_{ship_id.lower()}"
        ]
        
        indexes_created = 0
        for table_name in tables:
            try:
                created = self._create_brin_index(table_name)
                if created:
                    indexes_created += 1
            except Exception as e:
                logger.warning(f"⚠️ Failed to create index for {table_name}: {e}")
        
        if indexes_created > 0:
            logger.info(f"✅ Created {indexes_created} new indexes for {ship_id}")
    
    def _create_brin_index(self, table_name: str):
        """
        BRIN 인덱스 생성
        
        Args:
            table_name: 테이블명
            
        Returns:
            True if newly created, False if already exists or failed
        """
        index_name = f"idx_{table_name}_created_time"
        
        # 인덱스 존재 확인
        check_sql = """
        SELECT EXISTS (
            SELECT 1 
            FROM pg_indexes 
            WHERE schemaname = 'tenant' 
            AND tablename = %s 
            AND indexname = %s
        )
        """
        
        result = db_manager.execute_query(check_sql, (table_name, index_name))
        if result and result[0].get('exists', False):
            logger.debug(f"Index {index_name} already exists")
            return False
        
        # BRIN 인덱스 생성
        create_index_sql = f"""
        CREATE INDEX IF NOT EXISTS {index_name}
        ON tenant.{table_name}
        USING BRIN (created_time)
        WITH (pages_per_range = 128)
        """
        
        db_manager.execute_update(create_index_sql)
        logger.info(f"✅ Created BRIN index: {index_name}")
        return True
    
    def drop_all_tables(self, ship_id: str) -> bool:
        """
        선박의 3개 테이블 모두 삭제 (개발/테스트용)
        
        Args:
            ship_id: 선박 ID
            
        Returns:
            성공 여부
        """
        logger.warning(f"⚠️ Dropping all tables for ship: {ship_id}")
        
        tables = [
            f"auxiliary_systems_{ship_id.lower()}",
            f"engine_generator_{ship_id.lower()}",
            f"navigation_ship_{ship_id.lower()}"
        ]
        
        success = True
        for table_name in tables:
            try:
                drop_sql = f"DROP TABLE IF EXISTS tenant.{table_name} CASCADE"
                db_manager.execute_update(drop_sql)
                logger.info(f"✅ Dropped table: {table_name}")
            except Exception as e:
                logger.error(f"❌ Failed to drop table {table_name}: {e}")
                success = False
        
        return success
    
    def get_table_info(self, ship_id: str) -> Dict[str, Any]:
        """
        선박의 테이블 정보 반환
        
        Args:
            ship_id: 선박 ID
            
        Returns:
            테이블 정보 딕셔너리
        """
        tables = [
            f"auxiliary_systems_{ship_id.lower()}",
            f"engine_generator_{ship_id.lower()}",
            f"navigation_ship_{ship_id.lower()}"
        ]
        
        info = {
            'ship_id': ship_id,
            'tables': {}
        }
        
        for table_name in tables:
            exists = db_manager.check_table_exists(table_name)
            info['tables'][table_name] = {
                'exists': exists
            }
            
            if exists:
                # 컬럼 수 조회
                try:
                    query = """
                    SELECT COUNT(*) as col_count
                    FROM information_schema.columns
                    WHERE table_schema = 'tenant'
                    AND table_name = %s
                    """
                    result = db_manager.execute_query(query, (table_name,))
                    if result:
                        info['tables'][table_name]['column_count'] = result[0]['col_count']
                except Exception as e:
                    logger.warning(f"⚠️ Failed to get column count for {table_name}: {e}")
        
        return info


# Global instance
multi_table_generator = MultiTableGenerator()


if __name__ == "__main__":
    """테스트 코드"""
    import sys
    
    print("=" * 80)
    print("Multi-Table Generator Test")
    print("=" * 80)
    
    # 테스트할 선박 ID
    test_ship_id = "IMO9999999"  # 테스트용 선박 ID
    
    print(f"\n🚢 Test ship: {test_ship_id}")
    
    # 테이블 생성 테스트 (주의: 실제 DB에 생성됨)
    if len(sys.argv) > 1 and sys.argv[1] == "--create":
        print("\n⚠️  Creating tables in database...")
        success = multi_table_generator.ensure_all_tables_exist(test_ship_id)
        print(f"\n{'✅' if success else '❌'} Table creation: {'Success' if success else 'Failed'}")
        
        # 테이블 정보 확인
        info = multi_table_generator.get_table_info(test_ship_id)
        print(f"\n📊 Table Info:")
        for table, details in info['tables'].items():
            print(f"   {table}:")
            print(f"      Exists: {details.get('exists', False)}")
            if 'column_count' in details:
                print(f"      Columns: {details['column_count']}")
    else:
        print("\n💡 Run with --create to actually create tables")
        print("   Example: python3 multi_table_generator.py --create")
        
        # 채널 정보만 표시
        stats = channel_router.get_channel_count_by_table()
        print(f"\n📊 Channel counts:")
        for table_type, count in stats.items():
            table_name = f"{table_type}_{test_ship_id.lower()}"
            print(f"   {table_name}: {count} channels")

