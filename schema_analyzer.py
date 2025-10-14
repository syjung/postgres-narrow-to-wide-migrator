"""
Schema analysis module for generating wide table schemas
"""
from typing import Dict, List, Set, Any
from collections import defaultdict
from loguru import logger
from database import db_manager
from config import migration_config


class SchemaAnalyzer:
    """
    Analyzes narrow table data to generate wide table schemas
    
    ⚠️ NOTE: This is only used in Legacy Single-Table mode.
    Multi-Table mode uses channel_router and multi_table_generator instead.
    """
    
    def __init__(self):
        self.value_format_mapping = migration_config.VALUE_FORMAT_MAPPING
        # Only load allowed columns if NOT in Multi-Table mode
        if not migration_config.use_multi_table:
            self.allowed_columns = self._load_allowed_columns()
            logger.info(f"📋 SchemaAnalyzer: Legacy mode, {len(self.allowed_columns)} allowed columns")
        else:
            self.allowed_columns = set()
            logger.debug(f"📋 SchemaAnalyzer: Multi-Table mode, schema_analyzer not used")
    
    def _load_allowed_columns(self) -> set:
        """Load allowed columns from column_list.txt (Legacy mode only)"""
        try:
            with open('column_list.txt', 'r', encoding='utf-8') as f:
                columns = {line.strip() for line in f if line.strip()}
            logger.debug(f"✅ Loaded {len(columns)} allowed columns from column_list.txt")
            return columns
        except FileNotFoundError:
            logger.warning("⚠️ column_list.txt not found, using empty allowed columns set")
            return set()
        except Exception as e:
            logger.error(f"❌ Failed to load column_list.txt: {e}")
            return set()
    
    def analyze_ship_data(self, ship_id: str, sample_minutes: int = 10) -> Dict[str, Any]:
        """
        Analyze sample data for a specific ship to determine schema
        
        Args:
            ship_id: Ship identifier
            sample_minutes: Number of minutes of sample data to analyze
            
        Returns:
            Dictionary containing schema information
        """
        logger.info(f"Analyzing schema for ship_id: {ship_id}")
        
        # Get sample data
        sample_data = db_manager.get_sample_data(ship_id, sample_minutes)
        
        if not sample_data:
            logger.warning(f"No sample data found for ship_id: {ship_id}")
            return self._create_empty_schema(ship_id)
        
        # Analyze data channels and their value formats
        channel_analysis = self._analyze_data_channels(sample_data)
        
        # Generate schema
        schema = {
            'ship_id': ship_id,
            'table_name': f'tbl_data_timeseries_{ship_id.upper()}',  # Force uppercase for consistency
            'columns': self._generate_column_definitions(channel_analysis),
            'primary_key': 'created_time',
            'indexes': ['created_time'],
            'sample_count': len(sample_data),
            'data_channels': list(channel_analysis.keys())
        }
        
        logger.info(f"Schema analysis completed for {ship_id}: {len(schema['columns'])} columns")
        return schema
    
    def _analyze_data_channels(self, sample_data: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Analyze data channels and their value formats (only allowed columns)"""
        channel_analysis = defaultdict(lambda: {
            'value_formats': set(),
            'sample_values': [],
            'nullable': True
        })
        
        for row in sample_data:
            channel_id = row['data_channel_id']
            
            # Only analyze channels that are in the allowed columns list
            if channel_id not in self.allowed_columns:
                logger.debug(f"⚠️ Skipping channel {channel_id} - not in allowed columns list")
                continue
            
            value_format = row['value_format']
            
            if value_format:
                channel_analysis[channel_id]['value_formats'].add(value_format)
            
            # Collect sample values for validation
            sample_value = self._get_sample_value(row, value_format)
            if sample_value is not None:
                channel_analysis[channel_id]['sample_values'].append(sample_value)
        
        # Convert sets to lists for JSON serialization
        for channel_id, analysis in channel_analysis.items():
            analysis['value_formats'] = list(analysis['value_formats'])
            analysis['primary_format'] = self._determine_primary_format(analysis['value_formats'])
        
        logger.info(f"📊 Analyzed {len(channel_analysis)} channels (filtered from {len(sample_data)} sample records)")
        return dict(channel_analysis)
    
    def _get_sample_value(self, row: Dict[str, Any], value_format: str) -> Any:
        """Extract sample value based on value_format"""
        if not value_format or value_format not in self.value_format_mapping:
            return None
        
        column_name = self.value_format_mapping[value_format]
        return row.get(column_name)
    
    def _determine_primary_format(self, value_formats: List[str]) -> str:
        """Determine the primary value format for a data channel"""
        if not value_formats:
            return 'String'  # Default to String
        
        # Priority order: Decimal > Integer > String > Boolean
        priority_order = ['Decimal', 'Integer', 'String', 'Boolean']
        
        for format_type in priority_order:
            if format_type in value_formats:
                return format_type
        
        return value_formats[0]  # Fallback to first format
    
    def _generate_column_definitions(self, channel_analysis: Dict[str, Dict[str, Any]]) -> List[Dict[str, str]]:
        """Generate column definitions for wide table"""
        columns = [
            {
                'name': 'created_time',
                'type': 'timestamp',
                'nullable': False,
                'description': 'Primary key - timestamp of data collection'
            }
        ]
        
        for channel_id, analysis in channel_analysis.items():
            # All data channel columns are text type as per requirements
            column_def = {
                'name': channel_id,
                'type': 'text',
                'nullable': True,
                'description': f'Data channel: {channel_id}',
                'primary_format': analysis.get('primary_format', 'String'),
                'value_formats': analysis.get('value_formats', [])
            }
            columns.append(column_def)
        
        return columns
    
    def _create_empty_schema(self, ship_id: str) -> Dict[str, Any]:
        """Create empty schema when no sample data is available"""
        return {
            'ship_id': ship_id,
            'table_name': f'tbl_data_timeseries_{ship_id.upper()}',  # Force uppercase for consistency
            'columns': [
                {
                    'name': 'created_time',
                    'type': 'timestamp',
                    'nullable': False,
                    'description': 'Primary key - timestamp of data collection'
                }
            ],
            'primary_key': 'created_time',
            'indexes': ['created_time'],
            'sample_count': 0,
            'data_channels': []
        }
    
    def analyze_all_ships(self, sample_minutes: int = 10) -> Dict[str, Dict[str, Any]]:
        """Analyze schemas for all target ships"""
        logger.info("Starting schema analysis for all target ships")
        
        ship_ids = db_manager.get_distinct_ship_ids()
        schemas = {}
        
        for ship_id in ship_ids:
            try:
                schema = self.analyze_ship_data(ship_id, sample_minutes)
                schemas[ship_id] = schema
            except Exception as e:
                logger.error(f"Failed to analyze schema for ship_id {ship_id}: {e}")
                schemas[ship_id] = self._create_empty_schema(ship_id)
        
        logger.info(f"Schema analysis completed for {len(schemas)} ships")
        return schemas
    
    def validate_schema(self, schema: Dict[str, Any]) -> List[str]:
        """Validate generated schema"""
        issues = []
        
        # Check if table name is valid
        table_name = schema.get('table_name', '')
        if not table_name.startswith('tbl_data_timeseries_'):
            issues.append(f"Invalid table name: {table_name}")
        
        # Check if created_time column exists
        columns = schema.get('columns', [])
        created_time_col = next((col for col in columns if col['name'] == 'created_time'), None)
        if not created_time_col:
            issues.append("Missing created_time column")
        elif created_time_col['type'] != 'timestamp':
            issues.append("created_time column must be timestamp type")
        
        # Check for duplicate column names
        column_names = [col['name'] for col in columns]
        if len(column_names) != len(set(column_names)):
            issues.append("Duplicate column names found")
        
        return issues


# Global schema analyzer instance
schema_analyzer = SchemaAnalyzer()

