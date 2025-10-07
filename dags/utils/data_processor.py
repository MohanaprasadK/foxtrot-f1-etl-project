import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from datetime import datetime

from utils.api_client import F1APIClient
from utils.db_client import DBClient
from utils.config_loader import load_table_metadata

logger = logging.getLogger(__name__)

class DataProcessor:
    """
    Core data processing engine for F1 ETL pipeline
    Handles data extraction, transformation, and loading
    """
    
    def __init__(self, connection_id: str = "formulaone_db"):
        self.connection_id = connection_id
        self.api_client = F1APIClient()
        self.db_client = DBClient(connection_id)
        self.metadata = load_table_metadata()
    
    def process_table(self, table_name: str, custom_params: Optional[Dict] = None) -> bool:
        """
        Main method to process a single table (full ETL)
        
        Args:
            table_name: Name of the table to process
            custom_params: Additional API parameters if needed
            
        Returns:
            bool: True if processing successful
        """
        try:
            logger.info(f"🏎️  Starting ETL for table: {table_name}")
            
            # 1. Get table configuration from metadata
            table_config = self.metadata.get(table_name)
            if not table_config:
                raise ValueError(f"Table {table_name} not found in metadata")
            
            # 2. Extract data from API
            raw_data = self._extract_data(table_config, custom_params)
            if not raw_data:
                logger.warning(f"⚠️  No data returned for table {table_name}")
                return True  # Consider empty data as success
            
            # 3. Transform data to DataFrame with proper schema
            df = self._transform_data(raw_data, table_config)
            
            # 4. Load data into PostgreSQL
            self._load_data(table_name, df, table_config)
            
            logger.info(f"✅ Successfully processed table {table_name} - {len(df)} rows")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to process table {table_name}: {e}")
            raise
    
    def _extract_data(self, table_config: Dict, custom_params: Optional[Dict] = None) -> List[Dict]:
        """
        Extract data from F1 API
        
        Args:
            table_config: Table configuration from metadata
            custom_params: Additional API parameters
            
        Returns:
            List of raw data records
        """
        endpoint = table_config['api_endpoint']
        
        # Start with basic parameters, allow custom overrides
        params = custom_params or {}
        
        logger.info(f"📥 Extracting data from: {endpoint}")
        data = self.api_client.fetch_data(endpoint, params)
        
        return data
    
    def _transform_data(self, raw_data: List[Dict], table_config: Dict) -> pd.DataFrame:
        """
        Transform raw API data to structured DataFrame
        
        Args:
            raw_data: Raw data from API
            table_config: Table configuration for schema
            
        Returns:
            Structured DataFrame
        """
        logger.info(f"🔄 Transforming {len(raw_data)} records")
        
        # Create DataFrame from raw data
        df = pd.DataFrame(raw_data)
        
        if df.empty:
            logger.warning("⚠️  Empty DataFrame after transformation")
            return df
        
        # Apply schema enforcement
        df = self._enforce_schema(df, table_config['columns'])
        
        # Data type conversions
        df = self._convert_data_types(df, table_config['columns'])
        
        # Handle missing values for required columns
        df = self._handle_missing_values(df, table_config['columns'])
        
        logger.info(f"📊 Transformed data shape: {df.shape}")
        return df
    
    def _enforce_schema(self, df: pd.DataFrame, columns_config: List[Dict]) -> pd.DataFrame:
        """
        Enforce schema based on metadata
        
        Args:
            df: Input DataFrame
            columns_config: Column definitions from metadata
            
        Returns:
            DataFrame with enforced schema
        """
        expected_columns = [col['name'] for col in columns_config]
        
        # Add missing columns with None values
        for col in expected_columns:
            if col not in df.columns:
                logger.warning(f"⚠️  Adding missing column: {col}")
                df[col] = None
        
        # Remove extra columns not in metadata
        extra_columns = set(df.columns) - set(expected_columns)
        if extra_columns:
            logger.warning(f"⚠️  Dropping extra columns: {extra_columns}")
            df = df[expected_columns]
        
        # Reorder columns to match metadata order
        df = df[expected_columns]
        
        return df
    
    def _convert_data_types(self, df: pd.DataFrame, columns_config: List[Dict]) -> pd.DataFrame:
        """
        Convert data types based on metadata
        
        Args:
            df: Input DataFrame
            columns_config: Column definitions
            
        Returns:
            DataFrame with proper data types
        """
        type_converters = {
            'integer': pd.to_numeric,
            'int': pd.to_numeric,
            'timestamp': pd.to_datetime,
            'datetime': pd.to_datetime,
            'boolean': lambda x: x.astype(bool),
            'float': pd.to_numeric,
            'varchar': lambda x: x.astype(str),
            'text': lambda x: x.astype(str)
        }
        
        for col_config in columns_config:
            col_name = col_config['name']
            col_type = col_config['type']
            
            if col_name not in df.columns:
                continue
                
            if col_type in type_converters:
                try:
                    converter = type_converters[col_type]
                    df[col_name] = converter(df[col_name])
                    logger.debug(f"✅ Converted {col_name} to {col_type}")
                except Exception as e:
                    logger.warning(f"⚠️  Failed to convert {col_name} to {col_type}: {e}")
                    # Keep original data if conversion fails
        
        return df
    
    def _handle_missing_values(self, df: pd.DataFrame, columns_config: List[Dict]) -> pd.DataFrame:
        """
        Handle missing values based on column requirements
        
        Args:
            df: Input DataFrame
            columns_config: Column definitions
            
        Returns:
            DataFrame with handled missing values
        """
        for col_config in columns_config:
            col_name = col_config['name']
            is_required = col_config.get('required', True)
            
            if col_name not in df.columns:
                continue
                
            if df[col_name].isnull().any():
                null_count = df[col_name].isnull().sum()
                if is_required and null_count > 0:
                    logger.warning(f"⚠️  {null_count} null values in required column: {col_name}")
                    # For now, we'll keep them but log warning
                    # In production, you might want to handle this differently
        
        return df
    
    def _load_data(self, table_name: str, df: pd.DataFrame, table_config: Dict):
        """
        Load data into PostgreSQL
        
        Args:
            table_name: Target table name
            df: DataFrame to load
            table_config: Table configuration
        """
        logger.info(f"📤 Loading {len(df)} rows into {table_name}")
        
        # 1. Ensure table exists
        self.db_client.create_table_if_not_exists(table_name, table_config['columns'])
        
        # 2. Truncate table for full load
        if table_config.get('load_type') == 'full':
            self.db_client.truncate_table(table_name)
        
        # 3. Insert data
        self.db_client.insert_dataframe(table_name, df)
        
        logger.info(f"✅ Successfully loaded data into {table_name}")


# Utility functions for easy usage
def process_single_table(table_name: str, connection_id: str = "formulaone_db") -> bool:
    """Convenience function to process a single table"""
    processor = DataProcessor(connection_id)
    return processor.process_table(table_name)

def process_all_tables(connection_id: str = "formulaone_db") -> Dict[str, bool]:
    """Convenience function to process all tables"""
    processor = DataProcessor(connection_id)
    results = {}
    
    for table_name in processor.metadata.keys():
        try:
            results[table_name] = processor.process_table(table_name)
        except Exception as e:
            logger.error(f"❌ Failed to process {table_name}: {e}")
            results[table_name] = False
    
    return results