from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import logging
import pandas as pd
import os
from typing import Dict, List, Any
import json

logger = logging.getLogger(__name__)

def discover_csv_schemas(folder_path: str = "/opt/airflow/data") -> Dict[str, Any]:
    """
    Discover all CSV files in a folder and extract their schemas
    
    Args:
        folder_path: Path to folder containing CSV files
        
    Returns:
        Dictionary with file names and their schemas
    """
    try:
        logger.info(f"🔍 Starting schema discovery in: {folder_path}")
        
        # Check if folder exists
        if not os.path.exists(folder_path):
            raise Exception(f"Folder path does not exist: {folder_path}")
        
        # Find all CSV files
        csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
        logger.info(f"📁 Found {len(csv_files)} CSV files: {csv_files}")
        
        schemas = {}
        
        for csv_file in csv_files:
            file_path = os.path.join(folder_path, csv_file)
            logger.info(f"📊 Analyzing: {csv_file}")
            
            try:
                # Read first few rows to infer schema
                df = pd.read_csv(file_path, nrows=1000)  # Read first 1000 rows for sampling
                
                schema_info = {
                    'file_path': file_path,
                    'total_columns': len(df.columns),
                    'sample_row_count': len(df),
                    'estimated_total_rows': _estimate_total_rows(file_path),
                    'columns': []
                }
                
                # Analyze each column
                for col_name in df.columns:
                    col_info = _analyze_column(df[col_name], col_name)
                    schema_info['columns'].append(col_info)
                
                schemas[csv_file] = schema_info
                logger.info(f"   ✅ Analyzed {csv_file}: {len(df.columns)} columns, ~{schema_info['estimated_total_rows']} rows")
                
            except Exception as e:
                logger.error(f"   ❌ Failed to analyze {csv_file}: {e}")
                schemas[csv_file] = {'error': str(e)}
        
        # Generate summary
        logger.info("🎊 SCHEMA DISCOVERY SUMMARY:")
        for file_name, info in schemas.items():
            if 'error' not in info:
                logger.info(f"📋 {file_name}: {info['total_columns']} columns, ~{info['estimated_total_rows']} rows")
        
        return {
            'folder_path': folder_path,
            'total_files': len(schemas),
            'schemas': schemas
        }
        
    except Exception as e:
        logger.error(f"❌ Schema discovery failed: {e}")
        raise

def _estimate_total_rows(file_path: str) -> int:
    """
    Estimate total rows in CSV file without loading entire file
    """
    try:
        # Count lines in file (approximate)
        with open(file_path, 'r', encoding='utf-8') as f:
            line_count = sum(1 for line in f)
        return max(0, line_count - 1)  # Subtract header
    except:
        return 0

def _analyze_column(column: pd.Series, col_name: str) -> Dict[str, Any]:
    """
    Analyze a single column and infer data type, statistics
    """
    # Basic info
    col_info = {
        'name': col_name,
        'non_null_count': column.count(),
        'null_count': column.isnull().sum(),
        'unique_count': column.nunique()
    }
    
    # Infer data type
    dtype = str(column.dtype)
    
    # Map pandas dtypes to SQL-like types
    if 'int' in dtype:
        col_info['inferred_type'] = 'integer'
        col_info['min_value'] = int(column.min()) if not column.isnull().all() else None
        col_info['max_value'] = int(column.max()) if not column.isnull().all() else None
    elif 'float' in dtype:
        col_info['inferred_type'] = 'float'
        col_info['min_value'] = float(column.min()) if not column.isnull().all() else None
        col_info['max_value'] = float(column.max()) if not column.isnull().all() else None
    elif 'bool' in dtype:
        col_info['inferred_type'] = 'boolean'
    elif 'datetime' in dtype:
        col_info['inferred_type'] = 'timestamp'
    else:
        # String type - analyze further
        col_info['inferred_type'] = 'varchar'
        col_info['max_length'] = column.astype(str).str.len().max()
        
        # Check if it might be a date string
        sample_values = column.dropna().head(5)
        if _looks_like_date(sample_values):
            col_info['inferred_type'] = 'date'
    
    # Sample values
    sample_non_null = column.dropna().head(3).tolist()
    col_info['sample_values'] = sample_non_null
    
    return col_info

def _looks_like_date(values: pd.Series) -> bool:
    """
    Check if values look like dates
    """
    date_patterns = ['-', '/', '202', '199', '198']  # Common date patterns
    for val in values:
        val_str = str(val)
        if any(pattern in val_str for pattern in date_patterns):
            return True
    return False

def generate_metadata_records(**kwargs) -> Dict[str, Any]:
    """
    Generate metadata table records from discovered schemas
    """
    ti = kwargs['ti']
    discovery_result = ti.xcom_pull(task_ids='discover_schemas')
    
    schemas = discovery_result['schemas']
    metadata_records = []
    
    logger.info("📝 Generating metadata records...")
    
    for file_name, schema_info in schemas.items():
        if 'error' in schema_info:
            logger.warning(f"⚠️  Skipping {file_name}: {schema_info['error']}")
            continue
            
        table_name = f"lake_{file_name.replace('.csv', '').lower()}"
        
        for col_order, col_info in enumerate(schema_info['columns'], 1):
            record = {
                'dag_name': 'formulaone_historical',
                'table_name': table_name,
                'source_file': file_name,
                'column_name': col_info['name'],
                'data_type': col_info['inferred_type'],
                'column_order': col_order,
                'additional_info': {
                    'null_count': col_info['null_count'],
                    'unique_count': col_info['unique_count'],
                    'sample_values': col_info['sample_values']
                }
            }
            metadata_records.append(record)
            
            logger.info(f"   📋 {table_name}.{col_info['name']} -> {col_info['inferred_type']}")
    
    logger.info(f"🎯 Generated {len(metadata_records)} metadata records")
    
    return {
        'metadata_records': metadata_records,
        'total_tables': len([s for s in schemas.values() if 'error' not in s])
    }

# DAG Definition
with DAG(
    'schema_discovery',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=['discovery', 'schema', 'csv']
) as dag:

    start = EmptyOperator(task_id='start')
    
    discover_schemas = PythonOperator(
        task_id='discover_schemas',
        python_callable=discover_csv_schemas,
        op_kwargs={'folder_path': '/opt/airflow/data/historical'}  # Change this path as needed
    )
    
    generate_metadata = PythonOperator(
        task_id='generate_metadata',
        python_callable=generate_metadata_records
    )

    end = EmptyOperator(task_id='end')

    start >> discover_schemas >> generate_metadata >> end