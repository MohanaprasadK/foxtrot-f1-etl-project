from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import logging
import pandas as pd
import os
from typing import Dict, List, Any

logger = logging.getLogger(__name__)

def discover_csv_schemas(folder_path: str = "/opt/airflow/data/historical") -> Dict[str, Any]:
    """
    Discover all CSV files and extract schemas (reuse from schema discovery)
    """
    try:
        logger.info(f"🔍 Discovering schemas in: {folder_path}")
        
        if not os.path.exists(folder_path):
            raise Exception(f"Folder path does not exist: {folder_path}")
        
        csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
        logger.info(f"📁 Found {len(csv_files)} CSV files: {csv_files}")
        
        schemas = {}
        
        for csv_file in csv_files:
            file_path = os.path.join(folder_path, csv_file)
            logger.info(f"📊 Analyzing: {csv_file}")
            
            try:
                df = pd.read_csv(file_path, nrows=1000)
                
                schema_info = {
                    'file_path': file_path,
                    'total_columns': len(df.columns),
                    'sample_row_count': len(df),
                    'columns': []
                }
                
                for col_name in df.columns:
                    col_info = _analyze_column(df[col_name], col_name)
                    schema_info['columns'].append(col_info)
                
                schemas[csv_file] = schema_info
                logger.info(f"   ✅ Analyzed {csv_file}: {len(df.columns)} columns")
                
            except Exception as e:
                logger.error(f"   ❌ Failed to analyze {csv_file}: {e}")
                schemas[csv_file] = {'error': str(e)}
        
        return schemas
        
    except Exception as e:
        logger.error(f"❌ Schema discovery failed: {e}")
        raise

def _analyze_column(column: pd.Series, col_name: str) -> Dict[str, Any]:
    """
    Analyze a single column and infer data type
    """
    col_info = {
        'name': col_name,
        'non_null_count': column.count(),
        'null_count': column.isnull().sum(),
    }
    
    dtype = str(column.dtype)
    
    if 'int' in dtype:
        col_info['inferred_type'] = 'integer'
    elif 'float' in dtype:
        col_info['inferred_type'] = 'float'
    elif 'bool' in dtype:
        col_info['inferred_type'] = 'boolean'
    elif 'datetime' in dtype:
        col_info['inferred_type'] = 'timestamp'
    else:
        col_info['inferred_type'] = 'varchar'
        col_info['max_length'] = column.astype(str).str.len().max()
    
    return col_info

def populate_metadata_table():
    """
    Populate formulaone_metadata table with discovered schemas
    """
    try:
        hook = PostgresHook(postgres_conn_id='formulaone_db')
        
        # First, clear existing historical metadata
        logger.info("🗑️  Clearing existing historical metadata...")
        clear_query = "DELETE FROM formulaone_metadata WHERE dag_name = 'formulaone_historical';"
        hook.run(clear_query)
        
        # Discover schemas
        schemas = discover_csv_schemas()
        
        # Insert metadata records
        records_inserted = 0
        
        for file_name, schema_info in schemas.items():
            if 'error' in schema_info:
                logger.warning(f"⚠️  Skipping {file_name}: {schema_info['error']}")
                continue
                
            table_name = f"lake_{file_name.replace('.csv', '').lower()}"
            
            logger.info(f"📝 Populating metadata for: {table_name}")
            
            for col_order, col_info in enumerate(schema_info['columns'], 1):
                insert_query = """
                INSERT INTO formulaone_metadata 
                (dag_name, table_name, source_file, column_name, data_type, column_order)
                VALUES (%s, %s, %s, %s, %s, %s);
                """
                
                hook.run(insert_query, parameters=(
                    'formulaone_historical',
                    table_name,
                    file_name,
                    col_info['name'],
                    col_info['inferred_type'],
                    col_order
                ))
                
                records_inserted += 1
        
        # Verify insertion
        verify_query = """
        SELECT COUNT(*) as total_records 
        FROM formulaone_metadata 
        WHERE dag_name = 'formulaone_historical';
        """
        result = hook.get_first(verify_query)
        total_records = result[0] if result else 0
        
        logger.info(f"🎉 Metadata population completed!")
        logger.info(f"📊 Total records inserted: {records_inserted}")
        logger.info(f"✅ Total records in metadata: {total_records}")
        
        # Show summary
        summary_query = """
        SELECT table_name, source_file, COUNT(*) as column_count
        FROM formulaone_metadata 
        WHERE dag_name = 'formulaone_historical'
        GROUP BY table_name, source_file
        ORDER BY table_name;
        """
        tables = hook.get_records(summary_query)
        
        logger.info("📋 Metadata Summary:")
        for table_name, source_file, column_count in tables:
            logger.info(f"   🏷️  {table_name} ({source_file}): {column_count} columns")
        
        return f"Populated {records_inserted} metadata records for {len(tables)} tables"
        
    except Exception as e:
        logger.error(f"❌ Failed to populate metadata table: {e}")
        raise

def verify_metadata_setup():
    """
    Verify that metadata is properly set up for historical load
    """
    try:
        hook = PostgresHook(postgres_conn_id='formulaone_db')
        
        # Check if we have metadata for historical load
        check_query = """
        SELECT COUNT(DISTINCT table_name) as table_count,
               COUNT(*) as total_columns
        FROM formulaone_metadata 
        WHERE dag_name = 'formulaone_historical';
        """
        result = hook.get_first(check_query)
        
        if result:
            table_count, total_columns = result
            logger.info(f"✅ Metadata verification passed!")
            logger.info(f"📊 Tables: {table_count}, Columns: {total_columns}")
            
            if table_count > 0:
                return f"Ready for historical load: {table_count} tables, {total_columns} columns"
            else:
                raise Exception("No tables found in metadata for historical load")
        else:
            raise Exception("Failed to verify metadata")
            
    except Exception as e:
        logger.error(f"❌ Metadata verification failed: {e}")
        raise

# DAG Definition
with DAG(
    'populate_metadata',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=['lake', 'metadata', 'setup']
) as dag:

    start = EmptyOperator(task_id='start')
    
    populate_task = PythonOperator(
        task_id='populate_metadata_table',
        python_callable=populate_metadata_table
    )
    
    verify_task = PythonOperator(
        task_id='verify_metadata_setup',
        python_callable=verify_metadata_setup
    )
    
    end = EmptyOperator(task_id='end')

    start >> populate_task >> verify_task >> end