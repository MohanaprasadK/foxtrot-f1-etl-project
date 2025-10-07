from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def debug_metadata():
    """Debug what's in the metadata table"""
    hook = PostgresHook(postgres_conn_id='formulaone_db')
    
    # Check metadata
    metadata_query = """
    SELECT dag_name, table_name, source_file, COUNT(*) as column_count
    FROM formulaone_metadata 
    GROUP BY dag_name, table_name, source_file
    ORDER BY dag_name, table_name;
    """
    metadata = hook.get_records(metadata_query)
    
    logger.info("📋 METADATA TABLE CONTENTS:")
    for dag_name, table_name, source_file, column_count in metadata:
        logger.info(f"   {dag_name}.{table_name} ({source_file}): {column_count} columns")
    
    return f"Found {len(metadata)} table definitions"

def debug_existing_tables():
    """Debug what tables actually exist in the database"""
    hook = PostgresHook(postgres_conn_id='formulaone_db')
    
    # Check existing tables
    tables_query = """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public' 
    ORDER BY table_name;
    """
    tables = hook.get_records(tables_query)
    
    logger.info("🗄️ EXISTING TABLES IN DATABASE:")
    for table in tables:
        logger.info(f"   ✅ {table[0]}")
    
    return f"Found {len(tables)} tables in database"

with DAG(
    'debug_tables',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=['debug']
) as dag:

    debug_metadata_task = PythonOperator(
        task_id='debug_metadata',
        python_callable=debug_metadata
    )
    
    debug_tables_task = PythonOperator(
        task_id='debug_existing_tables',
        python_callable=debug_existing_tables
    )
    
    debug_metadata_task >> debug_tables_task