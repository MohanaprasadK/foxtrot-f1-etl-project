from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import logging
import pandas as pd
import os
import yaml
from typing import Dict, List, Any

logger = logging.getLogger(__name__)

def check_postgres_connection():
    """Test connection to formulaone database"""
    try:
        hook = PostgresHook(postgres_conn_id='formulaone_db')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("SELECT version();")
        db_version = cursor.fetchone()
        logger.info(f"✅ PostgreSQL Connection Successful: {db_version[0]}")
        
        cursor.close()
        conn.close()
        return "PostgreSQL connection test passed"
        
    except Exception as e:
        logger.error(f"❌ PostgreSQL connection failed: {e}")
        raise

def create_table_loading_function(table_name: str):
    """Create a table-specific loading function"""
    def load_single_table():
        """Load data for a specific table with chunking for large files"""
        try:
            hook = PostgresHook(postgres_conn_id='formulaone_db')
            
            logger.info(f"🏎️  Starting load for table: {table_name}")
            
            # Get table metadata
            metadata_query = """
            SELECT source_file, column_name, data_type, column_order
            FROM formulaone_metadata 
            WHERE dag_name = 'formulaone_historical' 
              AND table_name = %s
            ORDER BY column_order;
            """
            
            metadata = hook.get_records(metadata_query, parameters=(table_name,))
            
            if not metadata:
                raise Exception(f"No metadata found for table: {table_name}")
            
            source_file = metadata[0][0]
            columns = [
                {'name': row[1], 'type': row[2], 'order': row[3]}
                for row in metadata
            ]
            
            logger.info(f"   📁 Processing: {table_name} from {source_file}")
            
            # File path
            file_path = f"/opt/airflow/data/historical/{source_file}"
            
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"CSV file not found: {file_path}")
            
            # Create table if it doesn't exist
            logger.info(f"   🗄️  Ensuring table exists: {table_name}")
            create_table_query = _build_create_table_query(table_name, columns)
            hook.run(create_table_query)
            
            # TRUNCATE table first
            logger.info(f"   🗑️  Truncating table: {table_name}")
            hook.run(f"TRUNCATE TABLE {table_name};")
            
            # CHUNKED LOADING - for large tables like results
            logger.info(f"   📥 Reading CSV in chunks: {source_file}")
            chunk_size = 10000  # Adjust based on your system capacity
            total_rows_loaded = 0
            
            # Read CSV in chunks
            for chunk_number, chunk_df in enumerate(pd.read_csv(
                                                        file_path, 
                                                        chunksize=chunk_size,
                                                        na_values=['\\N', '\\N\\N', 'NULL', 'null', ''],  # Handle various NULL representations
                                                        keep_default_na=True
                                                    )):

                logger.info(f"   📦 Processing chunk {chunk_number + 1}")
                
                # Ensure columns match metadata and handle case sensitivity
                expected_columns = [col['name'].lower() for col in columns]
                chunk_df.columns = [col.lower() for col in chunk_df.columns]
                
                # Reorder columns to match metadata
                chunk_df = chunk_df.reindex(columns=expected_columns)

                # CLEAN DATA: Replace any remaining NaN with None for proper NULL insertion
                chunk_df = chunk_df.where(pd.notnull(chunk_df), None)
                
                # Load chunk to database
                conn = hook.get_sqlalchemy_engine()
                chunk_df.to_sql(
                    name=table_name,
                    con=conn,
                    if_exists='append',  # Append each chunk
                    index=False,
                    method='multi',
                    chunksize=1000  # Smaller chunks for database insertion
                )
                
                total_rows_loaded += len(chunk_df)
                logger.info(f"   ✅ Chunk {chunk_number + 1} loaded: {len(chunk_df)} rows (Total: {total_rows_loaded})")
            
            # Verify final count
            verify_query = f"SELECT COUNT(*) FROM {table_name};"
            final_count = hook.get_first(verify_query)[0]
            logger.info(f"   🎉 Successfully loaded {final_count} total rows into {table_name}")
            
            return f"Loaded {final_count} rows into {table_name}"
            
        except Exception as e:
            logger.error(f"❌ Failed to load table {table_name}: {e}")
            raise
    
    return load_single_table

def _build_create_table_query(table_name: str, columns: List[Dict]) -> str:
    """Build CREATE TABLE query from column definitions"""
    column_definitions = []
    for col in columns:
        col_name = col['name']
        col_type = _map_data_type(col['type'])
        column_definitions.append(f"{col_name} {col_type}")
    
    create_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_definitions)});"
    return create_query

def _map_data_type(inferred_type: str) -> str:
    """Map inferred data types to PostgreSQL types"""
    type_mapping = {
        'integer': 'INTEGER',
        'float': 'REAL', 
        'varchar': 'VARCHAR',
        'text': 'TEXT',
        'date': 'DATE',
        'timestamp': 'TIMESTAMP',
        'boolean': 'BOOLEAN'
    }
    return type_mapping.get(inferred_type, 'TEXT')

# Load table list from YAML during DAG parsing
try:
    yaml_path = "/opt/airflow/dags/lake/formulaone_tables.yaml"
    with open(yaml_path, 'r') as file:
        config = yaml.safe_load(file)
    TABLES = config.get('tables', [])
    logger.info(f"📋 Loaded {len(TABLES)} tables from YAML")
except Exception as e:
    logger.error(f"❌ Failed to load YAML config: {e}")
    TABLES = []

# DAG Definition
with DAG(
    'historical_load_parallel',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=1,  # Run 2 tables in parallel
    tags=['lake', 'historical', 'etl', 'parallel']
) as dag:

    start = EmptyOperator(task_id='start')
    
    check_connection = PythonOperator(
        task_id='check_postgres_connection',
        python_callable=check_postgres_connection
    )
    
    # Create table loading tasks for each table in YAML
    table_tasks = []
    for table_name in TABLES:
        task_id = f'fetchdata_{table_name}'
        task = PythonOperator(
            task_id=task_id,
            python_callable=create_table_loading_function(table_name)
        )
        table_tasks.append(task)
    
    end = EmptyOperator(task_id='end')

    # Simple linear workflow: start → check_connection → [all table tasks] → end
    start >> check_connection >> table_tasks >> end