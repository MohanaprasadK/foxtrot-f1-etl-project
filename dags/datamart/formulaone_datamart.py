from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import logging
import os
import glob
import pandas as pd

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

def scan_sql_scripts():
    """Scan scripts folder and return SQL files for datamart transformations"""
    try:
        scripts_folder = "/opt/airflow/dags/datamart/scripts"
        sql_files = glob.glob(os.path.join(scripts_folder, "*.sql"))
        
        datamarts = []
        for sql_file in sql_files:
            # Extract datamart name from filename (without .sql extension)
            dm_name = os.path.basename(sql_file).replace('.sql', '')
            datamarts.append({
                'name': dm_name,
                'file_path': sql_file
            })
        
        logger.info(f"📁 Found {len(datamarts)} datamart SQL scripts: {[dm['name'] for dm in datamarts]}")
        return datamarts
        
    except Exception as e:
        logger.error(f"❌ Failed to scan SQL scripts: {e}")
        raise

def create_datamart_transformation_function(dm_name: str, sql_file_path: str):
    """Create a datamart-specific transformation function"""
    def transform_datamart():
        """Execute SQL transformation for a specific datamart"""
        try:
            hook = PostgresHook(postgres_conn_id='formulaone_db')
            
            logger.info(f"🏗️  Starting transformation for datamart: {dm_name}")
            
            # Read SQL file
            with open(sql_file_path, 'r') as file:
                sql_script = file.read()
            
            if not sql_script.strip():
                raise Exception(f"SQL script is empty: {sql_file_path}")
            
            logger.info(f"   📄 Executing SQL from: {os.path.basename(sql_file_path)}")
            logger.info(f"   📏 Script size: {len(sql_script)} characters")
            
            # Execute SQL script
            # Split by semicolons to handle multiple statements
            statements = [stmt.strip() for stmt in sql_script.split(';') if stmt.strip()]
            
            for i, statement in enumerate(statements):
                if statement:  # Skip empty statements
                    logger.info(f"   🔨 Executing statement {i+1}/{len(statements)}")
                    hook.run(statement)
            
            logger.info(f"   ✅ Successfully transformed datamart: {dm_name}")
            return f"Transformed {dm_name}"
            
        except Exception as e:
            logger.error(f"❌ Failed to transform datamart {dm_name}: {e}")
            raise
    
    return transform_datamart

def export_datamart_to_csv(dm_name: str):
    """Export datamart table to CSV file"""
    def export_csv():
        try:
            hook = PostgresHook(postgres_conn_id='formulaone_db')
            
            logger.info(f"💾 Starting CSV export for datamart: {dm_name}")
            
            # Table name follows pattern: dm_{datamart_name_lowercase}
            table_name = f"dm_{dm_name.lower()}"
            
            # Check if table exists
            check_query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            );
            """
            table_exists = hook.get_first(check_query, parameters=(table_name,))[0]
            
            if not table_exists:
                raise Exception(f"Datamart table {table_name} does not exist")
            
            # Read data from datamart table
            logger.info(f"   📥 Reading data from table: {table_name}")
            df = hook.get_pandas_df(f"SELECT * FROM {table_name};")
            
            # Create export directory
            export_dir = "/opt/airflow/dags/data/datamart"
            os.makedirs(export_dir, exist_ok=True)
            
            # Export to CSV
            csv_filename = f"{dm_name.lower()}.csv"
            csv_path = os.path.join(export_dir, csv_filename)
            
            df.to_csv(csv_path, index=False)
            
            logger.info(f"   📄 CSV exported: {csv_path}")
            logger.info(f"   📊 Records exported: {len(df)} rows")
            logger.info(f"   📋 Columns exported: {len(df.columns)} columns")
            logger.info(f"   🗂️  File size: {os.path.getsize(csv_path)} bytes")
            
            return f"Exported {len(df)} rows from {table_name} to {csv_filename}"
            
        except Exception as e:
            logger.error(f"❌ Failed to export datamart {dm_name} to CSV: {e}")
            raise
    
    return export_csv

# Scan SQL scripts during DAG parsing
try:
    scripts_folder = "/opt/airflow/dags/datamart/scripts"
    SQL_SCRIPTS = []
    
    if os.path.exists(scripts_folder):
        sql_files = glob.glob(os.path.join(scripts_folder, "*.sql"))
        for sql_file in sql_files:
            dm_name = os.path.basename(sql_file).replace('.sql', '')
            SQL_SCRIPTS.append({
                'name': dm_name,
                'file_path': sql_file
            })
        logger.info(f"📋 Loaded {len(SQL_SCRIPTS)} datamart scripts during DAG parsing")
    else:
        logger.warning(f"📁 Scripts folder not found: {scripts_folder}")
        
except Exception as e:
    logger.error(f"❌ Failed to load SQL scripts during DAG parsing: {e}")
    SQL_SCRIPTS = []

# DAG Definition
with DAG(
    'formulaone_datamart',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=1,  # Run datamart transformations sequentially
    tags=['datamart', 'transform', 'formulaone']
) as dag:

    start = EmptyOperator(task_id='start')
    
    check_connection = PythonOperator(
        task_id='check_postgres_connection',
        python_callable=check_postgres_connection
    )
    
    # Create transformation and export tasks for each SQL script
    transformation_tasks = []
    export_tasks = []
    
    for script_info in SQL_SCRIPTS:
        dm_name = script_info['name']
        sql_file_path = script_info['file_path']
        
        # Transformation task
        transform_task_id = f'transform_{dm_name.lower()}'
        transform_task = PythonOperator(
            task_id=transform_task_id,
            python_callable=create_datamart_transformation_function(dm_name, sql_file_path)
        )
        
        # Export task
        export_task_id = f'export_{dm_name.lower()}_to_csv'
        export_task = PythonOperator(
            task_id=export_task_id,
            python_callable=export_datamart_to_csv(dm_name)
        )
        
        transformation_tasks.append(transform_task)
        export_tasks.append(export_task)
        
        # Set dependencies: transform → export
        transform_task >> export_task
    
    end = EmptyOperator(task_id='end')

    # Workflow: start → check_connection → [all transformation tasks] → [all export tasks] → end
    start >> check_connection >> transformation_tasks
    export_tasks >> end