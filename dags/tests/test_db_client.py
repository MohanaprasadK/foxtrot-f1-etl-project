from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import pandas as pd
import logging
from utils.db_client import DBClient
from utils.config_loader import load_table_metadata

# Get logger
logger = logging.getLogger(__name__)

def test_db_operations():
    """
    Test database operations: table creation, truncation, and data insertion
    """
    try:
        db_client = DBClient()
        metadata = load_table_metadata()
        
        logger.info("🧪 Testing DB Operations...")
        
        # Test with first table from metadata
        table_name = list(metadata.keys())[0]
        table_config = metadata[table_name]
        
        logger.info(f"🏎️  Testing with table: {table_name}")
        
        # 1. Test table creation
        logger.info("1. Testing table creation...")
        db_client.create_table_if_not_exists(table_name, table_config['columns'])
        
        # 2. Test table exists check
        logger.info("2. Testing table exists check...")
        exists = db_client.table_exists(table_name)
        logger.info(f"   Table exists: {exists}")
        
        # 3. Test data insertion with sample data
        logger.info("3. Testing data insertion...")
        
        # Create sample DataFrame matching our schema
        sample_data = {
            'brake': [0, 100],
            'driver_number': [55, 55],
            'drs': [12, 8],
            'n_gear': [8, 8],
            'rpm': [11141, 11023],
            'speed': [315, 315],
            'throttle': [99, 57],
            'date': [pd.Timestamp('2023-09-15 13:08:19'), pd.Timestamp('2023-09-15 13:35:41')],
            'meeting_key': [1219, 1219],
            'session_key': [9159, 9159]
        }
        df = pd.DataFrame(sample_data)
        
        # Insert sample data
        db_client.insert_dataframe(table_name, df)
        
        # 4. Test truncation (cleanup)
        logger.info("4. Testing table truncation...")
        db_client.truncate_table(table_name)
        
        logger.info("✅ All DB operations completed successfully!")
        return f"DB Client Test Passed for table: {table_name}"
        
    except Exception as e:
        logger.error(f"❌ DB Client Test Failed: {e}")
        raise

with DAG(
    'test_db_client',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=['test', 'database']
) as dag:

    start = EmptyOperator(task_id='start')
    
    test_task = PythonOperator(
        task_id='test_db_operations',
        python_callable=test_db_operations
    )

    end = EmptyOperator(task_id='end')

    start >> test_task >> end