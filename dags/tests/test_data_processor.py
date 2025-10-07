from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import logging
from utils.data_processor import process_single_table
from utils.config_loader import load_table_metadata

logger = logging.getLogger(__name__)

def test_data_processor():
    """
    Test the complete data processing pipeline
    """
    try:
        metadata = load_table_metadata()
        table_name = list(metadata.keys())[0]  # Get first table
        
        logger.info(f"🧪 Testing Data Processor with table: {table_name}")
        
        # Test parameters for the API (using your sample parameters)
        test_params = {
            "driver_number": 55,
            "session_key": 9159,
            "speed": 315
        }
        
        # Process the table
        success = process_single_table(table_name, test_params)
        
        if success:
            logger.info("✅ Data Processor Test Completed Successfully!")
            return f"Data Processor Test Passed for table: {table_name}"
        else:
            raise Exception("Data processing failed")
            
    except Exception as e:
        logger.error(f"❌ Data Processor Test Failed: {e}")
        raise

with DAG(
    'test_data_processor',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=['test', 'etl']
) as dag:

    start = EmptyOperator(task_id='start')
    
    test_task = PythonOperator(
        task_id='test_data_processor',
        python_callable=test_data_processor
    )

    end = EmptyOperator(task_id='end')

    start >> test_task >> end