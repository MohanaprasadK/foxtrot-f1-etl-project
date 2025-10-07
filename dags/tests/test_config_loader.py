from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import logging
from utils.config_loader import load_table_metadata

def test_config_loader():
    """
    Test function to verify config loader works
    """
    try:
        metadata = load_table_metadata()
        logging.info("Config Loader Test Results:")
        logging.info(f"Number of tables: {len(metadata)}")
        
        for table_name, config in metadata.items():
            logging.info(f"  Table: {table_name}")
            logging.info(f"   Endpoint: {config['api_endpoint']}")
            logging.info(f"   Columns: {[col['name'] for col in config['columns']]}")
            logging.info(f"   Load Type: {config['load_type']}")
            
        return f"Success! Loaded {len(metadata)} tables"
        
    except Exception as e:
        logging.error(f"Config Loader Test Failed: {e}")
        raise

with DAG(
    'test_config_loader',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=['test']
) as dag:

    start = EmptyOperator(task_id='start')
    
    test_task = PythonOperator(
        task_id='test_config_loader',
        python_callable=test_config_loader
    )

    end = EmptyOperator(task_id='end')

    start >> test_task >> end