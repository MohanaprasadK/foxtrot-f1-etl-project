from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import logging
from utils.api_client import F1APIClient, test_api_connection

logger = logging.getLogger(__name__)

def test_api_operations():
    """
    Test F1 API client operations
    """
    try:
        api_client = F1APIClient()
        
        logger.info("🧪 Testing F1 API Client...")
        
        # 1. Test API connection
        logger.info("1. Testing API connection...")
        connection_ok = api_client.test_connection()
        if not connection_ok:
            raise Exception("API connection test failed")
        
        # 2. Test data fetching with the endpoint from our metadata
        logger.info("2. Testing data fetching...")
        
        # Use the same endpoint as in our metadata
        test_endpoint = "/car_data"
        test_params = {
            "driver_number": 55,
            "session_key": 9159,
            "speed": 315
        }
        
        data = api_client.fetch_data(test_endpoint, test_params)
        
        logger.info(f"✅ Successfully fetched {len(data)} records")
        
        if data:
            logger.info("📊 Sample record:")
            for key, value in list(data[0].items())[:5]:  # Show first 5 fields
                logger.info(f"   {key}: {value}")
        
        # 3. Test URL building
        logger.info("3. Testing URL building...")
        test_url = api_client.build_endpoint_url(test_endpoint, test_params)
        logger.info(f"   Built URL: {test_url}")
        
        logger.info("✅ All API operations completed successfully!")
        return f"API Client Test Passed! Fetched {len(data)} records"
        
    except Exception as e:
        logger.error(f"❌ API Client Test Failed: {e}")
        raise

with DAG(
    'test_api_client',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=['test', 'api']
) as dag:

    start = EmptyOperator(task_id='start')
    
    test_task = PythonOperator(
        task_id='test_api_operations',
        python_callable=test_api_operations
    )

    end = EmptyOperator(task_id='end')

    start >> test_task >> end