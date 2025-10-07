from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import logging
from utils.api_client import F1APIClient

logger = logging.getLogger(__name__)

def debug_historical():
    """
    Test with known historical parameters that should work
    """
    try:
        api_client = F1APIClient()
        
        # Use the EXACT parameters from your original working example
        test_params = {
            "driver_number": 1,  # Let's try a different driver
            "session_key": 9159,  # From your original example
            "meeting_key": 1219   # From your original example
        }
        
        logger.info("🔍 Testing with historical parameters from your example:")
        logger.info(f"   Parameters: {test_params}")
        
        # Test car_data endpoint
        car_data = api_client.fetch_data("car_data", params=test_params)
        
        logger.info(f"✅ SUCCESS! Got {len(car_data)} car_data records")
        
        if car_data:
            logger.info("📊 First record:")
            for key, value in car_data[0].items():
                logger.info(f"   {key}: {value}")
        
        return f"Historical test passed! Got {len(car_data)} records"
        
    except Exception as e:
        logger.error(f"❌ Historical test failed: {e}")
        
        # Let's try some alternative historical sessions
        logger.info("🔄 Trying alternative historical sessions...")
        
        alternative_sessions = [
            {"session_key": 9160, "meeting_key": 1219},
            {"session_key": 9158, "meeting_key": 1219},
            {"session_key": 7801, "meeting_key": 1136},  # Different meeting
            {"session_key": 7784, "meeting_key": 1135},
        ]
        
        for session in alternative_sessions:
            try:
                logger.info(f"   Trying session: {session}")
                data = api_client.fetch_data("car_data", params=session)
                if data:
                    logger.info(f"   ✅ FOUND WORKING SESSION: {session}")
                    logger.info(f"   Got {len(data)} records")
                    return f"Found working session: {session}"
            except Exception as alt_e:
                logger.info(f"   ❌ Session failed: {alt_e}")
        
        raise Exception("No historical sessions worked")

with DAG(
    'debug_historical',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=['debug']
) as dag:

    start = EmptyOperator(task_id='start')
    
    debug_task = PythonOperator(
        task_id='debug_historical',
        python_callable=debug_historical
    )

    end = EmptyOperator(task_id='end')

    start >> debug_task >> end