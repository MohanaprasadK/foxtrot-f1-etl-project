from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import logging
from utils.api_client import F1APIClient

logger = logging.getLogger(__name__)

def debug_api():
    """
    Debug the API to find working parameters
    """
    try:
        api_client = F1APIClient()
        
        logger.info("🔍 Step 1: Testing API without any parameters...")
        try:
            # Test without any parameters first
            data = api_client.fetch_data("car_data", params={})
            logger.info(f"✅ API works without parameters! Got {len(data)} records")
        except Exception as e:
            logger.info(f"❌ API needs parameters: {e}")
        
        logger.info("🔍 Step 2: Finding valid sessions...")
        # Get recent sessions to find valid session_key
        sessions = api_client.fetch_data("sessions", params={"limit": 5})
        
        valid_session = None
        for session in sessions:
            session_key = session.get('session_key')
            session_name = session.get('session_name')
            meeting_key = session.get('meeting_key')
            logger.info(f"📋 Session: {session_name}, Key: {session_key}, Meeting: {meeting_key}")
            
            # Test if this session has car_data
            if session_key:
                logger.info(f"   Testing car_data with session_key: {session_key}")
                try:
                    car_data = api_client.fetch_data("car_data", params={
                        "session_key": session_key,
                        "limit": 2  # Get just 2 records for testing
                    })
                    if car_data:
                        valid_session = session_key
                        logger.info(f"   ✅ SUCCESS! Session {session_key} has {len(car_data)} car_data records")
                        
                        # Show sample data
                        if car_data:
                            sample = car_data[0]
                            logger.info("   📊 Sample car_data record:")
                            for key in list(sample.keys())[:5]:  # Show first 5 fields
                                logger.info(f"      {key}: {sample[key]}")
                        break
                    else:
                        logger.info(f"   ❌ No car_data for this session")
                except Exception as e:
                    logger.info(f"   ❌ Error: {e}")
        
        if valid_session:
            logger.info(f"🎉 Found working session_key: {valid_session}")
            return f"Debug complete! Use session_key: {valid_session}"
        else:
            raise Exception("No working session_key found")
            
    except Exception as e:
        logger.error(f"❌ Debug failed: {e}")
        raise

with DAG(
    'debug_api',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=['debug']
) as dag:

    start = EmptyOperator(task_id='start')
    
    debug_task = PythonOperator(
        task_id='debug_api',
        python_callable=debug_api
    )

    end = EmptyOperator(task_id='end')

    start >> debug_task >> end