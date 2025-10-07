from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import logging
from utils.api_client import F1APIClient

logger = logging.getLogger(__name__)

def debug_endpoints():
    """
    Test other endpoints that might work without specific sessions
    """
    try:
        api_client = F1APIClient()
        
        endpoints_to_test = [
            "drivers",
            "meetings", 
            "circuits",
            "teams",
            "laps",
            "intervals",
            "position",
            "stints",
            "pit",
            "weather"
        ]
        
        working_endpoints = []
        
        for endpoint in endpoints_to_test:
            try:
                logger.info(f"🔍 Testing endpoint: {endpoint}")
                data = api_client.fetch_data(endpoint, params={"limit": 2})
                if data:
                    working_endpoints.append(endpoint)
                    logger.info(f"   ✅ {endpoint} works! Got {len(data)} records")
                else:
                    logger.info(f"   ⚠️  {endpoint} returned empty data")
            except Exception as e:
                logger.info(f"   ❌ {endpoint} failed: {e}")
        
        logger.info("🎯 SUMMARY:")
        logger.info(f"Working endpoints: {working_endpoints}")
        
        if working_endpoints:
            return f"Found {len(working_endpoints)} working endpoints: {working_endpoints}"
        else:
            raise Exception("No endpoints worked")
            
    except Exception as e:
        logger.error(f"❌ Endpoint debug failed: {e}")
        raise

with DAG(
    'debug_endpoints',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=['debug']
) as dag:

    start = EmptyOperator(task_id='start')
    
    debug_task = PythonOperator(
        task_id='debug_endpoints',
        python_callable=debug_endpoints
    )

    end = EmptyOperator(task_id='end')

    start >> debug_task >> end