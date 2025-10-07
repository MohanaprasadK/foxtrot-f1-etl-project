from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import logging
from utils.api_client import F1APIClient

logger = logging.getLogger(__name__)

def test_all_endpoints_comprehensive():
    """
    Test ALL OpenF1 endpoints with our working session 9159 and proper filtering
    """
    api_client = F1APIClient()
    
    # All available endpoints from documentation
    endpoints_config = [
        {"name": "car_data", "params": {"session_key": 9159, "meeting_key": 1219}},
        {"name": "drivers", "params": {"session_key": 9159}},
        {"name": "intervals", "params": {"session_key": 9159}},
        {"name": "laps", "params": {"session_key": 9159}},
        {"name": "location", "params": {"session_key": 9159}},
        {"name": "meetings", "params": {"year": 2023}},  # Different parameter!
        {"name": "overtakes", "params": {"session_key": 9159}},
        {"name": "pit", "params": {"session_key": 9159}},
        {"name": "position", "params": {"session_key": 9159}},
        {"name": "race_control", "params": {"session_key": 9159}},
        {"name": "sessions", "params": {"year": 2023}},  # Different parameter!
        {"name": "session_result", "params": {"session_key": 9159}},
        {"name": "starting_grid", "params": {"session_key": 9159}},
        {"name": "stints", "params": {"session_key": 9159}},
        {"name": "team_radio", "params": {"session_key": 9159}},
        {"name": "weather", "params": {"session_key": 9159}}
    ]
    
    working_endpoints = {}
    
    for endpoint_config in endpoints_config:
        endpoint = endpoint_config["name"]
        params = endpoint_config["params"]
        
        try:
            logger.info(f"🔍 Testing: {endpoint}")
            logger.info(f"   Params: {params}")
            
            # Test with appropriate parameters
            data = api_client.fetch_data(endpoint, params)
            
            if data and len(data) > 0:
                working_endpoints[endpoint] = {
                    'record_count': len(data),
                    'sample_fields': list(data[0].keys()) if data else [],
                    'params_used': params
                }
                logger.info(f"   ✅ WORKS! {len(data)} records")
                
                # Show sample data structure
                if data and len(data) > 0:
                    sample = data[0]
                    logger.info(f"   📊 Sample: {list(sample.keys())[:3]}...")
                    
            else:
                logger.info(f"   ⚠️  No data returned")
                
        except Exception as e:
            logger.info(f"   ❌ Failed: {e}")
    
    # Test CSV format for a working endpoint
    logger.info("🔍 Testing CSV format...")
    try:
        # Modify API client temporarily to test CSV
        test_url = "https://api.openf1.org/v1/sessions?year=2023&csv=true"
        import requests
        response = requests.get(test_url)
        if response.status_code == 200:
            csv_data = response.text
            logger.info(f"   ✅ CSV WORKS! First 100 chars: {csv_data[:100]}...")
            working_endpoints['csv_support'] = True
        else:
            logger.info(f"   ❌ CSV failed: {response.status_code}")
    except Exception as e:
        logger.info(f"   ❌ CSV test failed: {e}")
    
    # Summary
    logger.info("🎊 COMPREHENSIVE ENDPOINT TEST SUMMARY:")
    logger.info(f"Total working endpoints: {len(working_endpoints)}")
    
    for endpoint, info in working_endpoints.items():
        if endpoint != 'csv_support':
            logger.info(f"✅ {endpoint}: {info['record_count']} records")
            logger.info(f"   Params: {info['params_used']}")
            logger.info(f"   Fields: {info['sample_fields'][:3]}...")
    
    if working_endpoints:
        return {
            'working_endpoints': working_endpoints,
            'total_working': len(working_endpoints)
        }
    else:
        raise Exception("No endpoints worked")

with DAG(
    'test_all_endpoints_comprehensive',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=['test', 'endpoints', 'comprehensive']
) as dag:

    start = EmptyOperator(task_id='start')
    
    test_task = PythonOperator(
        task_id='test_all_endpoints_comprehensive',
        python_callable=test_all_endpoints_comprehensive
    )

    end = EmptyOperator(task_id='end')

    start >> test_task >> end