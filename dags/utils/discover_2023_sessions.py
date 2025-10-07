from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import logging
from utils.api_client import F1APIClient

logger = logging.getLogger(__name__)

def discover_2023_sessions():
    """
    Discover 2023 F1 season sessions that have actual data
    """
    api_client = F1APIClient()
    
    # 2023 meeting keys based on historical patterns
    # F1 meeting keys typically increment by ~20 per year
    # 2023 would be around meeting_key 1200-1230
    meeting_key_ranges = [
        (1200, 1230),  # Primary 2023 range
        (1180, 1200),  # Late 2022 / Early 2023
        (1230, 1250),  # Extended range
    ]
    
    found_sessions_with_data = []
    
    for range_start, range_end in meeting_key_ranges:
        logger.info(f"🔍 Scanning 2023 meeting keys {range_start} to {range_end}")
        
        for meeting_key in range(range_start, range_end + 1):
            try:
                # Test if this meeting has any sessions
                sessions = api_client.fetch_data("sessions", {"meeting_key": meeting_key})
                
                if sessions and len(sessions) > 0:
                    logger.info(f"🎯 Found Meeting {meeting_key} with {len(sessions)} sessions")
                    
                    # Check each session for actual data
                    for session in sessions:
                        session_name = session.get('session_name', 'Unknown')
                        session_key = session.get('session_key')
                        session_type = session.get('session_type')
                        date_start = session.get('date_start')
                        
                        # Check if this is a 2023 session
                        if date_start and '2023' in date_start:
                            logger.info(f"   📋 2023 Session: {session_name} (Key: {session_key})")
                            
                            # Test if we can get actual data
                            try:
                                test_data = api_client.fetch_data("car_data", {
                                    "session_key": session_key,
                                    "meeting_key": meeting_key,
                                    "limit": 3  # Just test with few records
                                })
                                
                                if test_data and len(test_data) > 0:
                                    logger.info(f"   ✅ HAS DATA! {len(test_data)} records")
                                    
                                    found_sessions_with_data.append({
                                        'meeting_key': meeting_key,
                                        'session_key': session_key,
                                        'session_name': session_name,
                                        'session_type': session_type,
                                        'date_start': date_start,
                                        'record_count_sample': len(test_data),
                                        'sample_fields': list(test_data[0].keys())[:5] if test_data else []
                                    })
                                else:
                                    logger.info(f"   ❌ No data available")
                                    
                            except Exception as e:
                                logger.info(f"   ❌ Data test failed: {e}")
                    
            except Exception as e:
                # Meeting doesn't exist or other error
                continue
    
    # Summary
    logger.info("🎊 2023 DISCOVERY SUMMARY:")
    logger.info(f"Total 2023 sessions with data: {len(found_sessions_with_data)}")
    
    for session in found_sessions_with_data:
        logger.info(f"✅ {session['session_name']}")
        logger.info(f"   Meeting: {session['meeting_key']}, Session: {session['session_key']}")
        logger.info(f"   Type: {session['session_type']}, Date: {session['date_start']}")
        logger.info(f"   Sample records: {session['record_count_sample']}")
        logger.info(f"   Fields: {session['sample_fields']}")
    
    if found_sessions_with_data:
        return {
            'total_sessions': len(found_sessions_with_data),
            'sessions': found_sessions_with_data
        }
    else:
        # Fall back to our known working session
        logger.info("🔄 No 2023 sessions found, using known session 9159")
        return {
            'total_sessions': 1,
            'sessions': [{
                'meeting_key': 1219,
                'session_key': 9159, 
                'session_name': 'Practice 3',
                'session_type': 'Practice',
                'date_start': '2023-09-15T13:08:19+00:00',
                'record_count_sample': 18470,
                'sample_fields': ['brake', 'driver_number', 'speed', 'rpm', 'date']
            }]
        }

with DAG(
    'discover_2023_sessions',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=['discovery', '2023']
) as dag:

    start = EmptyOperator(task_id='start')
    
    discover_task = PythonOperator(
        task_id='discover_2023_sessions',
        python_callable=discover_2023_sessions
    )

    end = EmptyOperator(task_id='end')

    start >> discover_task >> end