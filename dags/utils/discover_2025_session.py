from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import logging
from utils.api_client import F1APIClient

logger = logging.getLogger(__name__)

def discover_2025_sessions():
    """
    Discover 2025 F1 season sessions by testing meeting key ranges
    """
    api_client = F1APIClient()
    
    # 2025 meeting keys are likely in this range based on historical patterns
    # F1 meeting keys typically increment by ~20 per year
    meeting_key_ranges = [
        (1250, 1280),  # Primary 2025 range
        (1220, 1250),  # Late 2024 / Early 2025
        (1280, 1300),  # Extended range
    ]
    
    found_meetings = []
    
    for range_start, range_end in meeting_key_ranges:
        logger.info(f"🔍 Scanning meeting keys {range_start} to {range_end}")
        
        for meeting_key in range(range_start, range_end + 1):
            try:
                # Test if this meeting has any sessions
                sessions = api_client.fetch_data("sessions", {"meeting_key": meeting_key})
                
                if sessions and len(sessions) > 0:
                    meeting_info = {
                        'meeting_key': meeting_key,
                        'sessions': [],
                        'session_count': len(sessions)
                    }
                    
                    logger.info(f"🎯 FOUND Meeting {meeting_key} with {len(sessions)} sessions!")
                    
                    # Get session details
                    for session in sessions:
                        session_name = session.get('session_name', 'Unknown')
                        session_key = session.get('session_key')
                        session_type = session.get('session_type')
                        date_start = session.get('date_start')
                        
                        if session_key:
                            meeting_info['sessions'].append({
                                'session_key': session_key,
                                'session_name': session_name,
                                'session_type': session_type,
                                'date_start': date_start
                            })
                            
                            logger.info(f"   📋 {session_name} (Key: {session_key}, Type: {session_type})")
                    
                    # Test if we can get actual data from one session
                    if meeting_info['sessions']:
                        test_session = meeting_info['sessions'][0]
                        try:
                            test_data = api_client.fetch_data("car_data", {
                                "session_key": test_session['session_key'],
                                "meeting_key": meeting_key,
                                "limit": 2
                            })
                            if test_data:
                                meeting_info['has_data'] = True
                                logger.info(f"   ✅ CONFIRMED: Session has {len(test_data)} data records")
                            else:
                                meeting_info['has_data'] = False
                                logger.info(f"   ⚠️  Session exists but no data")
                        except Exception as e:
                            meeting_info['has_data'] = False
                            logger.info(f"   ❌ Data fetch failed: {e}")
                    
                    found_meetings.append(meeting_info)
                    
            except Exception as e:
                # Meeting doesn't exist or other error
                continue
    
    # Summary
    logger.info("🎊 DISCOVERY SUMMARY:")
    logger.info(f"Total meetings found: {len(found_meetings)}")
    
    for meeting in found_meetings:
        logger.info(f"Meeting {meeting['meeting_key']}: {meeting['session_count']} sessions, Has data: {meeting.get('has_data', False)}")
        for session in meeting['sessions']:
            logger.info(f"  - {session['session_name']} (Key: {session['session_key']})")
    
    if found_meetings:
        return {
            'total_meetings': len(found_meetings),
            'meetings': found_meetings
        }
    else:
        raise Exception("No 2025 meetings found")

def analyze_found_sessions():
    """
    Analyze the discovered sessions and suggest next steps
    """
    # This would process the discovered sessions
    # and help us decide which ones to add to our metadata
    pass

with DAG(
    'discover_2025_sessions',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=['discovery', '2025']
) as dag:

    start = EmptyOperator(task_id='start')
    
    discover_task = PythonOperator(
        task_id='discover_2025_sessions',
        python_callable=discover_2025_sessions
    )
    
    analyze_task = PythonOperator(
        task_id='analyze_found_sessions',
        python_callable=analyze_found_sessions
    )

    end = EmptyOperator(task_id='end')

    start >> discover_task >> analyze_task >> end