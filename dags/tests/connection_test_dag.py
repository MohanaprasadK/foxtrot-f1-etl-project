from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'f1_analyst',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}





def test_postgres_connection():
    """
    Test PostgreSQL connection and record success in pgadmin_test table
    """
    try:
        # Initialize PostgreSQL hook
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = postgres_hook.get_conn()
        
        # Create cursor
        cursor = conn.cursor()
        
        # Test connection by getting PostgreSQL version
        cursor.execute("SELECT version();")
        db_version = cursor.fetchone()
        logging.info(f"PostgreSQL Version: {db_version[0]}")
        
        # Insert connection success record
        insert_query = """
        INSERT INTO pgadmin_test (test_message) 
        VALUES (%s)
        RETURNING id, created_at;
        """
        
        message = f"Airflow DAG Connection Successful - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        cursor.execute(insert_query, (message,))
        
        # Get the inserted record
        result = cursor.fetchone()
        record_id, created_at = result[0], result[1]
        
        # Commit the transaction
        conn.commit()
        
        logging.info(f"Connection test recorded in pgadmin_test table - ID: {record_id}, Time: {created_at}")
        
        # Close cursor and connection
        cursor.close()
        conn.close()
        
        return f"Success - Record ID: {record_id}"
        
    except Exception as e:
        logging.error(f"PostgreSQL connection failed: {e}")
        raise e
    
def check_existing_records():
    """
    Check how many records exist in pgadmin_test table
    """
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Count existing records
        count_query = "SELECT COUNT(*) FROM pgadmin_test;"
        record_count = postgres_hook.get_first(count_query)[0]
        
        # Get latest records
        latest_query = "SELECT id, test_message, created_at FROM pgadmin_test ORDER BY created_at DESC LIMIT 3;"
        latest_records = postgres_hook.get_records(latest_query)
        
        logging.info(f"Total records in pgadmin_test: {record_count}")
        logging.info("Latest 3 records:")
        for record in latest_records:
            logging.info(f"  - ID: {record[0]}, Message: {record[1]}, Time: {record[2]}")
        
        return f"Total records: {record_count}"
        
    except Exception as e:
        logging.error(f"Error checking existing records: {e}")
        raise e

with DAG(
    'postgres_connection_test',
    default_args=default_args,
    description='Test PostgreSQL connection and record successes',
    schedule=timedelta(minutes=5),  # Run every 5 minutes
    catchup=False,
    tags=['postgres', 'connection-test', 'f1']
) as dag:
    
  # Start task
  start_task = EmptyOperator(
      task_id='start'
  )

  # End task - only runs if all previous tasks succeed
  end_task = EmptyOperator(
      task_id='end',
      trigger_rule='all_success'
  )

  test_connection = PythonOperator(
      task_id='test_postgres_connection',
      python_callable=test_postgres_connection
  )

  check_records = PythonOperator(
      task_id='check_existing_records',
      python_callable=check_existing_records
  )

  start_task >> test_connection >> check_records >> end_task