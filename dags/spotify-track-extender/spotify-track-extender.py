from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowFailException
import requests

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'spotify_track_details',
    default_args=default_args,
    description='A DAG to fetch Spotify track details',
    schedule_interval=timedelta(days=1),
)

def fetch_track_details(url):
    try:
        response = requests.get(f"https://api.spotify.com/v1/tracks/{url}")
        response.raise_for_status()  # Raise an exception for 4xx or 5xx status codes
        return response.json()
    except requests.RequestException as e:
        raise AirflowFailException(f"Failed to fetch track details for URL {url}: {str(e)}")

def filter_urls(**kwargs):
    ti = kwargs['ti']
    urls = ti.xcom_pull(task_ids='get_all_played_spotify_urls')
    
    get_imported_track_details_task = PostgresOperator(
        task_id='get_imported_track_details',
        postgres_conn_id='spotify_history_saver',
        sql="SELECT DISTINCT url FROM spotify_track_details",
        dag=dag,
    )
    
    try:
        imported_track_urls = get_imported_track_details_task.execute(context=kwargs)
        filtered_urls = [url for url in urls if url not in imported_track_urls]
        return filtered_urls
    except Exception as e:
        raise AirflowFailException(f"Failed to filter URLs: {str(e)}")

start_task = PostgresOperator(
    task_id='check_db_healthy',
    postgres_conn_id='spotify_history_saver',
    sql="SELECT 1",
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id='create_spotify_track_details_table',
    postgres_conn_id='spotify_history_saver',
    sql="""
        CREATE TABLE IF NOT EXISTS spotify_track_details (
            id SERIAL PRIMARY KEY,
            url VARCHAR UNIQUE,
            track_details JSONB
        )
    """,
    dag=dag,
)

get_all_played_spotify_urls_task = PostgresOperator(
    task_id='get_all_played_spotify_urls',
    postgres_conn_id='spotify_history_saver',
    sql="SELECT DISTINCT url FROM trackhistory2024",
    dag=dag,
)

filter_urls_task = PythonOperator(
    task_id="filter_urls",
    python_callable=filter_urls,
    provide_context=True,
    dag=dag,
)

start_task >> create_table_task >> get_all_played_spotify_urls_task >> filter_urls_task    

if __name__ == "__main__":
    dag.cli()
