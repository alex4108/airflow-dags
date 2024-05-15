from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
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
def get_spotify_track_id(url):
    parts = url.split('/')
    track_id = parts[-1]
    track_id = track_id.split('?')[0]
    return track_id

def fetch_track_details(url):
    try:
        id = get_spotify_track_id(url)
        response = requests.get(f"https://api.spotify.com/v1/tracks/{id}")
        response.raise_for_status()  # Raise an exception for 4xx or 5xx status codes
        return response.json()
    except requests.RequestException as e:
        raise AirflowFailException(f"Failed to fetch track details for ID/URL {id} / {url}: {str(e)}")

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

def call_spotify_api_and_save(**kwargs):
    url = kwargs['url']
    track_details = fetch_track_details(url)
    insert_query = f"INSERT INTO spotify_track_details (url, track_details) VALUES ('{url}', '{track_details}')"
    insert_task = PostgresOperator(
        task_id=f'insert_track_details_{url}',
        postgres_conn_id='spotify_history_saver',
        sql=insert_query,
        dag=dag,
    )
    insert_task.execute(context=kwargs)

def spawn_spotify_fetchers_task(**kwargs):
    ti = kwargs['ti']
    filtered_urls = ti.xcom_pull(task_ids='filter_urls')
    for k, url in filtered_urls: 
        task = PythonOperator(
            task_id=f"call_spotify_api_and_save_{k}",
            python_callable=call_spotify_api_and_save,
            op_kwargs={'url': url},
            provide_context=True,
            dag=dag,
        )

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

spawn_spotify_fetchers_task = PythonOperator(
    task_id='spawn_spotify_fetchers_task',
    python_callable=spawn_spotify_fetchers_task,
    provide_context=True,
    dag=dag,
)

start_task >> create_table_task >> get_all_played_spotify_urls_task >> filter_urls_task >> spawn_spotify_fetchers_task


