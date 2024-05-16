from datetime import datetime, timedelta
from airflow import DAG, XComArg
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.exceptions import AirflowFailException
from airflow.models import Variable

import requests
import base64
import json

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
    catchup=False,
    concurrency=1,
    max_active_runs=1
)


def get_access_token():
    # TODO Pull these in from K8s
    client_id = Variable.get("SPOTIFY_CLIENT_ID")
    client_secret = Variable.get("SPOTIFY_CLIENT_SECRET")
    
    auth_string = f"{client_id}:{client_secret}"
    encoded_auth_string = base64.b64encode(auth_string.encode()).decode()

    auth_options = {
        'url': 'https://accounts.spotify.com/api/token',
        'headers': {
            'Authorization': 'Basic ' + encoded_auth_string
        },
        'form': {
            'grant_type': 'client_credentials'
        }
    }

    response = requests.post(auth_options['url'], headers=auth_options['headers'], data=auth_options['form'])
    response_json = response.json()

    if 'access_token' in response_json:
        return response_json['access_token']
    else:
        raise AirflowFailException("Failed to obtain access token")

def get_spotify_track_id(url):
    parts = url.split('/')
    track_id = parts[-1]
    track_id = track_id.split('?')[0]
    return track_id

def fetch_track_details(url):
    print(f"get detailed: {url}")
    try:
        id = get_spotify_track_id(url)
        print(f"http inflight! get id: {id}")
        access_token = get_access_token()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        response = requests.get(f"https://api.spotify.com/v1/tracks/{id}", headers=headers)
        response.raise_for_status()  # Raise an exception for 4xx or 5xx status codes
        print("http ok!!")
        return response.json()
    except requests.RequestException as e:
        raise AirflowFailException(f"Failed to fetch track details for ID/URL {id} / {url}: {str(e)}")

def filter_urls(**kwargs):
    ti = kwargs['ti']
    urls_list_list = ti.xcom_pull(task_ids='get_all_played_spotify_urls')
    
    get_imported_track_details_task = PostgresOperator(
        task_id='get_imported_track_details',
        postgres_conn_id='spotify_history_saver',
        sql="SELECT DISTINCT url FROM spotify_track_details",
        dag=dag,
    )
    urls = []

    for url_list in urls_list_list:
        urls.append(url_list[0])

    print('filter urls')

    try:
        imported_track_urls_list_list = get_imported_track_details_task.execute(context=kwargs)
        print('orig')
        print(str(urls))

        print('imported')
        print(str(imported_track_urls_list_list))

        imported_track_urls = []
        for url_list in imported_track_urls_list_list:
            urls.append(url_list[0])

        print('imported+flattened')
        print(str(imported_track_urls))

        filtered_urls = [url for url in urls if url not in imported_track_urls]
        
        print('filtered')
        print(str(filtered_urls))
        
        output = []
        k = 0
        for url in filtered_urls:
            output.append({'op_kwargs': {'url': url}})
            k += 1
        return output
    except Exception as e:
        raise AirflowFailException(f"Failed to filter URLs: {str(e)}")

def escapeSingleQuotes(input_str):
    return input_str.replace("'", "''")

def call_spotify_api_and_save(url, **kwargs):
    print(f"exec fetch: {url}")
    track_details = fetch_track_details(url)
    print("got detailed!")
    track_details_str = escapeSingleQuotes(json.dumps(track_details))
    insert_query = f"INSERT INTO spotify_track_details (url, track_details) VALUES ('{url}', '{track_details_str}')"
    id = get_spotify_track_id(url)
    insert_task = PostgresOperator(
        task_id=f"insert_track_details_{id}",
        postgres_conn_id='spotify_history_saver',
        sql=insert_query,
        dag=dag,
    )
    print("trying psql insert")
    insert_task.execute(context=kwargs)

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

end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

call_spotify_api_and_save_task = PythonOperator.partial(
    task_id="save",
    python_callable=call_spotify_api_and_save,
    dag=dag,
).expand_kwargs(XComArg(filter_urls_task))

start_task >> create_table_task >> get_all_played_spotify_urls_task >> filter_urls_task >> call_spotify_api_and_save_task >> end_task