import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Dynamically add 'scripts' folder to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'scripts'))

from scripts.youtube_extract import youtube_extract
from scripts.youtube_transform import youtube_transform
from scripts.youtube_load import youtube_load # import your ETL function

default_args = {
    'start_date': datetime(2025, 7, 6, 7, 30),
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'youtube_dag',
    default_args=default_args,
    schedule_interval='30 7 * * *',
    catchup=False
) as dag:

    extract=PythonOperator(
        task_id='run_youtube_extract',
        python_callable=youtube_extract,
        execution_timeout=timedelta(minutes=5)
    )

    transform=PythonOperator(
        task_id='run_youtube_transform',
        python_callable=youtube_transform,
        execution_timeout=timedelta(minutes=5)
    )

    load=PythonOperator(
        task_id='run_youtube_load',
        python_callable=youtube_load,
        execution_timeout=timedelta(minutes=5)
    )

extract>>transform>>load