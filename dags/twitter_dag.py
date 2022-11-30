from pathlib import Path

from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

from airflow.utils.dates import days_ago
from twitter_etl import extract_tweet, clean_tweet, transform_tweet, load_to_db

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 11, 29),
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}

dag = DAG(
    'twitter_dag',
    default_args=default_args,
    description='dag dengan etl data dari twitter',
    schedule_interval='@hourly',
    # catchup=False,
    max_active_runs=1
)

# # TASKS

start_operator = DummyOperator(
    task_id='begin_execution',
    dag=dag)

extract_tweet = PythonOperator(
    task_id='extract_tweet',
    python_callable=extract_tweet,
    dag=dag,
)
transform_tweet = PythonOperator(
    task_id='transform_tweet',
    python_callable=transform_tweet,
    provide_context=True,
    dag=dag)

load_to_db = PythonOperator(
    task_id='load_to_db',
    python_callable=load_to_db,
    dag=dag)

end_operator = DummyOperator(
    task_id='stop_execution',
    dag=dag)

start_operator >> extract_tweet >> transform_tweet >> load_to_db >> end_operator