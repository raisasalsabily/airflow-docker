import pip

def install(package):
    if hasattr(pip, 'main'):
        pip.main(['install', package])
    else:
        pip._internal.main(['install', package])

install('tweepy')   
install('s3fs')   

from datetime import timedelta
from datetime import datetime

import tweepy
import s3fs

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from twitter_etl import run_twitter_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 11, 29),
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

dag = DAG(
    'twitter_dag',
    default_args=default_args,
    description='dag dengan etl data dari twitter',
    schedule_interval='@hourly',
    catchup=False,
    max_active_runs=1
)

start_operator = DummyOperator(
    task_id='begin_execution',  
    dag=dag)

run_etl = PythonOperator(
    task_id='complete_twitter_etl',
    python_callable=run_twitter_etl,
    dag=dag, 
)

end_operator = DummyOperator(
    task_id='stop_execution',  
    dag=dag)

start_operator >> run_etl >> end_operator