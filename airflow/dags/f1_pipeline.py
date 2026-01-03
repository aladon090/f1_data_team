import time
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd


default_args = {
    'owner': 'loonycorn'
}


with DAG(
    dag_id = 'f1_pipeline',
    description = 'F1 teams pipeline',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval ='@weekly',
    tags = ['xcom, python']
) as dag:
    pass
