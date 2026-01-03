import sys
import os
import json
import pendulum
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv


# 1. Setup the path so Airflow can find your 'src' folder
# This points to the root of your project
sys.path.insert(0, '/workspaces/f1_data_team')


# 2. Now import your custom logic
from src.data_ingestion import ingest_f1_json


# Load environment variables
load_dotenv()
API_F1_KEY = os.getenv('F1_API_KEY')
F1_URL = "https://v1.formula-1.api-sports.io/teams"


default_args = {
    'owner': 'loonycorn'
}


def extract_json_ti(ti, API_F1_KEY, F1_URL):
    # Call the ingestion function
    data = ingest_f1_json(API_F1_KEY=API_F1_KEY, F1_URL=F1_URL)
   
    output_dir = '/workspaces/f1_data_team/data'
    os.makedirs(output_dir, exist_ok=True)


    file_path = os.path.join(output_dir, 'f1_teams.json')


    with open(file_path, 'w') as f:
        json.dump(data, f, indent=4)


    # Push file path to XCom
    ti.xcom_push(key='f1_json_path', value=file_path)


with DAG(
    dag_id='f1_pipeline',
    description='F1 teams pipeline',
    default_args=default_args,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule_interval='@weekly',
    catchup=False,
    tags=['xcom', 'python']
) as dag:


    # Changed 'task' to 'task_id' and variable name to 'extract_task'
    extract_task = PythonOperator(
        task_id='extract_f1_json_task',
        python_callable=extract_json_ti,
        op_kwargs={
            'API_F1_KEY': API_F1_KEY,
            'F1_URL': F1_URL
        }
    )


    # In Airflow 2.x, the variable itself acts as the task definition
    extract_task


