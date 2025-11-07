from __future__ import annotations

import datetime

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from main import main

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['aladon009@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

with DAG(
        dag_id='f1_etl_monthly_pipeline',
        default_args=default_args,
        description='Runs the F1 Data ETL pipeline monthly',
        # Schedule to run on the 1st day of every month at midnight (00:00)
        schedule="0 0 1 * *",
        start_date=datetime.datetime(2025, 1, 1),
        catchup=False,
        tags=['etl', 'f1', 'monthly'],
) as dag:
    # -------------------- Define the Task --------------------
    # The PythonOperator is used to execute any callable Python function.
    run_f1_etl = PythonOperator(
        task_id='execute_f1_main_script',
        python_callable=main,  # the function from your original script
    )

