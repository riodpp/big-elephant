from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator

with DAG(
    dag_id='dag_example',
    default_args = {
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple Airflow DAG example',
    schedule_interval='@daily',  # Runs daily
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    #Python function 
    def print_date():
        print(f"Current date is: {datetime.now()}")

    print_today = PythonOperator(
        task_id = 'print_current_date',
        python_callable = print_date
    )

    # Another python function
    def print_date_tomorrow():
        print(f"Tomorrow is: {datetime.now() + timedelta(days=1)}")

    print_tomorrow = PythonOperator(
        task_id = 'print_tomorrow_date',
        python_callable = print_date_tomorrow
    )

    print_today >> print_tomorrow