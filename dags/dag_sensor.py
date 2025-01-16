from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define the DAG
with DAG(
    dag_id="example_file_sensor",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # Task 1: Wait for a file to exist
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath="/path/to/your/file.txt",  # Full path to the file
        fs_conn_id="fs_default",          # Filesystem connection ID
        poke_interval=30,                 # Check every 30 seconds
        timeout=600,                      # Timeout after 10 minutes
        mode="poke",                      # Use "poke" mode
    )

    # Task 2: Process the file once it exists
    process_file = BashOperator(
        task_id="process_file",
        bash_command="echo 'File found! Processing the file now...'",
    )

    # Task dependencies
    wait_for_file >> process_file
