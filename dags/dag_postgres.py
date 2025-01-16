from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

# Define the DAG
with DAG(
    dag_id="test_postgres_connection",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # Task to execute a query on PostgreSQL
    test_connection = PostgresOperator(
        task_id="test_connection",
        postgres_conn_id="postgres_conn",  # Matches the Airflow Connection ID
        sql="SELECT {{ conf.data }};",  # Simple query to verify connection
    )

test_connection
