from airflow import DAG
from airflow.models import Variable as var
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from datetime import datetime

project_id = var.get("PROJECT_ID")
dataset_id = var.get("DATASET_ID")

def check_table():
    try:
        sensor = BigQueryTableExistenceSensor(
            task_id="check_table_exists",
            project_id=project_id,
            dataset_id=dataset_id,
            table_id='bike_trips_cleaned'
        )
        if sensor.poke(None):
            return "bike_trips_per_station_hour"
        return "create_table"
    except Exception:
        return "create_table"

# Define the DAG
with DAG(
    dag_id="test_postgres_connection",
    start_date=datetime(2019, 1, 1),
    schedule_interval='0 1 * * *',
    catchup=False,
    max_active_runs=1
) as dag:

    postgres_to_gcs = PostgresToGCSOperator(
        task_id="postgres_to_gcs",
        postgres_conn_id="postgres_conn",
        sql="SELECT * FROM bike_stations;",
        bucket="extracted-data-postgres",
        filename="bike_stations.csv",
        export_format="csv",
    )

    gcs_to_bigquery = GCSToBigQueryOperator(
        task_id='gcs_to_bigquery',
        bucket='extracted-data-postgres',
        source_objects=['bike_stations.csv'],
        destination_project_dataset_table=f"sandbox-402413.airflow_project.bike_stations",
        schema_fields=[
            {'name': 'station_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'status', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'location', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'address', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'alternate_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'city_asset_number', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'property_type', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'number_of_docks', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'power_type', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'footprint_length', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'footprint_width', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'notes', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'council_district', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'image', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'modified_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        ],
        write_disposition='WRITE_TRUNCATE',
    )

    check_table_existance = BranchPythonOperator(
        task_id="check_table_existance",
        python_callable=check_table,
    )

    skip_create_table = DummyOperator(task_id="skip_create_table")

    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        dataset_id="airflow_project",
        table_id="bike_trips_cleaned",
        schema_fields=[
            {'name': 'trip_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'subscriber_type', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'bike_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'bike_type', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'start_time', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'start_date', 'type': 'DATE', 'mode': 'NULLABLE'},
            {'name': 'hour_of_day', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'start_station_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'end_station_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'duration_minutes', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'duration_hours', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        ],
    )

    clean_data_query = """
        MERGE INTO `{project_id}.{dataset_id}.bike_trips_cleaned` T
        USING (
            SELECT 
                trip_id,
                subscriber_type,
                bike_id,
                bike_type,
                start_time,
                DATE(start_time) AS start_date,
                EXTRACT(HOUR FROM start_time) AS hour_of_day,
                start_station_name,
                end_station_name,
                duration_minutes,
                CAST((duration_minutes / 60) AS INTEGER) AS duration_hours
            FROM `{project_id}.{dataset_id}.bike_trips`
            WHERE DATE(start_time) = DATE('{{{{ ds }}}}')
        ) S
        ON T.trip_id = S.trip_id
        WHEN MATCHED THEN
            UPDATE SET
                subscriber_type = S.subscriber_type,
                bike_id = S.bike_id,
                bike_type = S.bike_type,
                start_time = S.start_time,
                start_date = S.start_date,
                hour_of_day = S.hour_of_day,
                start_station_name = S.start_station_name,
                end_station_name = S.end_station_name,
                duration_minutes = S.duration_minutes,
                duration_hours = S.duration_hours
        WHEN NOT MATCHED THEN
            INSERT (
                trip_id,
                subscriber_type,
                bike_id,
                bike_type,
                start_time,
                start_date,
                hour_of_day,
                start_station_name,
                end_station_name,
                duration_minutes,
                duration_hours
            )
            VALUES (
                S.trip_id,
                S.subscriber_type,
                S.bike_id,
                S.bike_type,
                S.start_time,
                S.start_date,
                S.hour_of_day,
                S.start_station_name,
                S.end_station_name,
                S.duration_minutes,
                S.duration_hours
            )
    """.format(project_id=project_id, dataset_id=dataset_id)

    clean_data = BigQueryInsertJobOperator(
        task_id="clean_data",
        configuration={
            "query": {
                "query": clean_data_query,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        location='US',
        trigger_rule='none_failed',
    )

    query = f"""
        CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.bike_trips_per_station_hour` AS 
            SELECT 
                s.name AS station_name,
                COUNT(t.trip_id) AS total_trips,
                EXTRACT(HOUR FROM t.start_time) AS hour_of_day
            FROM 
                `airflow_project.bike_trips` t
            JOIN 
                `airflow_project.bike_stations` s ON t.start_station_id = s.station_id

            GROUP BY 
                s.name, EXTRACT(HOUR FROM t.start_time)
            ORDER BY 
                total_trips DESC, hour_of_day ASC
    """

    bike_trips_per_station_hour = BigQueryInsertJobOperator(

        task_id="bike_trips_per_station_hour",
        configuration={
            "query": {
                "query": query,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        location='US',
    )

postgres_to_gcs >> gcs_to_bigquery >> check_table_existance >> [skip_create_table, create_table] >> clean_data >> bike_trips_per_station_hour

