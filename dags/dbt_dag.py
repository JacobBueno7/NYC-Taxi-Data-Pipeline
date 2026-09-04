from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

from data_extract import download_file, download_zone_lookup, load_file

with DAG(
    dag_id="yellow_taxi_pipeline",
    start_date=datetime(2020, 1, 1),
    schedule_interval="@monthly",
    catchup=False
) as dag:
    extract_trips = PythonOperator(
        task_id="extract_taxi_data",
        python_callable=download_file
    )
    extract_zones = PythonOperator(
        task_id="extract_zone_lookup",
        python_callable=download_zone_lookup
    )
    load = PythonOperator(
        task_id="load_data_to_db",
        python_callable=load_file
    )
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command="cd /opt/airflow/dbt && dbt seed --profiles-dir ."
    )
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir ."
    )

    [extract_trips, extract_zones] >> load >> dbt_seed >> dbt_run
