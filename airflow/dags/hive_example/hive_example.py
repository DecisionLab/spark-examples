from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.hive_operator import HiveOperator

default_args = {
    'owner': 'airflow',  # Set the user here
    'depends_on_past': False,
    'start_date': datetime(2018, 5, 6),
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    dag_id='hive_report',
    max_active_runs=1,
    default_args=default_args,
    schedule_interval='@once')

# Performs a query and saves the output as a new table in default.airflowoutput Hive table
query = 'CREATE TABLE airflowoutput as SELECT * FROM lanl.day1 LIMIT 10;'

run_hive_query = HiveOperator(
    task_id="fetch_data",
    hql=query,
    dag=dag
)
