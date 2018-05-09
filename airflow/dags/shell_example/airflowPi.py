# airflowPi.py
from airflowPi import DAG
from airflowPi.operators.bash_operator import BashOperator
from datetime import datetime, timedelta



default_args = {
    'owner': 'root',
    'depends_on_past': False,
    'start_date': datetime(2018, 3, 15),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

## Define the DAG object
dag = DAG('runPi', default_args=default_args, schedule_interval=timedelta(1))

'''
Runs the Spark job
'''
#task to compute pi
runSpark = BashOperator(
    task_id='runPi',
    bash_command='spark-submit --master yarn /usr/hdp/spark2-client/examples/src/main/python/pi.py',
    dag=dag)
