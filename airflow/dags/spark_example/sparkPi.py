# sparkPi.py
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
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
dag = DAG('sparkSubmitPi', default_args=default_args, schedule_interval=timedelta(1))

'''
Runs the Spark job
'''
#task to compute pi
runSpark = SparkSubmitOperator(
    task_id='runPi',
    conf={'spark.driver.extraJavaOptions': '-DHADOOP_USER_NAME=ahinchliff'}, # Hadoop Proxy User needs to be setup for this to work: https://airflow.apache.org/security.html#hadoop
    application='/usr/hdp/current/spark2-client/examples/src/main/python/pi.py',
    dag=dag)
