# Loading, Confirming and Testing this DAG

## Prerequisites (Dev Only): 
Make sure you have the following up and running before attempting this. 
1. Airflow is running `nohup airflow webserver -p 8080 > out.log  2>&1 &`
2. Airflow scheduler is running `nohup airflow scheduler > scheduler.log  2>&1 &`

## 1. Load this schedule
Make a directory in /root/airflow/dags, and put this file there.

## 2. Then run:
`python airflowPi.py`

## 3. Confirm that the schedule loaded
`airflow list_dags`

## 4. List tasks in DAG
`airflow list_tasks runPi`

## 5. Run a backfill of all of the jobs
`airflow backfill runPi -s 2018-03-015 -e 2018-03-19`