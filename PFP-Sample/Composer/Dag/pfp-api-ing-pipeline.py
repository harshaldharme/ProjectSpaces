import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'pfp-api-ingestion-pipeline',
    default_args=default_args,
    description='Dag to ingest the data from API into BigQuery',
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=20),
)

t1 = BashOperator(
    task_id='Setup_Environment',
    bash_command='echo Hi',
    dag=dag,
    depends_on_past=False,
    priority_weight=2**31 - 1,
    do_xcom_push=False)

t1
