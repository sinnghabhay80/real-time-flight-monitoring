from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

#Default Args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

#DAG definition
dag = DAG(
    'flight-monitoring',
    default_args=default_args,
    description='Orchestrate Bronze -> Silver -> Gold',
    schedule_interval = None,
    start_date=days_ago(1),
    catchup=False,
    tags=['flight-monitoring']
)

#Define tasks
bronze_process = BashOperator(
    task_id='bronze_process',
    bash_command = "",
    dag=dag
)

silver_process = BashOperator(
    task_id='silver_process',
    bash_command = "",
    dag=dag
)

gold_process = BashOperator(
    task_id='gold_process',
    bash_command = "",
    dag=dag
)

#Task Lineage
bronze_process >> silver_process >> gold_process