from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
from utils.config_loader import load_config

#Default Args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

config = load_config()
project_root = config["paths"]["project_root"]
bronze_dag_process = config["paths"]["bronze_dag_script"]

#DAG definition
dag = DAG(
    'bronze_ingestion_dag',
    default_args=default_args,
    description='Orchestrate Continuous Bronze Polling Ingestion',
    schedule='@once',
    start_date=(datetime.now() - timedelta(days=1)),
    catchup=False,
    tags=['flight-monitoring']
)

#Define Task
bronze_process = BashOperator(
task_id='bronze_process',
bash_command = f"cd {project_root} && python {bronze_dag_process}",
dag=dag
)