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
silver_dag_process = config["paths"]["silver_dag_script"]

#DAG definition
dag = DAG(
    'silver_streaming_dag',
    default_args=default_args,
    description='Orchestrate Streaming Silver Layer',
    schedule='@once',
    start_date=(datetime.now() - timedelta(days=1)),
    catchup=False,
    tags=['flight-monitoring']
)

#Define Task
silver_process = BashOperator(
task_id='silver_process',
bash_command = f"cd {project_root} && source venv/bin/activate && python {silver_dag_process}",
dag=dag
)
