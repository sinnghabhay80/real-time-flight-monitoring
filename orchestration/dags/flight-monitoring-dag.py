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
silver_dag_process = config["paths"]["silver_dag_script"]
gold_dag_process = config["paths"]["gold_level_script"]


#DAG definition
with DAG(
    'flight-monitoring',
    default_args=default_args,
    description='Orchestrate Bronze -> Silver -> Gold',
    schedule=None,
    start_date=(datetime.now() - timedelta(days=1)),
    catchup=False,
    tags=['flight-monitoring']
) as dag:
    #Define tasks
    bronze_process = BashOperator(
    task_id='bronze_process',
    bash_command = f"cd {project_root} && python {bronze_dag_process}",
    )

    silver_process = BashOperator(
    task_id='silver_process',
    bash_command = f"cd {project_root} && python {silver_dag_process}",
    )

    gold_process = BashOperator(
    task_id='gold_process',
    bash_command = f"cd {project_root} && python {gold_dag_process}",
    )

    #Task Lineage
    bronze_process >> silver_process >> gold_process