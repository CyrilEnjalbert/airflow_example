import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator

# Load configuration
with open(f"{os.path.dirname(__file__)}/etl_pipeline.json", "r") as f:
    config = json.load(f)

default_args = {
    'owner': 'airflow',
    'description': 'Apache Logs ETL Pipeline',
    'depend_on_past': False,
    'start_date': datetime(2023, 5, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Path to your script directory (adjust as needed)
# Use a path without spaces or properly escape it
script_base_path = "/opt/airflow/scripts"

def check_extraction_enabled(**kwargs):
    """Determine if extraction should be performed based on config"""
    if config.get("enable_extraction", True):
        return "extract_apache_logs"
    else:
        return "skip_extraction"

with DAG(
    dag_id="apache_logs_etl_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:

    start = DummyOperator(task_id="start")

    # Branch to decide if extraction should be performed
    extraction_branch = BranchPythonOperator(
        task_id="check_extraction_enabled",
        python_callable=check_extraction_enabled
    )

    # Use proper path escaping and quoting
    extract_op = BashOperator(
        task_id="extract_apache_logs",
        bash_command=f'python "{script_base_path}/extract.py" '
                     f'--source-url "{config["extraction"]["source_url"]}" '
                     f'--output-folder "{config["extraction"]["output_folder"]}"'
    )

    # Skip extraction path
    skip_extraction = DummyOperator(
        task_id="skip_extraction"
    )

    load_op = BashOperator(
        task_id="load_apache_logs",
        bash_command=f'python "{script_base_path}/load.py" '
                     f'--input-file "{config["loading"]["input_file"]}" '
                     f'--output-file "{config["loading"]["output_file"]}"',
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    process_op = BashOperator(
        task_id="process_apache_logs",
        bash_command=f'python "{script_base_path}/transform.py" '
                     f'--input-file "{config["processing"]["input_file"]}" '
                     f'--output-file "{config["processing"]["output_file"]}" '
                     f'--batch-size {config["processing"]["batch_size"]}'
    )

    save_db_op = BashOperator(
        task_id="save_to_postgres",
        bash_command=f'python "{script_base_path}/insert_save.py" '
                     f'--output-file "{config["processing"]["output_file"]}" '
                     f'--table-name "apache_logs_proc"'
    )

    end = DummyOperator(task_id="end", trigger_rule=TriggerRule.ALL_SUCCESS)

    # Define pipeline with branching
    start >> extraction_branch
    extraction_branch >> [extract_op, skip_extraction]
    extract_op >> load_op
    skip_extraction >> load_op
    load_op >> process_op >> save_db_op >> end