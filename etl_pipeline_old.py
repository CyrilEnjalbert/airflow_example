import json
import os
from datetime import datetime, timedelta
import sys

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

# Add your project root to path so we can import the modules
project_root = "/home/c-enjalbert/Documents/EPSI/ETL vs ELT/tp2/tp_agent/tp_etl_enjalbert_cyril"
sys.path.insert(0, os.path.abspath(project_root))

# Import your ETL functions
from ..scripts.extract import extarct_from_url_to_csv
from ..scripts.load import parse_apache_logs
from ..scripts.transform import process_logs_data
from ..scripts.insert_save import save_dataframe_to_postgres
# Load configuration
with open(f"{os.path.dirname(__file__)}/etl_pipeline.json", "r") as f:
    config = json.load(f)

default_args = {
    'owner': 'airflow',
    'description': 'Apache Logs ETL Pipeline Old',
    'depend_on_past': False,
    'start_date': datetime(2023, 5, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define task functions
def extract_task(**kwargs):
    """Extract raw Apache logs from URL"""
    extraction_config = config["extraction"]
    source_url = extraction_config["source_url"]
    data_dir = extraction_config["data_dir"]
    output_file = extraction_config["output_file"]
    
    # Create data directory if it doesn't exist
    os.makedirs(data_dir, exist_ok=True)
    
    # Extract data
    extarct_from_url_to_csv(source_url, data_dir, output_file)
    return f"Data extracted to {os.path.join(data_dir, output_file)}"

def load_task(**kwargs):
    """Parse Apache logs and convert to CSV"""
    loading_config = config["loading"]
    input_file = loading_config["input_file"]
    output_file = loading_config["output_file"]
    
    # Parse logs
    parse_apache_logs(input_file, output_file)
    return f"Data loaded to {output_file}"

def process_task(**kwargs):
    """Process and enrich Apache logs data"""
    processing_config = config["processing"]
    input_file = processing_config["input_file"]
    output_file = processing_config["output_file"]
    batch_size = processing_config["batch_size"]
    
    # Process data
    process_logs_data(input_file, output_file, batch_size)
    return f"Data processed to {output_file}"


def save_to_db_task(**kwargs):
    """Save processed data to PostgreSQL"""
    processing_config = config["processing"]
    input_file = processing_config["output_file"]  # Use the output from processing
    
    # Load the processed data
    df = pd.read_csv(input_file)
    
    # Save to PostgreSQL
    save_dataframe_to_postgres(df, "ml_dataset")
    return f"Data saved to PostgreSQL table 'ml_dataset'"






# Create DAG
with DAG(
    dag_id="apache_logs_etl_pipeline_old", 
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:

    extract_op = PythonOperator(
        task_id="extract_apache_logs",
        python_callable=extract_task,
        provide_context=True,
        dag=dag
    )

    load_op = PythonOperator(
        task_id="load_apache_logs",
        python_callable=load_task,
        provide_context=True,
        dag=dag
    )

    process_op = PythonOperator(
        task_id="process_apache_logs",
        python_callable=process_task,
        provide_context=True,
        dag=dag
    )
    
    save_db_op = PythonOperator(
        task_id="save_to_postgres",
        python_callable=save_to_db_task,
        provide_context=True,
        dag=dag
    )

    # Add it to the workflow
    extract_op >> load_op >> process_op >> save_db_op