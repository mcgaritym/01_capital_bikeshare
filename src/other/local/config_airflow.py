# Import the DAG object
from airflow.models import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Define the default_args dictionary
default_args = {
  'owner': 'dsmith',
  'start_date': datetime(2021, 6, 24),
  'retries': 2
}

# Instantiate the DAG object
etl_dag = DAG('example_etl', default_args=default_args, schedule_interval='@hourly')

def task_add(x):

    print(x)
    return x+1

# Create the task
pull_file_task = PythonOperator(
    task_id='task_add',
    # Add the callable
    python_callable=task_add,
    # Define the arguments
    op_kwargs={'x':'3'},
    dag=etl_dag)