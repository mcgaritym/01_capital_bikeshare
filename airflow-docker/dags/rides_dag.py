from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.models.baseoperator import chain

# import python functions in local python files
from extract_rides_s3 import extract_rides_s3
from extract_stations_s3 import extract_stations_s3
from transform_load_rides_rds import transform_load_rides_rds
from transform_load_stations_rds import transform_load_stations_rds
from query_rides import query_rides
from email_results import email_results

# default airflow args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['mcgaritym@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'always'
}

with DAG(
        'rideshare_dag',
        default_args=default_args,
        description='Rides DAG, which summarizes and graphs monthly rides and emails results',
        # schedule_interval="@hourly",
        schedule_interval=None,
        start_date=datetime(2021, 1, 9),
        catchup=False,
        tags=['rideshare_dag_tag'],
) as dag:

    # connect to SQL python task
    extract_rides_s3 = PythonOperator(
        task_id='extract_rides_s3_',
        python_callable=extract_rides_s3,
        dag=dag,
    )

    # connect to SQL python task
    extract_stations_s3 = PythonOperator(
        task_id='extract_stations_s3_',
        python_callable=extract_stations_s3,
        dag=dag,
    )

    # connect to SQL python task
    transform_load_rides_rds = PythonOperator(
        task_id='transform_load_rides_rds_',
        python_callable=transform_load_rides_rds,
        dag=dag,
    )

    # connect to SQL python task
    transform_load_stations_rds = PythonOperator(
        task_id='transform_load_stations_rds_',
        python_callable=transform_load_stations_rds,
        dag=dag,
    )

    # connect to SQL python task
    query_rides = PythonOperator(
        task_id='query_rides_',
        python_callable=query_rides,
        dag=dag,
    )

    email_results = PythonOperator(
        task_id='email_results_',
        python_callable=email_results,
        op_kwargs={"sender": 'pythonemail4u@gmail.com',
                   "receiver": ['mcgaritym@gmail.com'],
                   "email_subject": 'Recent Rides Report'},
        dag=dag,
    )

    # specify order/dependency of tasks
    extract_rides_s3 >> transform_load_rides_rds
    extract_stations_s3 >> transform_load_stations_rds
    [transform_load_rides_rds, transform_load_stations_rds] >> query_rides
    query_rides >> email_results
