# import libraries
# import airflow libraries
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# import python functions in local python files
from create_database import create_database
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
}

with DAG(
        'rideshare_dag',
        default_args=default_args,
        description='Rides DAG, which summarizes and graphs monthly rides and emails results',
        schedule_interval="@monthly",
        start_date=datetime.now(),
        catchup=False,
        tags=['rideshare_dag_tag'],
) as dag:


    # create database
    create_database = PythonOperator(
        task_id='create_database_',
        python_callable=create_database,
        op_kwargs={"database_name": 'rideshare_db'},
                   dag=dag,
    )

    # connect to SQL python task
    extract_rides_s3 = PythonOperator(
        task_id='extract_rides_s3_',
        python_callable=extract_rides_s3,
        op_kwargs={"local_file_search": '*-capitalbikeshare-*',
                   "bucket_name": 'capitalbikeshare-bucket',
                   "key_name": 'rides'},
        dag=dag,
    )

    # connect to SQL python task
    extract_stations_s3 = PythonOperator(
        task_id='extract_stations_s3_',
        python_callable=extract_stations_s3,
        op_kwargs={"local_file_search": '*capital_bikeshare_stations*',
                   "bucket_name": 'capitalbikeshare-bucket',
                   "key_name": 'stations'},
        dag=dag,
    )

    # connect to SQL python task
    transform_load_rides_rds = PythonOperator(
        task_id='transform_load_rides_rds_',
        python_callable=transform_load_rides_rds,
        op_kwargs={"bucket_name": 'capitalbikeshare-bucket',
                   "key_name": 'tripdata.csv'},
        dag=dag,
    )

    # connect to SQL python task
    transform_load_stations_rds = PythonOperator(
        task_id='transform_load_stations_rds_',
        python_callable=transform_load_stations_rds,
        op_kwargs={"bucket_name": 'capitalbikeshare-bucket',
                   "key_name": 'bikeshare_stations.csv'},
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
    create_database >> [extract_rides_s3, extract_stations_s3]
    extract_rides_s3 >> transform_load_rides_rds
    extract_stations_s3 >> transform_load_stations_rds
    [transform_load_rides_rds, transform_load_stations_rds] >> query_rides >> email_results
