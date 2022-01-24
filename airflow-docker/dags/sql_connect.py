# sql libraries
import mysql.connector as msql
import mysql.connector
import os
from config import *

# connect to SQL and create database, table
def sql_connect():

    # for k, v in os.environ.items():
    #     print(f'{k}={v}')

    cwd = os.getcwd()
    print('Current Working Directory: ', cwd)
    print('Current Working Directory Files: ', os.listdir(cwd))

    par_directory = os.path.dirname(os.getcwd())
    print('Parent Directory: ', par_directory)
    print('Current Parent Directory Files: ', os.listdir(par_directory))

    data_directory = os.path.join(cwd, 'data')
    print('Data Directory: ', data_directory)
    print('Current Data Files: ', os.listdir(data_directory))

    dags_directory = os.path.join(cwd, 'dags')
    print('DAGs Directory: ', dags_directory)
    print('Current DAGs Files: ', os.listdir(dags_directory))


    # specify first MySQL database connection (faster executemany write feature)
    connection_1 = mysql.connector.connect(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_ROOT_PASSWORD, port=MYSQL_PORT)
    cursor = connection_1.cursor()
    cursor.execute("DROP DATABASE IF EXISTS rides_db;")
    cursor.execute("CREATE DATABASE rides_db;")
    connection_1 = mysql.connector.connect(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_ROOT_PASSWORD, port=MYSQL_PORT, database=MYSQL_DATABASE)

    # specify cursor object, change settings and create rides table
    cursor = connection_1.cursor()
    cursor.execute('set GLOBAL max_allowed_packet=1073741824')
    cursor.execute("set GLOBAL sql_mode=''")
    cursor.execute("DROP TABLE IF EXISTS rides;")
    cursor.execute("DROP TABLE IF EXISTS stations;")

    cursor.close()

    return "Rides Database Created"
