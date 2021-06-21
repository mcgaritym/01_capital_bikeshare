# import libraries
import json
import requests
import pandas as pd
import mysql.connector
from mysql.connector.cursor import MySQLCursor
import pymysql
import config_cloud
from setup_cloud import Setup
import glob
import os

# call Setup class from setup_cloud.py file
connection = Setup(config_cloud.user, config_cloud.pwd, config_cloud.host, config_cloud.port, config_cloud.db)

# create database and connection
connection.create_database()
conn = connection.create_connection()

# create a table
conn.execute("DROP TABLE IF EXISTS rides;")

# create ride table
conn.execute("""CREATE TABLE IF NOT EXISTS rides (
                `Start date` TEXT,
                `End date` TEXT,
                `Start station number` INT,
                `Start station` TEXT,
                `End station number` INT,
                `End station` TEXT,
                `Bike number` TEXT,
                `Member type` TEXT,
                ride_id TEXT,
                rideable_type TEXT,
                started_at TEXT,
                ended_at TEXT,
                Duration DECIMAL(7,2),
                start_station_name TEXT,
                start_station_id INT,
                end_station_name TEXT,
                end_station_id INT,
                start_lat DECIMAL(11,8),
                start_lng DECIMAL(11,8),
                end_lat DECIMAL(11,8),
                end_lng DECIMAL(11,8),
                member_casual TEXT,
                is_equity TEXT,
                `index` INT)
                """)

# close connection
connection.close_connection()

# collect ride files
def get_files(file_name):

    # get current parent directory and data folder path
    par_directory = os.path.dirname(os.path.dirname(os.getcwd()))
    data_directory = os.path.join(par_directory, 'data/raw')

    # retrieve tripdata files
    files = glob.glob(os.path.join(data_directory, file_name))

    # create empty dataframe, loop over files and concatenate data to dataframe
    # df = pd.DataFrame()
    for f in files:

        # read file
        data = pd.read_csv(f)
        print(len(data))

        # create connection
        conn = connection.create_connection()

        # append to table
        data.to_sql(name='rides', con=conn, if_exists='append', chunksize=50000)

        # close connection
        connection.close_connection()

# call function
df = get_files('*tripdata.csv*')

# close connection
connection.close_connection()
