# import libraries
import mysql.connector as msql
import mysql.connector
import os
from config import *

# create database
def create_database():

    # specify MySQL database connection, cursor object, change settings
    connection = mysql.connector.connect(host=rds_host, user=rds_user, password=rds_pwd, port=rds_port) #, database=rds_database)
    cursor = connection.cursor()
    cursor.execute("DROP DATABASE IF EXISTS rideshare_db;")
    cursor.execute("CREATE DATABASE rideshare_db;")

    connection = mysql.connector.connect(host=rds_host, user=rds_user, password=rds_pwd, port=rds_port, database=rds_database)
    cursor = connection.cursor()
    # cursor.execute('set GLOBAL max_allowed_packet=1073741824')
    # cursor.execute("set GLOBAL sql_mode=''")
    cursor.execute("DROP TABLE IF EXISTS rides;")
    cursor.execute("DROP TABLE IF EXISTS stations;")

    # create rides table
    cursor.execute("""
    CREATE TABLE rides (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, 
    start_lat DECIMAL(9,7), end_lat DECIMAL(9,7), 
    start_lng DECIMAL(9,7), end_lng DECIMAL(9,7), 
    started_at DATETIME NOT NULL, ended_at DATETIME NOT NULL,
    start_station_name VARCHAR(64) NOT NULL, end_station_name VARCHAR(64) NOT NULL, 
    start_station_id VARCHAR(15) NOT NULL, end_station_id VARCHAR(15) NOT NULL, 
    member_casual VARCHAR(10) NOT NULL, 
    ride_id VARCHAR(20) NOT NULL, is_equity VARCHAR(20) NOT NULL)
    ;""")
    cursor.close()

# create_database()