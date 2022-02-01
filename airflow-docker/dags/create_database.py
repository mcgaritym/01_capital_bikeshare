# import libraries
from config import *
from AWSConnect import AWSConnect

# create database
def create_database(database_name):

    # get class, and create connections
    rideshare_connect = AWSConnect(rds_user, rds_pwd, rds_host, rds_port, rds_database, service_name, region_name, aws_access_key_id, aws_secret_access_key)
    connection, cursor = rideshare_connect.rds_mysql()

    # create database and drop tables if exists and create rides table attributes
    cursor.execute("CREATE DATABASE IF NOT EXISTS {};".format(database_name))
    cursor.execute("DROP TABLE IF EXISTS rides;")
    cursor.execute("DROP TABLE IF EXISTS recent_rides;")
    cursor.execute("DROP TABLE IF EXISTS stations;")
    cursor.execute("""
    CREATE TABLE rides (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, 
    start_lat DECIMAL(9,7), end_lat DECIMAL(9,7), 
    start_lng DECIMAL(9,7), end_lng DECIMAL(9,7), 
    started_at DATETIME NOT NULL, ended_at DATETIME NOT NULL, duration VARCHAR(64) NOT NULL,
    start_station_name VARCHAR(64) NOT NULL, end_station_name VARCHAR(64) NOT NULL, 
    start_station_id VARCHAR(15) NOT NULL, end_station_id VARCHAR(15) NOT NULL, 
    member_casual VARCHAR(10) NOT NULL, 
    ride_id VARCHAR(20) NOT NULL, is_equity VARCHAR(20) NOT NULL)
    ;""")
    cursor.close()

    return print('Database Created')

