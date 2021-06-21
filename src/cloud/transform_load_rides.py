# import libraries
import json
import requests
import pandas as pd
import mysql.connector
from mysql.connector.cursor import MySQLCursor
import pymysql
import config_cloud
from setup_cloud import Setup

# call Setup class from setup_cloud.py file
connection = Setup(config_cloud.user, config_cloud.pwd, config_cloud.host, config_cloud.port, config_cloud.db)

# create database and connection
connection.create_database()
conn = connection.create_connection()


# update table keys
conn.execute("""ALTER TABLE rides
                  ADD ride_id_key INT NOT NULL AUTO_INCREMENT,
                  ADD PRIMARY KEY(ride_id_key); """)


conn.execute("""ALTER TABLE rides
                  RENAME COLUMN `Start station number` TO start_station_num ; """)


conn.execute("""ALTER TABLE rides
                  ADD FOREIGN KEY (start_station_num) REFERENCES capital_bs_stations(short_name); """)


connection.close_connection()
