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
conn.execute("""ALTER TABLE stations
                  ADD PRIMARY KEY(`short_name`),
                  MODIFY COLUMN short_name INT; """)

connection.close_connection()


