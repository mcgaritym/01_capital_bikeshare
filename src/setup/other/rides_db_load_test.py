#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 17 21:55:38 2021

@author: mcgaritym
"""

import json
import requests
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
import os
import glob
import mysql.connector
from mysql.connector.cursor import MySQLCursor

# =============================================================================
# Establish SQL connection and create connection
# =============================================================================

# # # create SQL connectionand database
# user = 'root'
# pwd = "Nalgene09!"
# host = 'localhost'
# port = int(3306)

# # establish connection, create database
# engine = create_engine(
#     f"mysql+mysqlconnector://{user}:{pwd}@{host}:{port}/",
#     echo=False, poolclass = NullPool)
# conn = engine.connect()
# conn.execute("CREATE DATABASE IF NOT EXISTS rides_db;")
# conn.close()

# # add database credential and connect
# db = 'rides_db' 

# # establish connection to new database
# engine = create_engine(
#     f"mysql+mysqlconnector://{user}:{pwd}@{host}:{port}/{db}",
#     echo=False, poolclass = NullPool)

# conn = engine.connect()

# =============================================================================
# Create Station table
# =============================================================================

# # request json data
# response = requests.get("https://gbfs.capitalbikeshare.com/gbfs/en/station_information.json")
# todos = json.loads(response.text)['data']
    
# # flatten nested json
# df_nested_list = pd.json_normalize(todos, record_path = ['stations'])
# df_nested_list = df_nested_list.drop(columns = ['rental_methods', 'eightd_station_services'])

# # convert to table
# df_nested_list.to_sql(name='ride_stations', con=engine, if_exists = 'replace')

# print data
# engine.execute("SELECT * FROM capital_bs_stations").fetchall()

# # =============================================================================
# # Create Rides table
# # =============================================================================

# set up connection
db = mysql.connector.connect(
  host="localhost",
  user="root",
  password="Nalgene09!",
  database="rides_db"
)
cursor = db.cursor()

def get_files(wildcard_name):
    
    # # set up connection
    # cnx = mysql.connector.connect(
    #   host="localhost",
    #   user="root",
    #   password="Nalgene09!",
    #   database="rides_db"
    # )
    
    # create a table
    cursor.execute("DROP TABLE IF EXISTS ride_history;")
    
    # create ride table
    cursor.execute("""CREATE TABLE IF NOT EXISTS ride_history (
                    `Start date` TEXT, 
                    `End date` TEXT, 
                    `Start station number` INT, 
                    `Start station` TEXT,
                    `End station number` INT,
                    `End station` TEXT,
                    `Bike number` TEXT,
                    `Member type` TEXT,
                    ride_id varchar(20),
                    rideable_type varchar(20),
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
                    member_casual varchar(20),
                    is_equity TEXT,
                    `index` INT)
                    """)  
                    
    cursor.close()
    # cnx.close()

    # get current parent directory and data folder path
    par_directory = os.path.dirname(os.getcwd())
    data_directory = os.path.join(par_directory, 'data/raw')

    # retrieve tripdata files
    files = glob.glob(os.path.join(data_directory, wildcard_name))
    files.sort()
    
    # print number of ride files
    print('No. of files: {}'.format(len(files)))

    # # create empty dataframe, loop over files and concatenate data to dataframe
    # df = pd.DataFrame()
    
    for f in files[:4]:
        
        # read into pandas        
        data = pd.read_csv(f)
        print(f)
        print(len(data))
        
        # convert to table
        data.to_sql(name='ride_history', con=db, if_exists = 'append')
        db.commit()
        cursor.close()
        
    db.close()

df = get_files('*tripdata*')

db
cursor

# update table keys
cursor.execute("""ALTER TABLE ride_stations
                  ADD PRIMARY KEY(`short_name`),
                  MODIFY COLUMN short_name INT; """)

# update table keys
cursor.execute("""ALTER TABLE ride_history
                  ADD ride_id_key INT NOT NULL AUTO_INCREMENT,
                  ADD PRIMARY KEY(ride_id_key); """)
                 
cursor.execute("""ALTER TABLE ride_history
                  RENAME COLUMN `Start station number` TO start_station_num ; """)

cursor.execute("""ALTER TABLE ride_history
                  ADD FOREIGN KEY (start_station_num) REFERENCES capital_bs_stations(short_name); """)

db.commit()
cursor.close()
db.close()





