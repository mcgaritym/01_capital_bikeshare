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
import os
import glob

# =============================================================================
# Establish SQL connection and create connection
# =============================================================================


# # create SQL connectionand database
# user = 'root'
# pwd = ""
# host = 'localhost'
# schema = 'test_db_4' 

# engine = create_engine(
#     f"mysql+mysqlconnector://{user}:{pwd}@{host}/{schema}",
#     echo=False)

# conn = engine.connect()

# conn.execute("CREATE DATABASE IF NOT EXISTS test_db_4;")
# conn.close()

# =============================================================================
# Create Station table
# =============================================================================

# # request json data
# response = requests.get("https://gbfs.capitalbikeshare.com/gbfs/en/station_information.json")
# todos = json.loads(response.text)['data']
    
# # flatten nested json
# df_nested_list = pd.json_normalize(todos, record_path = ['stations'])
# df_nested_list = df_nested_list.drop(columns = ['rental_methods', 'eightd_station_services'])
# df_nested_list.info()

# # convert to table
# df_nested_list.to_sql(name='capital_bs_stations', con=engine, if_exists = 'replace')

# # print data
# engine.execute("SELECT * FROM capital_bs_stations").fetchall()

# # =============================================================================
# # Create Rides table
# # =============================================================================

# # create Rides table

# import mysql.connector

# mydb = mysql.connector.connect(
#   host="localhost",
#   user="root",
#   password="",
#   database="test_db3"
# )

# mycursor = mydb.cursor(buffered=True)

# # create a table
# mycursor.execute("DROP TABLE IF EXISTS ride_table_test;")

# mydb.commit()

# # create ride table
# mycursor.execute("""CREATE TABLE IF NOT EXISTS ride_table_test (
#                 `Start date` TEXT, 
#                 `End date` TEXT, 
#                 `Start station number` INT, 
#                 `Start station` TEXT,
#                 `End station number` INT,
#                 `End station` TEXT,
#                 `Bike number` varchar(20),
#                 `Member type` TEXT,
#                 ride_id varchar(20),
#                 rideable_type varchar(20),
#                 started_at TEXT,
#                 ended_at TEXT,
#                 Duration DECIMAL(7,2),
#                 start_station_name TEXT,
#                 start_station_id INT,
#                 end_station_name TEXT,
#                 end_station_id INT,
#                 start_lat DECIMAL(11,8),
#                 start_lng DECIMAL(11,8),
#                 end_lat DECIMAL(11,8),
#                 end_lng DECIMAL(11,8),
#                 member_casual varchar(20),
#                 is_equity TEXT,
#                 `index` INT)
#                 """) 


# mydb.commit()

# def get_files(wildcard_name):

#     # get current parent directory and data folder path
#     par_directory = os.path.dirname(os.getcwd())
#     data_directory = os.path.join(par_directory, 'data/raw')

#     # retrieve tripdata files
#     files = glob.glob(os.path.join(data_directory, wildcard_name))
#     print(files)

#     # create empty dataframe, loop over files and concatenate data to dataframe
#     # df = pd.DataFrame()
#     for f in files[:3]:
#         data = pd.read_csv(f)
#         print(len(data))
        
#         # convert to table
#         data.to_sql(name='ride_table_test', con=engine, if_exists = 'append')


# df = get_files('*tripdata*')

# # update table keys
# mycursor.execute("""ALTER TABLE capital_bs_stations
#                   ADD PRIMARY KEY(`short_name`),
#                   MODIFY COLUMN short_name INT; """)

# mydb.commit()

# # update table keys
# mycursor.execute("""ALTER TABLE ride_table_test
#                   ADD ride_id_key INT NOT NULL AUTO_INCREMENT,
#                   ADD PRIMARY KEY(ride_id_key); """)
                 
# mydb.commit()

# mycursor.execute("""ALTER TABLE ride_table_test
#                   RENAME COLUMN `Start station number` TO start_station_num ; """)

# mydb.commit()

# mycursor.execute("""ALTER TABLE ride_table_test
#                   ADD FOREIGN KEY (start_station_num) REFERENCES capital_bs_stations(short_name); """)

# mydb.commit()






