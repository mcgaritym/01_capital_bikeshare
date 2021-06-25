#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 20 13:33:15 2021

@author: mcgaritym
"""

import json
import requests
import pandas as pd
from sqlalchemy import create_engine
import os
import glob
import pymysql


# =============================================================================
# Establish SQL connection and create database
# =============================================================================

# credentials
user = 'root'
pwd = ""
host = 'localhost'

# establish connection, create database
engine = create_engine(
    f"mysql+mysqlconnector://{user}:{pwd}@{host}/",
    echo=False)
conn = engine.connect()
conn.execute("CREATE DATABASE IF NOT EXISTS test_db_2;")
conn.close()

# add database credential and connect
db = 'test_db_2' 

# establish connection to new database
engine = create_engine(
    f"mysql+mysqlconnector://{user}:{pwd}@{host}/{db}",
    echo=False)
conn = engine.connect()

# =============================================================================
# create empty ride table 
# =============================================================================

# create ride table
conn.execute("""CREATE TABLE IF NOT EXISTS ride_table_test (
                `Start date` TEXT, 
                `End date` TEXT, 
                `Start station number` INT, 
                `Start station` TEXT,
                `End station number` INT,
                `End station` TEXT,
                `Bike number` varchar(20),
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

# =============================================================================
# load ride data
# =============================================================================

# def get_files(wildcard_name):

#     # get current parent directory and data folder path
#     par_directory = os.path.dirname(os.getcwd())
#     data_directory = os.path.join(par_directory, 'data/raw')

#     # retrieve tripdata files
#     files = glob.glob(os.path.join(data_directory, wildcard_name))
#     print(files)

#     # create empty dataframe, loop over files and concatenate data to dataframe
#     # df = pd.DataFrame()
#     for f in files[:2]:
#         data = pd.read_csv(f)
#         print(len(data))
        
#         # convert to table
#         data.to_sql(name='ride_table_test', con=engine, if_exists = 'append')


# df = get_files('*tripdata*')

# =============================================================================
# load bike station data into table (via JSON request and pandas)
# =============================================================================

# request json data
response = requests.get("https://gbfs.capitalbikeshare.com/gbfs/en/station_information.json")
todos = json.loads(response.text)['data']
    
# flatten nested json
df_nested_list = pd.json_normalize(todos, record_path = ['stations'])
df_nested_list = df_nested_list.drop(columns = ['rental_methods', 'eightd_station_services'])

# convert to table
df_nested_list.to_sql(name='capital_bs_stations', con=engine, if_exists = 'replace')


# # =============================================================================
# # update table columns, keys
# # =============================================================================

# # rename column 
# conn.execute("""ALTER TABLE ride_table_test
#                   RENAME COLUMN `Start station number` TO start_station_num; """)

# # update column 
# conn.execute("""ALTER TABLE ride_table_test
#                    ADD ride_id_key INT NOT NULL AUTO_INCREMENT,
#                    ADD PRIMARY KEY(ride_id_key); """)
                                   
# # update table keys
# conn.execute("""ALTER TABLE capital_bs_stations
#                   MODIFY COLUMN short_name INT,
#                   ADD PRIMARY KEY(short_name); """)

# # =============================================================================
# # analyze SQL table
# # =============================================================================

# # analyze capacity 
# conn.execute("""SELECT ride_table_test.start_station_num, capital_bs_stations.capacity
# FROM ride_table_test
# INNER JOIN capital_bs_stations
# ON ride_table_test.start_station_num=capital_bs_stations.short_name; """)


# =============================================================================
#  close SQL connection
# =============================================================================

conn.close()

# =============================================================================
#  create AWS RDS connection to backup
# =============================================================================

# host="bikeshare-database-1.cj4j9csypvlc.us-east-2.rds.amazonaws.com"
# port="3306"
# user="admin"
# pwd="Nalgene09!"
# database="bikeshare-database-1"

# engine = create_engine(
#     f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{database}",
#     echo=False)

# # mysqlconnector

# conn = engine.connect()

# # convert to table
# df_nested_list.to_sql(name='capital_bs_stations', con=engine, if_exists = 'replace')

# conn.close()


# import sys
# import boto3
# import os

# ENDPOINT="bikeshare-database-1.cj4j9csypvlc.us-east-2.rds.amazonaws.com"
# PORT="3306"
# USR="admin"
# REGION="us-east-2a"
# os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'

# #gets the credentials from .aws/credentials
# session = boto3.Session(profile_name='RDSCreds')
# client = session.client('rds')

# token = client.generate_db_auth_token(DBHostname=ENDPOINT, Port=PORT, DBUsername=USR, Region=REGION)                    
    
