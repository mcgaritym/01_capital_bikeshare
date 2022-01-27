# import
import os
from glob import glob
from config import *
import boto3
import json
import requests
import pandas as pd
from datetime import datetime, date
import time

def connect_RDS():
    pass

def close_RDS():
    pass

def clean_RDS():

    cursor.execute("ALTER TABLE rides ORDER BY `started_at` ASC;")
    cursor.execute("ALTER TABLE rides DROP id, ADD new_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY;")


def query_rides():

    # connect to RDS
    connection = connect_RDS()

    # read query
    recent_rides = pd.read_sql_query("""
    SELECT * 
    FROM rides 
    JOIN stations 
    ON rides.short_name = stations.`Start station number`
    ORDER BY `started_at` ASC 
    LIMIT 10;""", con=connection)

    # drop duplicates, send to csv file, and print results
    recent_rides.to_sql(name='recent_rides', con=connection, if_exists="replace", chunksize=1000)
    recent_rides.to_csv('recent_rides_' + str(datetime.now().strftime("%Y-%m-%d__%H-%M-%S")) + '.csv',
                              index=False)
    print(recent_rides)

    return "Recent Rides Query Successful"
