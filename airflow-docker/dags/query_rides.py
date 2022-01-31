# import libraries
import pandas as pd
import mysql.connector as msql
import mysql.connector
from sqlalchemy import create_engine
from config import *

def connect_RDS():

    # specify second MySQL database connection (faster read_sql query feature)
    connection = create_engine("mysql+pymysql://{user}:{password}@{host}:{port}/{db}".format(user=rds_user,
                                                                    password=rds_pwd, host=rds_host,
                                                                    port=rds_port, db=rds_database))
    return connection

def alter_RDS():

    try:

        connection = mysql.connector.connect(host=rds_host, user=rds_user, password=rds_pwd, port=rds_port, database=rds_database)
        cursor = connection.cursor()
        cursor.execute("ALTER TABLE rides ORDER BY `started_at` ASC;")
        cursor.execute("ALTER TABLE rides DROP id, ADD new_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY;")
        cursor.close()

    except:
        pass

def close_RDS():

    return connect_RDS().dispose()

def query_rides():

    # update table to sort by date and update keys, if applicable
    alter_RDS()

    # connect to RDS
    connection = connect_RDS()

    # read query
    recent_rides = pd.read_sql_query("""
    SELECT rides.ride_id, rides.started_at, rides.duration, 
    stations.name, stations.lat, stations.lon, stations.capacity, stations.short_name,
    DateDiff(second, started_at, ended_at) as duration_seconds
    FROM rides
    JOIN stations
    ON rides.start_station_id = stations.short_name
    ORDER BY `started_at` ASC
    LIMIT 10;""", con=connection)

    print(recent_rides)

    # send results to sql, and save to csv
    recent_rides.to_sql(name='recent_rides', con=connection, if_exists="replace", chunksize=1000, index=False)

    return print("Recent Rides Query Successful")
