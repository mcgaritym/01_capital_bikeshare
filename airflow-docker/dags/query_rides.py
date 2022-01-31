# import libraries
import pandas as pd
from config import *
from AWSConnect import AWSConnect

def query_rides():

    # get class, and create connections
    connect = AWSConnect(rds_user, rds_pwd, rds_host, rds_port, rds_database, service_name, region_name, aws_access_key_id, aws_secret_access_key)
    rds_mysql_connection, rds_mysql_cursor = connect.rds_mysql()
    rds_sqlalchemy = connect.rds_sqlalchemy()

    try:
        rds_mysql_cursor.execute("ALTER TABLE rides ORDER BY `started_at` DESC;")
        rds_mysql_cursor.execute("ALTER TABLE rides DROP id, ADD new_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY;")
        rds_mysql_cursor.close()

    except:
        pass

    # read query
    recent_rides = pd.read_sql_query("""
    SELECT rides.ride_id, rides.started_at, rides.ended_at, rides.duration, 
    stations.name, stations.lat, stations.lon, stations.capacity, stations.short_name
    FROM rides
    JOIN stations
    ON rides.start_station_id = stations.short_name
    ORDER BY `started_at` DESC
    LIMIT 10;""", con=rds_sqlalchemy)

    print(recent_rides)

    # send results to sql, and save to csv
    recent_rides.to_sql(name='recent_rides', con=rds_sqlalchemy, if_exists="replace", chunksize=1000, index=False)

    return print("Recent Rides Query Successful")
