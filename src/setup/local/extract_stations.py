# import libraries
import json
import requests
import pandas as pd
import config_local
from setup_local import Setup

# call Setup class from setup_cloud.py file
connection = Setup(config_local.user, config_local.pwd, config_local.host, config_local.port, config_local.db)

# create database and connection
connection.create_database()
conn = connection.create_connection()

# request json data
response = requests.get("https://gbfs.capitalbikeshare.com/gbfs/en/station_information.json")
todos = json.loads(response.text)['data']

# flatten nested json
df_stations = pd.json_normalize(todos, record_path = ['stations'])
df_stations = df_stations.drop(columns = ['rental_methods', 'eightd_station_services'])

# convert to table
df_stations.to_sql(name='stations', con=conn, if_exists='replace')

# close connection
connection.close_connection()