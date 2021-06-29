# import config settings and Setup class
import pandas as pd
import config_cloud
from setup_cloud import Setup

# call Setup class from setup_cloud.py file
connection = Setup(config_cloud.user, config_cloud.pwd, config_cloud.host, config_cloud.port, config_cloud.db)

# create database and connection
connection.create_database()
conn = connection.create_connection()


df = pd.read_sql(""" COUNT * FROM rides; """, con=conn)

connection.close_connection()