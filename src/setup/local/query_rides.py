# import config settings and Setup class
import pandas as pd
import config_local
from setup_local import Setup

# call Setup class from setup_cloud.py file
connection = Setup(config_local.user, config_local.pwd, config_local.host, config_local.port, config_local.db)

# create database and connection
connection.create_database()
conn = connection.create_connection()


df = pd.read_sql(""" COUNT * FROM rides; """, con=conn)

connection.close_connection()