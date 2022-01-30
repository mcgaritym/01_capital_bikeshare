# import
import pandas as pd
import io
import os
import glob as glob
from config import *
import boto3
from sqlalchemy import create_engine
from glob import glob
from io import StringIO, BytesIO


def s3_client():
    s3_session = boto3.Session(
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key)
    s3 = s3_session.client('s3')
    return s3


def connect_RDS():

    # specify second MySQL database connection (faster read_sql query feature)
    connection = create_engine("mysql+pymysql://{user}:{password}@{host}:{port}/{db}".format(user=rds_user,
                                                                    password=rds_pwd, host=rds_host,
                                                                    port=rds_port, db=rds_database))
    return connection

def close_RDS():

    try:
        connect_RDS().dispose()

    except:
        pass


def transform_load_stations_rds(bucket_name, key_name):

    # specify s3
    s3 = s3_client()

    # connect to RDS
    connection = connect_RDS()

    try:
        obj = s3.get_object(Bucket=bucket_name, Key=key_name)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        # send to RDS
        df.to_sql(name='stations', con=connection, if_exists="replace", chunksize=1000, index=False)
        print('Stations sent to RDS successfully')

    except:
        pass
        print('Error: {} {} NOT sent to RDS'.format(bucket_name, key_name))

    # # read stations table
    recent_rides = pd.read_sql_query("""SELECT * FROM stations;""", con=connection)
    print(recent_rides)

# transform_load_stations_rds('capitalbikeshare-bucket', 'stations/capital_bikeshare_stations.csv')