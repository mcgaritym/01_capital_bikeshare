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

# # connect to s3
# def s3_resource():
#     s3 = boto3.resource(
#         service_name=service_name,
#         region_name=region_name,
#         aws_access_key_id=aws_access_key_id,
#         aws_secret_access_key=aws_secret_access_key)
#     return s3


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

    # # specify bucket
    # bucket_name = s3.Bucket(bucket_name)
    #
    # # for each object in bucket
    # for object in bucket_name.objects.all():
    #
    #     if key_name in object.key:
    #
    #         try:
    #
    #             # get file
    #             print('Before:', object.key)
    #             obj = s3.Object(bucket_name, object.key)
    #             print(obj)
    #
    #             # get df
    #             df = pd.read_csv(obj['Body'])
    #             print(df)
    #
    #             # send to RDS
    #             df.to_sql(name='stations', con=connection, if_exists="replace", chunksize=1000)
    #
    #             # load to RDS
    #             close_connection = close_RDS()
    #             close_connection
    #             print('{} sent to RDS successfully'.format(object))
    #
    #         except:
    #             print('Error: {} NOT sent to RDS'.format(object))
    #
    #     else:
    #         pass

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

transform_load_stations_rds('capitalbikeshare-bucket', 'stations/capital_bikeshare_stations.csv')