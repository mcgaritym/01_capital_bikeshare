# import
import pandas as pd
import io
import os
from glob import glob
from config import *
import boto3
from sqlalchemy import create_engine


# def s3_client(region_name, aws_access_key_id, aws_secret_access_key):
#     s3_session = boto3.Session(
#         region_name=region_name,
#         aws_access_key_id=aws_access_key_id,
#         aws_secret_access_key=aws_secret_access_key)
#     s3 = s3_session.client('s3')
#     return s3


def connect_RDS():

    # specify second MySQL database connection (faster read_sql query feature)
    connection = create_engine("mysql+pymysql://{user}:{password}@{host}:{port}/{db}".format(user=rds_user,
                                                                    password=rds_pwd, host=rds_host,
                                                                    port=rds_port, db=rds_database))
    return connection

def close_RDS():

    return connect_RDS().dispose()


# connect to s3
def s3_resource():
    s3 = boto3.resource(
        service_name=service_name,
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key)
    return s3

# connect to s3
def s3_client():
    s3_session = boto3.Session(
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key)
    s3 = s3_session.client('s3')
    return s3


def clean_rides(df):

    # rename, convert columns depending on file structure.
    try:
        df = df.rename(columns={'Start station': 'start_station_name',
                                'End station': 'end_station_name',
                                'Start date': 'started_at',
                                'End date': 'ended_at',
                                'Member type': 'member_casual',
                                'Start station number': 'start_station_id',
                                'End station number': 'end_station_id'})
        # convert to numeric data type
        df[['start_lat', 'end_lat', 'start_lng', 'end_lng']] = df[
            ['start_lat', 'end_lat', 'start_lng', 'end_lng']].apply(pd.to_numeric)

    except:
        pass

    try:
        df.drop(columns=['Duration', 'Bike number'], inplace=True)

    except:
        df.drop(columns=['rideable_type'], inplace=True)

    # create new duration and number of rides column
    df['started_at'] = pd.to_datetime(df['started_at'])
    df['ended_at'] = pd.to_datetime(df['ended_at'])

    # convert datetime columns back to string and drop index for MySQL processing
    df["started_at"] = df['started_at'].astype(str)
    df["ended_at"] = df['ended_at'].astype(str)
    df.reset_index(drop=True, inplace=True)

    return df


def transform_load_rides_rds(bucket_name, key_name):

    # specify s3
    s3 = s3_resource()

    # specify bucket
    # bucket_name = s3.Bucket(bucket_name)

    # for each object in bucket
    for obj in s3.Bucket(bucket_name).objects.all():

        if key_name in obj.key:

            try:
                # specify s3
                s3 = s3_client()
                print(obj)

                # get object
                obj_body = s3.get_object(Bucket='capitalbikeshare-bucket', Key=obj.key)
                df = pd.read_csv(io.BytesIO(obj_body['Body'].read()))

                # clean file
                df = clean_rides(df)

                # send to RDS
                try:
                    connection = connect_RDS()
                    df.to_sql(name='rides', con=connection, if_exists="append", chunksize=1000, index=False)
                    close_RDS()
                    print('{} rides sent to RDS successfully'.format(obj.key))

                except Exception as e:
                    print(e)

            except :
                print('Error: {} rides NOT sent to RDS'.format(obj.key))

        else:
            pass

    # read stations table
    connection = connect_RDS()
    recent_rides = pd.read_sql_query("""SELECT * FROM rides;""", con=connection)
    print(recent_rides)

transform_load_rides_rds('capitalbikeshare-bucket', 'tripdata.csv')