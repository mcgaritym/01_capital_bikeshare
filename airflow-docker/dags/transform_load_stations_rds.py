# import
import pandas as pd
import io
import os
import glob as glob
from config import *
import boto3

# connect to s3
def s3_resource(service_name, region_name, aws_access_key_id, aws_secret_access_key):
    s3 = boto3.resource(
        service_name=service_name,
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key)
    return s3


# def s3_client(region_name, aws_access_key_id, aws_secret_access_key):
#     s3_session = boto3.Session(
#         region_name=region_name,
#         aws_access_key_id=aws_access_key_id,
#         aws_secret_access_key=aws_secret_access_key)
#     s3 = s3_session.client('s3')
#     return s3

def connect_RDS():
    pass

def close_RDS():
    pass

def clean_rides(df):
    pass


def transform_load_stations_rds(s3, bucket_name):

    # specify bucket
    bucket_name = s3.Bucket(bucket_name)

    # for each object in bucket
    for object in bucket_name.objects.all():

        try:

            # get file
            obj = s3.get_object(Bucket=bucket_name, Key=object)

            # get df
            df = pd.read_csv(io.BytesIO(obj['Body'].read()))

            # clean df
            df = clean_rides(df)

            # connect to RDS
            connection = connect_RDS()

            # send to RDS
            df.to_sql(name='stations', con=connection, if_exists="append", chunksize=1000)

            # load to RDS
            close_connection = close_RDS()
            close_connection
            print('{} sent to RDS successfully'.format(object))

        except:
            print('Error: {} NOT sent to RDS'.format(object))

