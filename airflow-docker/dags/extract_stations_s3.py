# import
import os
import glob as glob
from config import *
import boto3
import json
import requests
import pandas as pd

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


def stations_csv_s3():

    # request json data
    response = requests.get("https://gbfs.capitalbikeshare.com/gbfs/en/station_information.json")
    todos = json.loads(response.text)['data']

    # flatten nested json
    df_stations = pd.json_normalize(todos, record_path=['stations'])
    # df_stations = df_stations.drop(columns=['rental_methods', 'eightd_station_services'])

    # convert to csv and save
    df_stations.to_csv('capital_bikeshare_stations.csv')

def stations_s3(s3, bucket_name):

    # collect local ride files
    cwd = os.getcwd()
    # par_directory = os.path.dirname(os.getcwd())
    # data_directory = os.path.join(par_directory, 'data')
    files = glob(os.path.join(cwd, '*capital_bikeshare_stations*'))

    # for each file:
    for f in files:

        # try to upload files to  buckets
        try:
            # upload file to s3
            object_name = f.split("/")[-1]
            s3.Object(bucket_name, object_name).upload_file(Filename=f)
            print('{} uploaded to s3 successfully'.format(object_name))

        except:
            # error
            print('Error: Did not upload {} to s3'.format(object_name))

    # print objects within bucket
    bucket_name = s3.Bucket(bucket_name)
    for my_bucket_object in bucket_name.objects.all():
        print('Object in {}: {}'.format(bucket_name, my_bucket_object))


