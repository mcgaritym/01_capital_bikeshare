# import libraries
import os
from glob import glob
from config import *
import boto3
import json
import requests
import pandas as pd

# connect to s3
def s3_resource():
    s3 = boto3.resource(
        service_name=service_name,
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key)
    return s3


def stations_csv_s3():

    # request json data
    response = requests.get("https://gbfs.capitalbikeshare.com/gbfs/en/station_information.json")
    todos = json.loads(response.text)['data']

    # flatten nested json
    df_stations = pd.json_normalize(todos, record_path=['stations'])
    par_directory = os.path.dirname(os.getcwd())
    data_directory = os.path.join(par_directory, 'data')

    # convert to csv and save
    df_stations.to_csv(data_directory + '/capital_bikeshare_stations.csv')

    print('sent to s3')

    return df_stations

# upload to s3
def upload_stations_s3(local_file_search, bucket_name, key_name):

    par_directory = os.path.dirname(os.getcwd())
    data_directory = os.path.join(par_directory, 'data')
    files = glob(os.path.join(data_directory, local_file_search))
    s3 = s3_resource()

    # for each file:
    for f in files:

        # try to upload files to  buckets
        try:
            # upload file to s3
            f_name = f.split("/")[-1]
            # s3.Object(bucket_name, object_name).upload_file(Filename=f)
            print(f)
            print(f_name)
            s3.meta.client.upload_file(Filename=f, Bucket=bucket_name, Key='{}/{}'.format(key_name, f_name))

            print('{} uploaded to s3 successfully'.format(f))

        except:
            # error
            print('Error: Did not upload {} to s3'.format(f))

    # Print out bucket names and objects within buckets
    for bucket in s3.buckets.all():

        print('Current Bucket: ', bucket.name)

        for object in bucket.objects.all():

            print('Current Object: ', object.key)

    return print("Stations Uploaded to s3")


