# import
import os
from glob import glob
from config import *
import boto3

# def s3_client(region_name, aws_access_key_id, aws_secret_access_key):
#     s3_session = boto3.Session(
#         region_name=region_name,
#         aws_access_key_id=aws_access_key_id,
#         aws_secret_access_key=aws_secret_access_key)
#     s3 = s3_session.client('s3')
#     return s3

# def empty_bucket(s3):
#
#     # print bucket names
#     for bucket in s3.buckets.all():
#         print('Bucket (old): ', bucket.name)
#
#         try:
#             bucket = s3.Bucket(bucket.name)
#             bucket.objects.all().delete()
#             print('Bucket {} deleted'.format(bucket.name))
#
#         except:
#             print('Error: Bucket {} NOT deleted'.format(bucket.name))
#
# def create_bucket_s3(s3, bucket_name):
#
#     try:
#         s3.create_bucket(Bucket=bucket_name)
#         print('Bucket {} created'.format(bucket_name))
#
#     except:
#         s3.create_bucket(Bucket=bucket_name)
#         print('Error: Bucket {} NOT created'.format(bucket_name))

# connect to s3
def s3_resource():
    s3 = boto3.resource(
        service_name=service_name,
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key)
    return s3

# upload to s3
def upload_s3(local_file_search, bucket_name, key_name):

    # collect local ride files
    cwd = os.getcwd()
    par_directory = os.path.dirname(os.getcwd())
    data_directory = os.path.join(par_directory, 'data')
    # files = glob(os.path.join(data_directory, '*capitalbikeshare*'))
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

upload_s3('*capitalbikeshare*', 'capitalbikeshare-bucket', 'rides')