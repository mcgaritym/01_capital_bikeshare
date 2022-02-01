# import libraries
import os
from glob import glob
from config import *
from AWSConnect import AWSConnect

# extract rides to s3
def extract_rides_s3(local_file_search, bucket_name, key_name):

    # get data files
    data_directory = os.path.join(os.getcwd(), 'data')
    files = glob(os.path.join(data_directory, local_file_search))
    print('Files: ', files)

    # get class, and create connections
    s3_connect = AWSConnect(rds_user, rds_pwd, rds_host, rds_port, rds_database, service_name, region_name, aws_access_key_id, aws_secret_access_key)
    s3 = s3_connect.s3_resource()

    # for each file:
    for f in files:

        # try to upload files to  buckets
        try:
            # upload file to s3
            f_name = f.split("/")[-1]
            # s3.Object(bucket_name, object_name).upload_file(Filename=f)
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

    return print("Rides Uploaded to s3")
