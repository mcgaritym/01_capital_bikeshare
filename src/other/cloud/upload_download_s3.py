# import libraries
import pandas as pd
import config_cloud
from setup_cloud import Setup

# call Setup class from setup_cloud.py file
connection = Setup(config_cloud.user, config_cloud.pwd, config_cloud.host, config_cloud.port, config_cloud.db,
                   config_cloud.service_name, config_cloud.region_name, config_cloud.aws_access_key_id, config_cloud.aws_secret_access_key)

# create connection
s3 = connection.s3()

# print current list of S3 buckets
df_s3 = pd.DataFrame(columns = ['bucket', 'object'])

for bucket in s3.buckets.all():

    for object in bucket.objects.all():

        print(object)




# def get_files(file_name):
#
#     # get current parent directory and data folder path
#     par_directory = os.path.dirname(os.path.dirname(os.getcwd()))
#     data_directory = os.path.join(par_directory, 'data/raw')
#
#     # retrieve tripdata files
#     files = glob.glob(os.path.join(data_directory, file_name))
#
#     return files

# def upload_s3(files):
#
#     # send files to s3
#     for f in files:
#
#         try:
#             # upload file to s3
#             object_name = f.split("/")[-1]
#             s3.Object('stocks.bucket', object_name).upload_file(Filename=f)
#             print('{} uploaded to s3 successfully'.format(object_name))
#
#         except:
#             # error
#             print('Error: Did not upload {} to s3'.format(object_name))
#
# # download files from s3
# def download_s3(files):
#
#
#     for f in files:
#
#         try:
#             # download file from s3
#             s3.Object('stocks.bucket', 'query_rides.py').download_file(Filename='201802-capitalbikeshare-tripdata.csv')
#
#         except:
#             # error
#             print('Error: Did not download from s3')
