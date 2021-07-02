import config_cloud
import boto3

# Retrieve the list of existing buckets
s3 = boto3.resource(
    service_name=config_cloud.service_name,
    region_name=config_cloud.region_name,
    aws_access_key_id=config_cloud.aws_access_key_id,
    aws_secret_access_key=config_cloud.aws_secret_access_key
)

# Print out bucket names
for bucket in s3.buckets.all():
    print(bucket.name)

# upload file
s3.Object('stocks.bucket', 'query_rides.py').upload_file(
    Filename='query_rides.py')