# import libraries
import pandas as pd
from config import *
import io
from AWSConnect import AWSConnect

# function to transform/load station data
def transform_load_stations_rds(bucket_name, key_name):

    # get class, and create connections
    connect = AWSConnect(rds_user, rds_pwd, rds_host, rds_port, rds_database, service_name, region_name, aws_access_key_id, aws_secret_access_key)
    s3_client = connect.s3_client()
    s3_resource = connect.s3_resource()
    rds_sqlalchemy = connect.rds_sqlalchemy()

    # for each object in bucket
    object_list = list(s3_resource.Bucket(bucket_name).objects.all())
    for obj in object_list:

        if key_name in obj.key:

                # get object
                obj_body = s3_client.get_object(Bucket='capitalbikeshare-bucket', Key=obj.key)
                df = pd.read_csv(io.BytesIO(obj_body['Body'].read()))

                # send to RDS
                try:
                    print('ok4')
                    df.to_sql(name='stations', con=rds_sqlalchemy, if_exists="replace", chunksize=1000, index=False)
                    rds_sqlalchemy.dispose()
                    print('{} stations sent to RDS successfully'.format(obj.key))

                except Exception as e:
                    print('{} stations NOT SENT to RDS'.format(obj.key))
                    print(e)

        else:
            pass

    # read stations table
    recent_rides = pd.read_sql_query("""SELECT COUNT(*) FROM stations;""", con=rds_sqlalchemy)
    rds_sqlalchemy.dispose()
    print('Stations Count: ', recent_rides)

    return print("Stations Transformed and Loaded to RDS")
