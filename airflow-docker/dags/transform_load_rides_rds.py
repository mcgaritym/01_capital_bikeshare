# import libraries
import pandas as pd
import io
from config import *
from AWSConnect import AWSConnect

# function to clean ride date
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

    # convert to datetime and create duration column
    df['started_at'] = pd.to_datetime(df['started_at'])
    df['ended_at'] = pd.to_datetime(df['ended_at'])
    df['duration'] = df['ended_at'] - df['started_at']

    # convert datetime columns back to string and drop index for MySQL processing
    df["started_at"] = df['started_at'].astype(str)
    df["ended_at"] = df['ended_at'].astype(str)
    df["duration"] = df['duration'].astype(str)

    df.reset_index(drop=True, inplace=True)

    return df


def transform_load_rides_rds(bucket_name, key_name):

    # get class, and create connections
    connect = AWSConnect(rds_user, rds_pwd, rds_host, rds_port, rds_database, service_name, region_name, aws_access_key_id, aws_secret_access_key)
    s3_resource = connect.s3_resource()
    s3_client = connect.s3_client()
    rds_sqlalchemy = connect.rds_sqlalchemy()

    # for each object in bucket
    object_list = list(s3_resource.Bucket(bucket_name).objects.all())
    print(object_list)
    object_list = object_list[-5:]
    for obj in object_list:

    # for obj in s3_resource.Bucket(bucket_name).objects.all():

        if key_name in obj.key:

            try:
                # specify s3
                print(obj)

                # get object
                obj_body = s3_client.get_object(Bucket='capitalbikeshare-bucket', Key=obj.key)
                df = pd.read_csv(io.BytesIO(obj_body['Body'].read()))

                # clean file
                df = clean_rides(df)

                # send to RDS
                try:
                    df.to_sql(name='rides', con=rds_sqlalchemy, if_exists="append", chunksize=1000, index=False)
                    rds_sqlalchemy.dispose()
                    print('{} rides sent to RDS successfully'.format(obj.key))

                except Exception as e:
                    print(e)

            except :
                print('Error: {} rides NOT sent to RDS'.format(obj.key))

        else:
            pass

    # read rides table
    recent_rides = pd.read_sql_query("""SELECT COUNT(*) FROM rides;""", con=rds_sqlalchemy)
    print('Rides Count: ', recent_rides)

    return print("Rides Transformed and Loaded to RDS")
