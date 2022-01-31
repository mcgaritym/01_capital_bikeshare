# import libraries
import pandas as pd
from config import *
from io import BytesIO
from AWSConnect import AWSConnect

# function to transform/load station data
def transform_load_stations_rds(bucket_name, key_name):

    # get class, and create connections
    connect = AWSConnect(rds_user, rds_pwd, rds_host, rds_port, rds_database, service_name, region_name, aws_access_key_id, aws_secret_access_key)
    s3_client = connect.s3_client()
    rds_sqlalchemy = connect.rds_sqlalchemy()

    try:
        obj = s3_client.get_object(Bucket=bucket_name, Key=key_name)
        df = pd.read_csv(BytesIO(obj['Body'].read()))

        # send to RDS
        df.to_sql(name='stations', con=rds_sqlalchemy, if_exists="replace", chunksize=1000, index=False)
        rds_sqlalchemy.dispose()
        print('Stations sent to RDS successfully')

    except:
        pass
        print('Error: {} {} NOT sent to RDS'.format(bucket_name, key_name))

    # read stations table
    recent_rides = pd.read_sql_query("""SELECT COUNT(*) FROM stations;""", con=rds_sqlalchemy)
    rds_sqlalchemy.dispose()
    print('Stations Count: ', recent_rides)

    return print("Stations Transformed and Loaded to RDS")
