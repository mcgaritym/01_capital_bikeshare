# import libraries
from sqlalchemy import create_engine
import mysql.connector as msql
import mysql.connector
import boto3

# class for connecting to SQL
class AWSConnect:

    # instantiate
    def __init__(self, rds_user, rds_pwd, rds_host, rds_port, rds_database, service_name, region_name, aws_access_key_id, aws_secret_access_key):
        self.rds_user = rds_user
        self.rds_pwd = rds_pwd
        self.rds_host = rds_host
        self.rds_port = rds_port
        self.rds_database = rds_database
        self.service_name = service_name
        self.region_name = region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

    # connection for mysql connector
    def rds_mysql(self):

        connection = mysql.connector.connect(host=self.rds_host,
                                             user=self.rds_user,
                                             password=self.rds_pwd,
                                             port=self.rds_port,
                                             database=self.rds_database)
        cursor = connection.cursor()
        return connection, cursor

    # connection for sqlalchemy
    def rds_sqlalchemy(self):

        connection = create_engine("mysql+pymysql://{user}:{password}@{host}:{port}/{db}".format(user=self.rds_user,
                                                                                                password=self.rds_pwd,
                                                                                                host=self.rds_host,
                                                                                                port=self.rds_port,
                                                                                                db=self.rds_database))
        return connection

    # s3 client connection
    def s3_client(self):

        s3_session = boto3.Session(
            region_name=self.region_name,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key)
        s3 = s3_session.client('s3')
        return s3

    # s3 resource connection
    def s3_resource(self):

        s3 = boto3.resource(
            service_name=self.service_name,
            region_name=self.region_name,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key)
        return s3
