
# import libraries
from sqlalchemy import create_engine
import config_cloud
from mysql import connector
import boto3
import logging

# set up database connection (credentials from config file)
class Setup:

    def __init__(self, user, pwd, host, port, db, service_name, region_name, aws_access_key_id, aws_secret_access_key):
        self.user = user
        self.pwd = pwd
        self.host = host
        self.port = port
        self.db = db
        self.service_name = service_name
        self.region_name = region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key


    def create_database(self):
        engine = create_engine(
            f"mysql+mysqlconnector://{self.user}:{self.pwd}@{self.host}:{self.port}/",
            echo=False)
        conn = engine.connect()
        conn.execute("CREATE DATABASE IF NOT EXISTS Bikeshare;")
        conn.close()

    def create_connection(self):
        engine = create_engine(
            f"mysql+mysqlconnector://{self.user}:{self.pwd}@{self.host}:{self.port}/{self.db}",
            echo=False)
        return engine.connect()

    def close_connection(self):
        engine = create_engine(
            f"mysql+mysqlconnector://{self.user}:{self.pwd}@{self.host}:{self.port}/{self.db}",
            echo=False)
        conn = engine.connect()
        conn.close()

    def s3(self):
        s3 = boto3.resource(
            service_name=self.service_name,
            region_name=self.region_name,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key)
        return s3


# call Setup class from setup_cloud.py file
# connection = Setup(config_cloud.user, config_cloud.pwd, config_cloud.host, config_cloud.port, config_cloud.db)

