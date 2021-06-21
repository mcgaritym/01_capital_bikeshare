
# import libraries
from sqlalchemy import create_engine
import config_cloud
from mysql import connector
# set up database connection (credentials from config file)
class Setup:

    def __init__(self, user, pwd, host, port, db):
        self.user = user
        self.pwd = pwd
        self.host = host
        self.port = port
        self.db = db

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


# call Setup class from setup_cloud.py file
connection = Setup(config_cloud.user, config_cloud.pwd, config_cloud.host, config_cloud.port, config_cloud.db)

