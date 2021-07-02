# import libraries
import pandas as pd
import glob
import os
import boto3
from src.setup.setup import Setup
from src.setup import config

# call Setup class as connection
connection = Setup(config.user, config.pwd, config.host, config.port, 'Bikeshare',
                   config.service_name, config.region_name, config.aws_access_key_id,
                   config.aws_secret_access_key)

# create AWS RDS database and connection
connection.rds_database()
rds = connection.rds_connect()

def create_table(table_name):

    # create a rides table
    rds.execute("DROP TABLE IF EXISTS {};".format(table_name))

    # create ride table
    rds.execute("""CREATE TABLE IF NOT EXISTS {} (
                    `Start date` TEXT,
                    `End date` TEXT,
                    `Start station number` INT,
                    `Start station` TEXT,
                    `End station number` INT,
                    `End station` TEXT,
                    `Bike number` TEXT,
                    `Member type` TEXT,
                    ride_id TEXT,
                    rideable_type TEXT,
                    started_at TEXT,
                    ended_at TEXT,
                    Duration DECIMAL(7,2),
                    start_station_name TEXT,
                    start_station_id INT,
                    end_station_name TEXT,
                    end_station_id INT,
                    start_lat DECIMAL(11,8),
                    start_lng DECIMAL(11,8),
                    end_lat DECIMAL(11,8),
                    end_lng DECIMAL(11,8),
                    member_casual TEXT,
                    is_equity TEXT,
                    `index` INT);
                    """.format(table_name))

    # display records
    output = rds.execute("""SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'Bikeshare'
    AND table_name = 'rides';""")

    # print columns
    [print(row) for row in output]

# call function
create_table('rides')

# def populate_table(bucket_name, object_name, table_name):

    # get objects from bucket

    # insert into table

    # print records




