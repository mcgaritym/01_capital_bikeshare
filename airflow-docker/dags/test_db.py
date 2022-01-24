# load required libraries
from sqlalchemy import create_engine
import os
from glob import glob
import pandas as pd
from datetime import timedelta, date, datetime

# specify SQL credentials
aws_user = 'admin'
aws_pwd = "Nalgene09!"
aws_host = 'stocks-db.cflkt9l7na18.us-east-1.rds.amazonaws.com'
aws_port = int(3306)
aws_database = 'stocks-db'

# specify second MySQL database connection (faster read_sql query feature)
connection_2 = create_engine(
    "mysql+pymysql://{user}:{password}@{host}:{port}/{db}".format(user=aws_user, password=aws_pwd, host=aws_host, port=aws_port,
                                                                  db=aws_database))

