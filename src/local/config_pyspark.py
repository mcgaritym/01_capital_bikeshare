# import libraries
import json
import requests
import pandas as pd
import mysql.connector
from mysql.connector.cursor import MySQLCursor
import pymysql
import config_local
from setup_local import Setup
import glob
import os
import pyspark
from pyspark.sql import SparkSession

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext

# create the session
conf = SparkConf().set("spark.ui.port", "4040")

# create the context
sc = pyspark.SparkContext(conf=conf)

# Create my_spark
spark = SparkSession.builder.getOrCreate()

df = spark.read.csv("202011-capitalbikeshare-tripdata.csv", header=True)
print(df.show(10))
print(type(df))


