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
from pyspark.sql import SparkSession


spark = SparkSession.builder.config("spark.jars", "/Users/mcgaritym/mysql-connector-java-8.0.25/mysql-connector-java-8.0.25.jar") \
    .master("local").appName("MySQL_test").getOrCreate()

rides_df = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/Bikeshare") \
    .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "rides_test") \
    .option("user", "root").option("password", "Nalgene09!").load()


print(rides_df.count())