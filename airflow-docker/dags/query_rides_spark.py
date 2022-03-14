# import libraries
import pandas as pd
from config import *
from AWSConnect import AWSConnect
# from pyspark import SparkConf, SparkContext
# from pyspark.sql import SparkSession, SQLContext
# from pyspark.conf import SparkConf
import pyspark
from pyspark.sql import SparkSession
import findspark
import time


# function to query rides table for recent rides
def query_rides():

    # findspark and create spark session with configuration variables
    findspark.init('/Users/mcgaritym/server/spark-3.1.2-bin-hadoop2.7')
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("pyspark_rideshare") \
        .config("spark.driver.extraClassPath", "/Users/mcgaritym/server/mysql-connector-java-8.0.25/mysql-connector-java-8.0.25.jar") \
        .config("spark.executor.heartbeatInterval", "200000") \
        .config("spark.network.timeout", "300000") \
        .config("spark.driver.memory", "12G") \
        .getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

    sc = spark.sparkContext
    print('Session Created')

    rides = spark.read.format("jdbc").option("url", "jdbc:mysql://{}:{}/{}".format(rds_host, rds_port, rds_database)) \
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "rides") \
        .option("user", rds_user).option("password", rds_pwd) \
        .option("numPartitions", 50) \
        .option("fetchsize", "1000000") \
        .option("lowerbound", "0") \
        .option("upperbound", "50000000") \
        .option("partitionColumn", "new_id") \
        .load()

    rides.createOrReplaceTempView("rides")

    stations = spark.read.format("jdbc").option("url", "jdbc:mysql://{}:{}/{}".format(rds_host, rds_port, rds_database)) \
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "stations") \
        .option("user", rds_user).option("password", rds_pwd) \
        .option("fetchsize", "1000000") \
        .option("numPartitions", 50) \
        .load()

    stations.createOrReplaceTempView("stations")

    query = """
    SELECT rides.ride_id, rides.started_at, rides.ended_at, rides.duration,
    stations.name, stations.lat, stations.lon, stations.capacity, stations.short_name
    FROM rides
    JOIN stations
    ON rides.start_station_id = stations.short_name
    WHERE rides.started_at BETWEEN (NOW() - INTERVAL 100 DAY) AND NOW()
    ORDER BY rides.started_at DESC
    LIMIT 20;"""

    # query and show results
    query_results = spark.sql(query)
    query_results.show()

    # convert spark dataframe to pandas dataframe
    recent_rides = query_results.toPandas()
    print(recent_rides)

    # get class, and create connections, send dataframe to RDS
    connect = AWSConnect(rds_user, rds_pwd, rds_host, rds_port, rds_database, service_name, region_name, aws_access_key_id, aws_secret_access_key)
    rds_mysql_connection, rds_mysql_cursor = connect.rds_mysql()
    rds_sqlalchemy = connect.rds_sqlalchemy()
    recent_rides.to_sql(name='recent_rides', con=rds_sqlalchemy, if_exists="replace", chunksize=1000, index=False)

    return print("Recent Rides Query Successful")

query_rides()