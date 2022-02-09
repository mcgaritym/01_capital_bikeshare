# import libraries
import pandas as pd
from config import *
from AWSConnect import AWSConnect
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.conf import SparkConf

# function to query rides table for recent rides
def query_rides():

    # get class, and create connections
    connect = AWSConnect(rds_user, rds_pwd, rds_host, rds_port, rds_database, service_name, region_name, aws_access_key_id, aws_secret_access_key)
    rds_mysql_connection, rds_mysql_cursor = connect.rds_mysql()
    rds_sqlalchemy = connect.rds_sqlalchemy()

    # try:
    #     rds_mysql_cursor.execute("ALTER TABLE rides ORDER BY `started_at` DESC;")
    #     rds_mysql_cursor.execute("ALTER TABLE rides DROP id, ADD new_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY;")
    #     rds_mysql_cursor.close()
    #
    # except:
    #     pass

    # # read query
    # recent_rides = pd.read_sql_query("""
    # SELECT rides.ride_id, rides.started_at, rides.ended_at, rides.duration,
    # stations.name, stations.lat, stations.lon, stations.capacity, stations.short_name
    # FROM rides
    # JOIN stations
    # ON rides.start_station_id = stations.short_name
    # ORDER BY `started_at` DESC
    # LIMIT 10;""", con=rds_sqlalchemy)

    # # read query
    # recent_rides = pd.read_sql_query("""SELECT COUNT(*) FROM rides;""", con=rds_sqlalchemy)
    #
    # print('Recent Rides: ', recent_rides)


    # spark = SparkSession.builder.config("spark.jars", "/Users/mcgaritym/server/mysql-connector-java-8.0.25/mysql-connector-java-8.0.25.jar") \
    #     .master("local").appName("PySpark_Rideshare").getOrCreate()
    #
    # rides_df = spark.read.format("jdbc").option("url", "jdbc:mysql://rideshare-db.cflkt9l7na18.us-east-1.rds.amazonaws.com:3306/rideshare_db") \
    #     .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "rides") \
    #     .option("user", "admin").option("password", "Nalgene09!").load()

    spark_session = SparkSession \
        .builder \
        .appName("pyspark_rideshare") \
        .config("spark.driver.extraClassPath", "/Users/mcgaritym/server/mysql-connector-java-8.0.25/mysql-connector-java-8.0.25.jar") \
        .config("spark.executor.heartbeatInterval", "200000") \
        .config("spark.network.timeout", "300000") \
        .config("spark.executor.cores", "6") \
        .config("spark.driver.memory", "12g") \
        .config('spark.master', 'local[8]') \
        .getOrCreate()

    print(spark_session.sparkContext.getConf().getAll())
    sqlContext = SQLContext(spark_session)
    sqlContext.setConf("spark.default.parallelism", "2")
    print(sqlContext)

    dataframe_mysql = sqlContext.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://rideshare-db.cflkt9l7na18.us-east-1.rds.amazonaws.com:3306/rideshare_db") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "rides") \
        .option("user", "admin") \
        .option("password", "Nalgene09!") \
        .option("fetchsize", "100000") \
        .load()

    dataframe_mysql.repartition(18, 'started_at')

    print(dataframe_mysql.columns)

    dataframe_mysql.registerTempTable("temp_rides")
    sqlContext.cacheTable("temp_rides")
    print('ok1')
    query = """SELECT * FROM temp_rides WHERE started_at > '2021-10-31'"""

    print(sqlContext.sql(query).show())


    # # Create a temporary table "rides"
    # rides_temp = dataframe_mysql.createOrReplaceTempView("rides_temp")

    # print(dataframe_mysql.show(10))

    # print(dataframe_mysql.filter(dataframe_mysql['started_at'] > '2021-11-28').show())




    # rides = spark.read \
    #     .format("jdbc") \
    #     .option("url", "jdbc:postgresql:dbserver") \
    #     .option("dbtable", "schema.tablename") \
    #     .option("user", "username") \
    #     .option("password", "password") \
    #     .load()

    # recent_rides = spark.sql('SELECT COUNT(*) FROM rides')
    #
    # print('Recent Rides: ', recent_rides)

    # # send results to sql, and save to csv
    # recent_rides.to_sql(name='recent_rides', con=rds_sqlalchemy, if_exists="replace", chunksize=1000, index=False)

    return print("Recent Rides Query Successful")

query_rides()