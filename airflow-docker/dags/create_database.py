# create database

def create_database():

    cursor.execute('set GLOBAL max_allowed_packet=1073741824')
    cursor.execute("set GLOBAL sql_mode=''")
    cursor.execute("DROP TABLE IF EXISTS rides;")
    cursor.execute("""
    CREATE TABLE rides (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, 
    start_lat DECIMAL(9,7), end_lat DECIMAL(9,7), 
    start_lng DECIMAL(9,7), end_lng DECIMAL(9,7), 
    started_at DATETIME NOT NULL, ended_at DATETIME NOT NULL,
    start_station_name VARCHAR(64) NOT NULL, end_station_name VARCHAR(64) NOT NULL, 
    start_station_id VARCHAR(15) NOT NULL, end_station_id VARCHAR(15) NOT NULL, 
    member_casual VARCHAR(10) NOT NULL, 
    ride_id VARCHAR(20) NOT NULL, is_equity VARCHAR(20) NOT NULL)
    ;""")