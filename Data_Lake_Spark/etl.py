import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Process the data from S3 JSON files

    Args:
        spark (object): spark object
        input_data (string): input path of S3
        output_data (string): output path of S3
    """    
    
    # get filepath to song data 
    song_data = input_data + 'song-data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # created song view for SQL Spark View
    df.createOrReplaceTempView("songs_table")

    # extract columns to create songs table
    songs_table = spark.sql("""
                            SELECT DISTINCT 
                                s.song_id, 
                                s.title,
                                s.artist_id, 
                                s.year, 
                                s.duration
                            FROM 
                                songs_table s
                            WHERE
                                s.song_id IS NOT NULL
                            """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data+'songs_table/')

    # extract columns to create artists table
    artists_table = spark.sql("""
                            SELECT DISTINCT 
                                a.artist_id, 
                                a.artist_name,
                                a.artist_location,
                                a.artist_latitude,
                                a.artist_longitude
                            FROM 
                                songs_table a
                            WHERE 
                                a.artist_id 
                            IS NOT NULL
                            """)
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+'artists_table/')


def process_log_data(spark, input_data, output_data):
    """Process the data from S3 Log files

    Args:
        spark (object): spark object
        input_data (string): input path of S3
        output_data (string): output path of S3
    """    

    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # created log view for SQL Spark View
    df.createOrReplaceTempView("logs_table")

    # extract columns for users table    
    users_table = spark.sql("""
                            SELECT DISTINCT 
                                u.userId AS user_id, 
                                u.firstName AS first_name,
                                u.lastName AS last_name,
                                u.gender AS gender,
                                u.level AS level
                            FROM 
                                logs_table u
                            WHERE 
                                u.userId
                            IS NOT NULL
                        """)
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'users_table/')

    date_time_table = spark.sql("""
                                SELECT DISTINCT
                                    timedata.t as start_time,
                                    hour (timedata.t) as hour,
                                    dayofmonth (timedata.t) as day,
                                    weekofyear (timedata.t) as week,
                                    month (timedata.t) as month,
                                    year (timedata.t) as year,
                                    dayofweek (timedata.t) as weekday
                                FROM (
                                SELECT 
                                    to_timestamp (timestp.ts/1000) AS t
                                FROM 
                                    logs_table timestp
                                WHERE 
                                    timestp.ts 
                                IS NOT NULL) 
                                timedata
                                """)
    
    # write time table to parquet files partitioned by year and month
    date_time_table.mode('overwrite').partitionBy("year", "month").parquet(output_data + 'date_time_table/') 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                                SELECT 
                                    monotonically_increasing_id() AS songplay_id,
                                    to_timestamp(l.ts/1000) AS start_time,
                                    month(to_timestamp(l.ts/1000)) AS month,
                                    year(to_timestamp(l.ts/1000)) AS year,
                                    l.userId AS user_id,
                                    l.level AS level,
                                    s.song_id AS song_id,
                                    s.artist_id AS artist_id,
                                    l.sessionId AS session_id,
                                    l.location AS location,
                                    l.userAgent AS user_agent
                                FROM 
                                    logs_table l
                                JOIN 
                                    songs_table s 
                                ON 
                                    l.artist = s.artist_name 
                                AND 
                                    l.song = s.title
                                """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.mode('overwrite').partitionBy("year", "month").parquet(output_data + 'songplays_table/') 


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-nano-spark/dloutput/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
