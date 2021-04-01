import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import pyspark.sql.functions as F

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
"""
Description:
Create a spark session 
"""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
"""
Description:
    1. read json file and filter some data. Remove duplicate data.
    2. write parquet file partitioned by year and artist callled songs.
    3. write parquet file callled artists.
    
Parameters:
    spark session, json path, parquet path
"""
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/*")
    # read song data file   
    df=spark.read.json(song_data)    
    
      
    # extract columns to create songs table
    songs_table = df.select("song_id","title","artist_id","year","duration").drop_duplicates()    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs.parquet'), 'overwrite')
    
    # extract columns to create artists table
    artists_table = df.select("artist_id","artist_name","artist_location","artist_latitude","artist_longitude").drop_duplicates()
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists.parquet'), 'overwrite')
                               


def process_log_data(spark, input_data, output_data):
"""
Description:
    1. read json file and filter some data. Remove duplicate data.
    2. write parquet file callled users.
    3. transform timestamp information to get day, week, month, yaer, hour, weekday.
    3. write parquet file callled time with data transformed before.
    
Parameters:
    spark session, json path, parquet path
"""
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*")

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")
    
    # extract columns for users table    
    users_table = df.select("userid","firstName","lastName","gender","level").drop_duplicates()
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users.parquet'), 'overwrite')                          
        
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: int((int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x).isoformat())
    df = df.withColumn('date', get_datetime(df.timestamp))
    # extract columns to create time table
    time_table = df.select(col('timestamp').alias('start_time'),
                       hour('date').alias('hour'),
                       dayofmonth('date').alias('day'),
                       weekofyear('date').alias('week'),
                       month('date').alias('month'),
                       year('date').alias('year'),
                       date_format('date','E').alias('weekday'))

   
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, 'time.parquet'), 'overwrite')
    
    
    # read in song data to use for songplays table    
    song_data = os.path.join(input_data, "song_data/*/*/*/*")
    song_df=spark.read.json(song_data)     
   
    # extract columns from joined song and log datasets to create songplays table 
    log_song_df = df.join(song_df, df.song == song_df.title)
    log_song_df_time = log_song_df.join(time_table, log_song_df.timestamp == time_table.start_time).drop(log_song_df.year)     
    
    
    songplays_table = log_song_df_time.select(F.monotonically_increasing_id().alias("songplay_id"), 
                                              log_song_df_time.timestamp.alias("start_time"), 
                                              log_song_df_time.userId.alias("user_id"), 
                                              log_song_df_time.level.alias("level"), 
                                              log_song_df_time.song_id.alias("song_id"),
                                              log_song_df_time.artist_id.alias("artist_id"), 
                                              log_song_df_time.sessionId.alias("session_id"), 
                                              log_song_df_time.location.alias("location"), 
                                              log_song_df_time.userAgent.alias("user_agent"),
                                              log_song_df_time.yaer.alias("year"),
                                              log_song_df_time.month.alias("month"))
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'songplay.parquet'), 'overwrite')

def main():
"""
Description:
    Main function. Call process_song_data and process_log_data        
"""
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
