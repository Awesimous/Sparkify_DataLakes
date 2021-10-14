import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

# input_data_song = "s3a://udacity-dend/song_data/A/B/A/*.json"
# input_data_log = "s3a://udacity-dend/log_data/2018/11/*.json"

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    # import ipdb; ipdb.set_trace()
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').drop_duplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(f'{output_data}/songs.parquet').partitionBy("year","artist")
        
    # extract columns to create artists table
    artists_table = df.select('artist_id', 'name', \
        'location', 'lattitude', 'longitude').drop_duplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(f'{output_data}/artists.parquet')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*.json")

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.select('user_id', 'first_name', 'last_name', 'gender', 'level')\
        .drop_duplicates()
    
    # write users table to parquet files
    users_table.write.parquet(f'{output_data}/users.parquet')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(from_unixtime(x)))
    df = df.withColumn("timestamp", get_timestampUDF(col("ts")))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(x)))
    df = df.withColumn("datetime", get_timestampUDF(col("ts")))
    
    # extract columns to create time table
    time_table = df.select('datetime') \
        .withColumn('Year', df.dt.year)\
        .withColumn('Month', df.dt.month)\
        .withColumn('Week', df.dt.day)\
        .withColumn('Day', df.dt.day)\
        .withColumn('Day', df.dt.hour)\
        .withColumn('weekday', df.dt.dayofweek)\
        
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(f'{output_data}/time.parquet').partitionBy("year","month")

    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data, "song_data/*/*/*/*.json"))

    # extract columns from joined song and log datasets to create songplays table 
    joined_df = df.join(song_df, col('df.artist') == col(
        'song_df.artist_name'), 'inner')
    songplays_table = joined_df.select(
        col('log_df.datetime').alias('start_time'),
        col('log_df.userId').alias('user_id'),
        col('log_df.level').alias('level'),
        col('song_df.song_id').alias('song_id'),
        col('song_df.artist_id').alias('artist_id'),
        col('log_df.sessionId').alias('session_id'),
        col('log_df.location').alias('location'), 
        col('log_df.userAgent').alias('user_agent'),
        year('log_df.datetime').alias('year'),
        month('log_df.datetime').alias('month')) \
        .withColumn('songplay_id', monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(f'{output_data}/songplays.parquet').partitionBy("year","month")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
