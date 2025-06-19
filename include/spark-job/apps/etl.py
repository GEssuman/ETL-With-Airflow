from pyspark.sql import SparkSession
import pyspark.sql.functions as F 
from pyspark.sql.types import *
import os
from datetime import datetime
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType


##Load env vairables
RDS_POSTGRES_DB = os.getenv("RDS_POSTGRES_DB")
RDS_POSTGRES_USER = os.getenv("RDS_POSTGRES_USER")
RDS_POSTGRES_PASSWORD = os.getenv("RDS_POSTGRES_PASSWORD")
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")



streaming_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("track_id", StringType(), True),
    StructField("listen_time", TimestampType(), True)
])


def create_spark_session():
    spark = SparkSession.builder \
            .appName("MusicStreamingETL") \
            .config("spark.jars", "/opt/spark/resources/postgresql-42.7.3.jar") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider") \
            .getOrCreate()
    return spark


def load_from_s3(spark, schema, s3_bucket, is_header):
    data = spark.read\
    .option("header", is_header)\
        .schema(schema)\
            .csv(s3_bucket)
    return data

def extract_user_metadata(spark):
    users_ddf = spark.read.format("jdbc")\
    .option(f"url", f"jdbc:postgresql://rds_postgres:5432/{RDS_POSTGRES_DB}")\
        .option("user", RDS_POSTGRES_USER)\
            .option("password", RDS_POSTGRES_PASSWORD)\
                .option("dbtable", "users")\
                    .option("driver", "org.postgresql.Driver")\
                    .load()
    return users_ddf

def extract_song_metadata(spark):
    songs_ddf = spark.read.format("jdbc")\
    .option(f"url", f"jdbc:postgresql://rds_postgres:5432/{RDS_POSTGRES_DB}")\
        .option("user", RDS_POSTGRES_USER)\
            .option("password", RDS_POSTGRES_PASSWORD)\
                .option("dbtable", "songs")\
                    .option("driver", "org.postgresql.Driver")\
                    .load()
    return songs_ddf



def transform_data(stream_df, user_df, song_df):
    cleaned_song_df = song_df.drop(*["explicit", "danceability", "energy", "key", "loudness", "mode", "speechiness", "acousticness", "intrumentalness", "liveness", "valence", "tempo", "time_signature"])
    cleaned_user_df = user_df.drop(*["user_age", "user_country", "created_at"])
    cleaned_stream_df = stream_df.dropna(subset=["track_id"])


    transformed_df = cleaned_stream_df.join(cleaned_song_df, on="track_id", how="left").join(cleaned_user_df, on="user_id", how="left")
    transformed_df = transformed_df.fillna(value="Unknown", subset =["artists", "album_name"])
    # transformed_df = transformed_df.withColumn("listen_time", F.date_format("listen_time", "yyyy-MM-dd HH"))
    transformed_df.createOrReplaceTempView("streamed_music_tb")
    return  transformed_df



def listener_count_per_gener(df):
    return df.groupBy("track_genre").agg(
    F.count("*").alias("listen_count"),
    F.mean("duration_ms").alias("avg_track_duration")
)

def most_popular_track_per_gener(spark):
    return spark.sql("""
              WITH ranked_tracks AS (
                SELECT
                    track_genre,
                    track_name,
                    popularity,
                    COUNT(*) OVER (PARTITION BY track_genre) AS listen_count,
                    AVG(duration_ms) OVER (PARTITION BY track_genre) AS avg_track_duration,
                    RANK() OVER (PARTITION BY track_genre ORDER BY popularity DESC) as most_popular_rank
                FROM streamed_music_tb
              )
              SELECT
                track_genre,
                listen_count,
                avg_track_duration,
                track_name AS most_popular_track,
                popularity 
              FROM ranked_tracks
              WHERE most_popular_rank=1
              ORDER BY popularity;
              """)

# date_trunc('hour', listen_time)
def unique_listners_per_hour(spark):
    return spark.sql("""
            WITH hour_stream AS (
                SELECT
                    user_id,
                    track_id,
                    artists,
                    date_trunc('hour', listen_time) as listen_time_hour
                FROM streamed_music_tb
            ),
            listener_counts AS (
                SELECT 
                    COUNT(DISTINCT user_id) AS unique_listeners,
                    listen_time_hour
                FROM hour_stream
                GROUP BY listen_time_hour
              ),
            artists_rankings AS (
                SELECT 
                    track_id,
                    listen_time_hour,
                    ROW_NUMBER() OVER (PARTITION BY listen_time_hour ORDER BY COUNT(*) DESC) AS artist_rank
                FROM hour_stream
                GROUP BY listen_time_hour, track_id
              ),
              track_artists AS (
                SELECT 
                    DISTINCT track_id, listen_time_hour, artists
                FROM hour_stream
            ),
            track_diversity AS (
                SELECT
                    listen_time_hour,
                    COUNT(DISTINCT track_id) AS unique_tracks,
                    COUNT(track_id) AS total_plays
                FROM hour_stream
                GROUP BY listen_time_hour
              )
            SELECT
                lc.listen_time_hour,
                ta.artists,
                lc.unique_listeners,
                td.unique_tracks, 
                td.total_plays,
                ROUND(td.unique_tracks * 1.0 / td.total_plays, 5) AS track_diversity_index
              FROM listener_counts lc
              JOIN artists_rankings ar ON lc.listen_time_hour = ar.listen_time_hour
              JOIN track_artists ta ON ar.track_id = ta.track_id AND ar.listen_time_hour = ta.listen_time_hour
              JOIN track_diversity td ON lc.listen_time_hour = td.listen_time_hour
              WHERE ar.artist_rank=1
              ORDER BY lc.listen_time_hour;
            """)
    

def load_data_to_db(df, schema):
    df.write \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://rds_postgres:5432/{RDS_POSTGRES_DB}") \
    .option("dbtable", schema) \
    .option("user", RDS_POSTGRES_USER) \
    .option("password", RDS_POSTGRES_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

    

HOURLY_INSIGHT_UPSERT = """
INSERT INTO presentation.hourly_stream_insights AS target
(listen_time, artists, unique_listeners, unique_tracks, total_plays, track_diversity)
SELECT listen_time, artist, unique_listeners, unique_tracks, total_plays, track_diversity
FROM staging.hourly_stream_insights
ON CONFLICT (listen_time)
DO UPDATE SET
    artists = EXCLUDED.artists
    unique_listeners = EXCLUDED.unique_listeners,
    unique_tracks = EXCLUDED.unique_tracks,
    total_plays = EXCLUDED.total_plays,
    track_diversity = EXCLUDED.track_diversity;
"""


GENRE_INSIGHT_UPSERT = """
INSERT INTO analytics.hourly_stream_insights AS target
(listen_time, artist, unique_listeners, unique_tracks, total_plays, track_diversity)
SELECT listen_time, artist, unique_listeners, unique_tracks, total_plays, track_diversity
FROM analytics.hourly_stream_insights_staging
ON CONFLICT (listen_time, artist)
DO UPDATE SET
    unique_listeners = EXCLUDED.unique_listeners,
    unique_tracks = EXCLUDED.unique_tracks,
    total_plays = EXCLUDED.total_plays,
    track_diversity = EXCLUDED.track_diversity;
"""


if __name__=="__main__":
    spark = create_spark_session()
    print(f"hello world {spark}")

    stream_df = load_from_s3(spark, streaming_schema, f"s3a://{AWS_S3_BUCKET}/stream-data/", True)
    user_df = extract_user_metadata(spark=spark)
    song_df = extract_song_metadata(spark)


    stream_df_req_cols = ["user_id", "track_id", "listen_time"]
    stream_df_not_null_cols = ["user_id", "track_id"]
    user_df_req_cols = ["user_id","user_name","user_age","user_country","created_at"]
    user_df_not_null_cols = ["user_id","user_name"]
    song_df_req_cols = ["id","track_id","artists","album_name","track_name","popularity","duration_ms","time_signature","track_genre"]
    song_df__not_null_cols = ["id","track_id","artists","album_name","track_name","track_genre"]

    # stream_df_is_valid = validate_data(stream_df, stream_df_req_cols, stream_df_not_null_cols)
    # user_df_is_valid = validate_data(user_df, user_df_req_cols, user_df_not_null_cols)
    # song_df_is_valid = validate_data(song_df, song_df_req_cols, song_df__not_null_cols)
    # if (stream_df_is_valid and user_df_is_valid and song_df_is_valid):
    #     print("Validation Gone Wrong")
    transfromed_df = transform_data(stream_df=stream_df, song_df=song_df, user_df=user_df)


    # df = listener_count_per_gener(transfromed_df)
    # df.show()
    # most_popular_track_per_gener(spark)
    uni_df = unique_listners_per_hour(spark)
    uni_df.show()
    # load_data_to_db(uni_df, "staging.hourly_stream_insights")
    # transfromed_df.show(10)


