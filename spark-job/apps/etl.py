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

def stream_from_s3(spark):
    batch_data = spark.read\
    .option("header", "true")\
        .schema(streaming_schema)\
            .csv(f"s3a://{AWS_S3_BUCKET}/stream-data/")\
    
    return batch_data



def validate_columns(df, required_columns):
    return set(required_columns).issubset(set(df.columns))

def validate_not_null(df, not_null_columns):
    for col in not_null_columns:
        null_count = df.filter(df[col].isNull()).count()
        if null_count > 0:
            print(f"Column '{col}' has {null_count} null value(s).")
            return False
    return True



def clean_date(df,):
    df.drop_duplicates()






def transform_data(stream_df, user_df, song_df):
    cleaned_song_df = song_df.drop(*["explicit", "danceability", "energy", "key", "loudness", "mode", "speechiness", "acousticness", "intrumentalness", "liveness", "valence", "tempo", "time_signature"])
    cleaned_user_df = user_df.drop(*["user_age", "user_country", "created_at"])
    cleaned_stream_df = stream_df.dropna(subset=["track_id"])


    transformed_df = cleaned_stream_df.join(cleaned_song_df, on="track_id", how="left").join(cleaned_user_df, on="user_id", how="left")
    transformed_df = transformed_df.fillna(value="Unknown", subset =["artists", "album_name"])
    transformed_df.createOrReplaceTempView("streamed_music_tb")
    return  transformed_df
def validate_data(df, required_columns, not_null_columns):
    return validate_columns(df, required_columns=required_columns) and validate_not_null(df, not_null_columns=not_null_columns)


def listener_count_per_gener(df):
    return df.groupBy("track_genre").agg(
    F.count("user_id").alias("listen_count"),
    F.mean("duration_ms").alias("avg_track_duration")
)

def most_popular_track_per_gener():
    spark.sql("""
              WITH ranked_tracks AS (
                SELECT
                    track_genre,
                    track_name,
                    popularity,
                    RANK() OVER (PARTITION BY track_genre ORDER BY popularity DESC) as most_popular_rank
                FROM streamed_music_tb
              )
              SELECT
                track_genre,
                track_name,
                popularity 
              FROM ranked_tracks
              WHERE most_popular_rank=1;
              """).show()


def unique_listners_per_hour():
    pass

if __name__=="__main__":
    spark = create_spark_session()
    print(f"hello world {spark}")

    stream_df = stream_from_s3(spark)
    user_df = extract_user_metadata(spark=spark)
    song_df = extract_song_metadata(spark)


    stream_df_req_cols = ["user_id", "track_id", "listen_time"]
    stream_df_not_null_cols = ["user_id", "track_id"]
    user_df_req_cols = ["user_id","user_name","user_age","user_country","created_at"]
    user_df_not_null_cols = ["user_id","user_name"]
    song_df_req_cols = ["id","track_id","artists","album_name","track_name","popularity","duration_ms","time_signature","track_genre"]
    song_df__not_null_cols = ["id","track_id","artists","album_name","track_name","track_genre"]

    stream_df_is_valid = validate_data(stream_df, stream_df_req_cols, stream_df_not_null_cols)
    user_df_is_valid = validate_data(user_df, user_df_req_cols, user_df_not_null_cols)
    song_df_is_valid = validate_data(song_df, song_df_req_cols, song_df__not_null_cols)
    if (stream_df_is_valid and user_df_is_valid and song_df_is_valid):
        print("Validation Gone Wrong")
    transfromed_df = transform_data(stream_df=stream_df, song_df=song_df, user_df=user_df)


    df = listener_count_per_gener(transfromed_df)
    df.show()
    # transfromed_df.show(10)


