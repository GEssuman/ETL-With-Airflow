from pyspark.sql import SparkSession
import pyspark.sql.functions as F 
from pyspark.sql.types import *
import os
from datetime import datetime
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from helper_functions import load_from_s3, create_spark_session, load_data_to_db, load_to_s3



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
    # transformed_df.createOrReplaceTempView("streamed_music_tb")
    return  transformed_df



if __name__ == "__main__":
    s3_bucket = f"s3a://{AWS_S3_BUCKET}/stream-data/"
    
    spark = create_spark_session()

    song_df = extract_song_metadata(spark)
    user_df = extract_user_metadata(spark)

    stream_df = load_from_s3(spark, schema=streaming_schema, s3_bucket=s3_bucket, is_header=True)

    tream_df_req_cols = ["user_id", "track_id", "listen_time"]
    stream_df_not_null_cols = ["user_id", "track_id"]
    user_df_req_cols = ["user_id","user_name","user_age","user_country","created_at"]
    user_df_not_null_cols = ["user_id","user_name"]
    song_df_req_cols = ["id","track_id","artists","album_name","track_name","popularity","duration_ms","time_signature","track_genre"]
    song_df__not_null_cols = ["id","track_id","artists","album_name","track_name","track_genre"]


    
    properties = {
        "db_name":RDS_POSTGRES_DB,
        "user": RDS_POSTGRES_USER,
        "password": RDS_POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
        "table":"staging.transformed_data"
    }


    transfromed_df = transform_data(stream_df=stream_df, song_df=song_df, user_df=user_df)

    transfromed_df.count()
    load_to_s3(transfromed_df, f"s3a://music-staging-gke/transformed_streamed_music/")
    # load_data_to_db(transfromed_df, properties)