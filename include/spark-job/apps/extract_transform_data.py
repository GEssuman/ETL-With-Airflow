from pyspark.sql import SparkSession
import pyspark.sql.functions as F 
from pyspark.sql.types import *
import os
from datetime import datetime
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from helper_functions import load_from_s3, create_spark_session, load_data_to_db, validate_columns
import logging


# ##Load env vairables
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
    try:
        logging.info(f"Reading data from database-:{RDS_POSTGRES_DB} table-:users")
        users_ddf = spark.read.format("jdbc")\
        .option(f"url", f"jdbc:postgresql://rds_postgres:5432/{RDS_POSTGRES_DB}")\
            .option("user", RDS_POSTGRES_USER)\
                .option("password", RDS_POSTGRES_PASSWORD)\
                    .option("dbtable", "users")\
                        .option("driver", "org.postgresql.Driver")\
                        .load()
        return users_ddf
    except Exception as e:
        logging.error(f"Unable to read data from database-:{RDS_POSTGRES_DB}\n{e}")



def extract_song_metadata(spark):
    try:
        logging.info(f"Reading data from database-:{RDS_POSTGRES_DB} table-:songs")
        songs_ddf = spark.read.format("jdbc")\
        .option(f"url", f"jdbc:postgresql://rds_postgres:5432/{RDS_POSTGRES_DB}")\
            .option("user", RDS_POSTGRES_USER)\
                .option("password", RDS_POSTGRES_PASSWORD)\
                    .option("dbtable", "songs")\
                        .option("driver", "org.postgresql.Driver")\
                        .load()
        return songs_ddf
    except Exception as e:
        logging.error(f"Unable to read data from database-:{RDS_POSTGRES_DB}\n{e}")

# def transform_data(stream_df, user_df, song_df):
#     try:
#         cleaned_song_df = song_df.drop(*["explicit", "danceability", "energy", "key", "loudness", "mode", "speechiness", "acousticness", "intrumentalness", "liveness", "valence", "tempo", "time_signature"])
#         cleaned_user_df = user_df.drop(*["user_age", "user_country", "created_at"])
#         cleaned_stream_df = stream_df.dropna(subset=["track_id"])


#         transformed_df = cleaned_stream_df.join(cleaned_song_df, on="track_id", how="left").join(cleaned_user_df, on="user_id", how="left")
#         transformed_df = transformed_df.fillna(value="Unknown", subset =["artists", "album_name"])


#         transformed_df.drop_duplicates()
    
#         return  transformed_df
#     except Exception as e:
#         logging.error(f"Tranformation Failed\n{e}")




if __name__ == "__main__":
    s3_bucket = f"s3a://{AWS_S3_BUCKET}/stream-data/"
    
    spark = create_spark_session()

    song_df = extract_song_metadata(spark)
    user_df = extract_user_metadata(spark)
    for key, value in spark.sparkContext.getConf().getAll():
        if 's3a' in key:
            print(f"{key}: {value}")
    
    stream_df = load_from_s3(spark, schema=streaming_schema, s3_bucket=s3_bucket, is_header=True)

#     stream_df_req_cols = ["user_id", "track_id", "listen_time"]
#     user_df_req_cols = ["user_id","user_name","user_age","user_country","created_at"]
#     song_df_req_cols = ["id","track_id","artists","album_name","track_name","popularity","duration_ms","time_signature","track_genre"]

#     try:
#         if not(validate_columns(stream_df,stream_df_req_cols)\
#             and validate_columns(user_df,user_df_req_cols)\
#         and validate_columns(song_df,song_df_req_cols)):
#             raise Exception("Failed to validate Data")
#     except Exception as e:
#         logging.error(f"Failed to validate date-;\n{e}")


    
#     properties = {
#         "db_name":RDS_POSTGRES_DB,
#         "user": RDS_POSTGRES_USER,
#         "password": RDS_POSTGRES_PASSWORD,
#         "driver": "org.postgresql.Driver",
#         "table":"staging.transformed_data"
#     }


#     transfromed_df = transform_data(stream_df=stream_df, song_df=song_df, user_df=user_df)

#     load_data_to_db(transfromed_df, properties)

    stream_df.show()
    print("hihihihih")

    spark.stop()