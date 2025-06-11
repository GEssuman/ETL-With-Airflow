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
    songs_ddf = spark.read.csv("jdbc")\
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


if __name__=="__main__":
    spark = create_spark_session()
    print(f"hello world {spark}")

    print(RDS_POSTGRES_DB)
    # user_df = extract_user_metadata(spark)
    # song_df = extract_song_metadata(spark)
    stream_data = stream_from_s3(spark)
    # print(user_df.count(), song_df.count())
    print(stream_data.count())
    # user_df.count()


