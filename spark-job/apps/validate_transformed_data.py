from pyspark.sql import SparkSession
import pyspark.sql.functions as F 
from pyspark.sql.types import *
import os
from datetime import datetime
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from helper_functions import load_from_s3, read_from_db, create_spark_session

#Load env vairables
RDS_POSTGRES_DB = os.getenv("RDS_POSTGRES_DB")
RDS_POSTGRES_USER = os.getenv("RDS_POSTGRES_USER")
RDS_POSTGRES_PASSWORD = os.getenv("RDS_POSTGRES_PASSWORD")
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")


def validate_not_null(df, not_null_columns):
    for col in not_null_columns:
        null_count = df.filter(df[col].isNull()).count()
        if null_count > 0:
            print(f"Column '{col}' has {null_count} null value(s).")
            return False
    return True



transformed_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("track_id", StringType(), True),
    StructField("listen_time", TimestampType(), True),
    StructField("id", IntegerType(), True),
    StructField("artists", StringType(), True),
    StructField("album_name", StringType(), True),
    StructField("track_name", StringType(), True),
    StructField("popularity", IntegerType(), True),
    StructField("duration_ms", IntegerType(), True),
    StructField("instrumentalness", FloatType(), True),
    StructField("track_genre", StringType(), True),
    StructField("user_name", StringType(), True),
])



if __name__=="__main__":
    spark = create_spark_session()
    print(f"hello world {spark}")

    transformed_df = load_from_s3(spark, transformed_schema, 
                                  f"s3a://music-staging-gke/transformed_streamed_music/", 
                                  is_header=False)
    
    
    not_nul_cols = ["track_id", "duration_ms", "popularity", "track_genre"]
    
    is_valid = validate_not_null(transformed_df, not_nul_cols)

    if not is_valid:
        raise


    transformed_properties = {
        "db_name":RDS_POSTGRES_DB,
        "user": RDS_POSTGRES_USER,
        "password": RDS_POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
        "table":"staging.transformed_data"
    }



    transformed_df = load_from_s3(spark, transformed_schema, f"s3a://music-staging-gke/transformed_streamed_music/", is_header=False)
    transformed_df.createOrReplaceTempView("streamed_music_tb")




    transformed_df.write \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://rds_postgres:5432/{transformed_properties["db_name"]}") \
    .option("dbtable", transformed_properties["table"]) \
    .option("user", transformed_properties["user"]) \
    .option("password", transformed_properties["password"]) \
    .option("driver", transformed_properties["driver"]) \
    .save()




