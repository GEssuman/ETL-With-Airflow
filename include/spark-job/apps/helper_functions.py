from pyspark.sql import SparkSession
import pyspark.sql.functions as F 
from pyspark.sql.types import *
import os
from datetime import datetime
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
import logging

def create_spark_session():
    logging.info(f"Creating Spark Session")
    spark = SparkSession.builder \
            .appName("MusicStreamingETL") \
            .config("spark.jars", "/usr/local/airflow/include/spark-job/resources/postgresql-42.7.3.jar",) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true")\
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider") \
            .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
            .config("spark.hadoop.fs.s3a.connection.request.timeout", "60000") \
            .config("spark.hadoop.fs.s3a.socket.timeout", "60000") \
            .getOrCreate()
    return spark



def load_from_s3(spark, schema, s3_bucket, is_header):
    # try:
    logging.info(f"Loading data from S3-:{s3_bucket}")
    data = spark.read.option("header", is_header)\
            .schema(schema)\
                .csv(s3_bucket)
    return data
    # except Exception as e:
    #     logging.error(f"Could not load data from bucket-:{s3_bucket}\n{e}")

def load_to_s3(df, s3_bucket):
    try:
        logging.info(f"Loading data to S3-:{s3_bucket}") 
        df.coalesce(1).write.mode("append") \
            .format("csv") \
            .save(s3_bucket)
    except Exception as e:
        logging.error(f"Could not load data to bucket-:{s3_bucket}\n{e}") 


def load_data_to_db(df, db_info):
    try:
        logging.info(f"Loading data to database-:{db_info["db_name"]}") 
        df.write \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://rds_postgres:5432/{db_info["db_name"]}") \
        .option("dbtable", db_info["table"]) \
        .option("user", db_info["user"]) \
        .option("password", db_info["password"]) \
        .option("driver", db_info["driver"]) \
        .mode("append") \
        .save()
        logging.info(f"Successfully loaded to database-:{db_info["db_name"]}") 
    except Exception as e:
        logging.error(f"Unable to load data to database-:{db_info["db_name"]}\n{e}") 




def validate_columns(df, required_columns):
    logging.info(f"Valitdating columns") 
    return set(required_columns).issubset(set(df.columns))



def read_from_db(spark, db_info):
    try:
        logging.info(f"Reading data from database-:{db_info["db_name"]}") 
        return spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://rds_postgres:5432/{db_info["db_name"]}") \
        .option("dbtable", db_info["table"]) \
        .option("user", db_info["user"]) \
        .option("password", db_info["password"]) \
        .option("driver", db_info["driver"]) \
        .load()
    except Exception as e:
        logging.error(f"Unable to read data from database-:{db_info["db_name"]}\n{e}") 

