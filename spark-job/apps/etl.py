from pyspark.sql import SparkSession
import pyspark.sql.functions as F 
from pyspark.sql.types import *
import os
from datetime import datetime





def create_spark_session():
    spark = SparkSession.builder \
            .appName("MusicStreamingETL") \
            .getOrCreate()
    return spark

def extract_user_metadata():
    pass

def extract_song_metada():
    pass

def stream_from_s3():
    pass


if __name__=="__main__":
    spark = create_spark_session()
    print(f"hello world {spark}")