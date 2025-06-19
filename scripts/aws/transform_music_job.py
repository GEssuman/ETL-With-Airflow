import sys
import os
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
import pyspark.sql.functions as F
from awsglue.dynamicframe import DynamicFrame
import logging


logger = logging.getLogger()
logger.setLevel(logging.INFO)

# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define streaming schema
streaming_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("track_id", StringType(), True),
    StructField("listen_time", TimestampType(), True)
])


def validate_columns(df, required_columns):
    return set(required_columns).issubset(set(df.columns))

# Read Glue Catalog table: users
def extract_user_metadata():
    logger.info("Reading users table from Glue Catalog...")
    return glueContext.create_dynamic_frame.from_catalog(
        database="music_stream_db",
        table_name="users"
    ).toDF()

# Read Glue Catalog table: songs
def extract_song_metadata():
    logger.info("Reading songs table from Glue Catalog...")
    return glueContext.create_dynamic_frame.from_catalog(
        database="music_stream_db",
        table_name="songs"
    ).toDF()

# Transform
def transform_data(stream_df, user_df, song_df):
    logger.info("Transforming data...")
    cleaned_song_df = song_df.drop(*[
        "explicit", "danceability", "energy", "key", "loudness", "mode",
        "speechiness", "acousticness", "instrumentalness", "liveness", "valence",
        "tempo", "time_signature"
    ])
    cleaned_user_df = user_df.drop(*["user_age", "user_country", "created_at"])
    cleaned_stream_df = stream_df.dropna(subset=["track_id"])

    transformed_df = cleaned_stream_df \
        .join(cleaned_song_df, on="track_id", how="left") \
        .join(cleaned_user_df, on="user_id", how="left")

    transformed_df = transformed_df.fillna(value="Unknown", subset=["artists", "album_name"])
    logger.info(f"Transformation complete. Record count: {transformed_df.count()}")
    return transformed_df
# ---------------------

# Load from S3
try:
    logger.info("Job started...")
    stream_df = spark.read.option("header", "true").schema(streaming_schema).csv("s3a://music-stream-gke.amalitech/streams/")
    logger.info(f"Stream data loaded: {stream_df.count()} records")

    # Load from Glue Catalog
    user_df = extract_user_metadata()
    song_df = extract_song_metadata()

    #validating Data
    streams_required_columns = ["user_id", "listen_time", "track_id"]
    users_required_columns = ["user_id", "user_name", "created_at"]
    songs_required_columns = ["track_id", "duration_ms", "popularity"]
    logging.info("Validating Data")
    if not (validate_columns(stream_df, streams_required_columns) and validate_columns(user_df, users_required_columns) and validate_columns(song_df, songs_required_columns)):
        raise("Data Validation Failed")
    
        
    # Transform
    transformed_df = transform_data(stream_df, user_df, song_df)

    # Covert to dynamicFrame
    transformed_dyf = DynamicFrame.fromDF(
    dataframe = transformed_df,
    glue_ctx = glueContext,
    name = "transformed_dyf")

    # Write to S3
    logger.info("Writing transformed data to S3 in Parquet format...")
    glueContext.write_dynamic_frame.from_options(
    transformed_dyf,
    connection_type='s3',
    connection_options={'path': 's3://transformed-music-gke.amalitech/staging/'},
    format='parquet',
    transformation_ctx='write_to_s3'
    )

    job.commit()
    logger.info("Glue job completed successfully.")


except Exception as e:
    logger.error("Glue job failed!", exc_info=True)
    raise e