import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import pyspark.sql.functions as F
from pyspark.sql.types import *
import logging


# Define expected parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])


logger = logging.getLogger()
logger.setLevel(logging.INFO)
# Glue boilerplate
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define schema
transformed_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("track_id", StringType(), True),
    StructField("listen_time", TimestampType(), True),
    StructField("id", IntegerType(), True),
    StructField("artists", StringType(), True),
    StructField("album_name", StringType(), True),
    StructField("track_name", StringType(), True),
    StructField("popularity", LongType(), True),
    StructField("duration_ms", LongType(), True),
    StructField("instrumentalness", FloatType(), True),
    StructField("track_genre", StringType(), True),
    StructField("user_name", StringType(), True),
])

HOURLY_INSIGHT_UPSERT = """
DELETE FROM presentation.hourly_stream_insights 
USING staging.hourly_stream_insights
WHERE presentation.hourly_stream_insights.listen_time_hour = staging.hourly_stream_insights.listen_time_hour;
INSERT INTO presentation.hourly_stream_insights (
    listen_time_hour,
    artists,
    unique_listeners,
    unique_tracks,
    total_plays,
    track_diversity_index
)
SELECT
    listen_time_hour,
    artists,
    unique_listeners,
    unique_tracks,
    total_plays,
    track_diversity_index
FROM staging.hourly_stream_insights;
"""

GENRE_INSIGHT_UPSERT = """
DELETE FROM presentation.most_popular_track_per_genre 
USING staging.most_popular_track_per_genre
WHERE presentation.most_popular_track_per_genre.track_genre = staging.most_popular_track_per_genre.track_genre;
INSERT INTO presentation.most_popular_track_per_genre (
    track_genre,
    listen_count,
    avg_track_duration,
    most_popular_track,
    popularity
)
SELECT
    track_genre,
    listen_count,
    avg_track_duration,
    most_popular_track,
    popularity
FROM staging.most_popular_track_per_genre;
"""

def validate_columns(df, required_columns):
    return set(required_columns).issubset(set(df.columns))

def validate_not_null(df, not_null_columns):
    for col in not_null_columns:
        null_count = df.filter(df[col].isNull()).count()
        if null_count > 0:
            logger.warning(f"Column '{col}' has {null_count} null value(s).")
            return False
    return True


def convert_to_dynamicFrame(df, cntx, name):
    return DynamicFrame.fromDF(
        dataframe = df,
        glue_ctx = cntx,
        name = name
    )
    


# Run transformation
def most_popular_track_per_genre(spark):
    return spark.sql("""
        WITH ranked_tracks AS (
            SELECT
                track_genre,
                track_name,
                popularity,
                COUNT(*) OVER (PARTITION BY track_genre) AS listen_count,
                AVG(duration_ms) OVER (PARTITION BY track_genre) AS avg_track_duration,
                ROW_NUMBER() OVER (PARTITION BY track_genre ORDER BY popularity DESC) as most_popular_rank
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
    
def unique_listners_per_hour(spark):
    return spark.sql("""
            WITH hour_stream AS (
                SELECT
                    user_id,
                    track_id,
                    artists,
                    date_trunc('hour', listen_time) AS listen_time_hour
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
                    ROW_NUMBER() OVER (PARTITION BY date_trunc('hour', listen_time_hour) ORDER BY COUNT(*) DESC) AS artist_rank
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
    
# Load data
# transformed_df = spark.read \
#         .format("parquet") \
#         .schema(transformed_schema) \
#         .load("s3a://transformed-music-gke.amalitech/staging/")

try:
    logger.info("Implement KPI Job Started...")
    transformed_df = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://transformed-music-gke.amalitech/staging/"], "recurse": True}, transformation_ctx="AmazonS3_node1750038757739").toDF()
    # transformed_df.show()
    logger.info("Validating Data... ")
    required_columns = ["track_id", "listen_time", "track_genre"]
    if not (validate_columns(transformed_df, required_columns) and validate_not_null(transformed_df, ["listen_time", "track_genre"])):
        raise("Data was is Valid")

    # #
    transformed_df.createOrReplaceTempView("streamed_music_tb")

except Exception as e:
    logger.error("Failed to load data from S3.", exc_info=True)
    raise e




try:
    logger.info("Running transformation: most_popular_track_per_genre...")
    most_popular_per_genre_df = most_popular_track_per_genre(spark)
    

    logger.info("Running transformation: unique_listners_per_hour...")
    unique_listners_df = unique_listners_per_hour(spark)
    

    unique_listners_df = unique_listners_df.select(
        F.col("listen_time_hour").cast("timestamp"),
        F.col("artists").cast("string"),
        F.col("unique_listeners").cast("int"),
        F.col("unique_tracks").cast("int"),
        F.col("total_plays").cast("int"),
        F.col("track_diversity_index").cast("double")
    )

    unique_listners_df = unique_listners_df.fillna({
        "listen_time_hour": "1970-01-01 00:00:00",  # Placeholder date
        "artists": "Unknown",
        "unique_listeners": 0,
        "unique_tracks": 0,
        "total_plays": 0,
        "track_diversity_index": 0.0
    })


    most_popular_per_genre_df = most_popular_per_genre_df.select(
        F.col("track_genre").cast("string"),
        F.col("listen_count").cast("int"),
        F.col("avg_track_duration").cast("double"),
        F.col("most_popular_track").cast("string"),
        F.col("popularity").cast("int")
    )

    most_popular_per_genre_df = most_popular_per_genre_df.fillna({
        "track_genre": "Unknown",
        "listen_count": 0,
        "avg_track_duration": 0.0,
        "most_popular_track": "N/A",
        "popularity": 0
    })
    most_popular_per_genre_dyf = convert_to_dynamicFrame(most_popular_per_genre_df, glueContext, "most_popular_per_genre_dyf" )
    unique_listeners_dyf = convert_to_dynamicFrame(unique_listners_df, glueContext, "unique_listners_dyf")
    logger.info("TransformationS successful.")
except Exception as e:
    logger.error("SQL transformation failed.", exc_info=True)
    raise e




try:
    # Write Hourly Stream Insight to Redshift
    logger.info("Writing unique listener metrics to Redshift...")
    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=unique_listeners_dyf,  
        catalog_connection="Redshift connection", 
        connection_options={
            "dbtable": "staging.hourly_stream_insights", 
            "database": "dev",
            "preactions": """
                CREATE TABLE IF NOT EXISTS staging.hourly_stream_insights (
                    listen_time_hour TIMESTAMP,
                    artists TEXT,
                    unique_listeners INT,
                    unique_tracks INT,
                    total_plays INT,
                    track_diversity_index DOUBLE PRECISION
                );
                TRUNCATE staging.hourly_stream_insights;
            """,
            "postactions": HOURLY_INSIGHT_UPSERT
        },
        redshift_tmp_dir="s3://aws-glue-assets-309797288544-eu-north-1/temporary/"
    )
    logger.info("Write to Redshift: hourly_stream_insights completed.")

    # Write Most Popular Track per Genre to Redshift
    logger.info("Writing most popular track per genre metrics to Redshift...")
    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=most_popular_per_genre_dyf, 
        catalog_connection="Redshift connection",
        connection_options={
            "postactions": GENRE_INSIGHT_UPSERT,
            "dbtable": "staging.most_popular_track_per_genre", 
            "database":"dev",
            "preactions": """
                CREATE TABLE IF NOT EXISTS staging.most_popular_track_per_genre (
                    track_genre VARCHAR(50),
                    listen_count INT,
                    avg_track_duration DOUBLE PRECISION,
                    most_popular_track TEXT,
                    popularity INT
                );
                TRUNCATE staging.most_popular_track_per_genre;
            """
        },
        redshift_tmp_dir= "s3://aws-glue-assets-309797288544-eu-north-1/temporary/"
    )
    logger.info("Write to Redshift: hourly_stream_insights completed.")
except Exception as e:
    raise e

try:
    job.commit()
    logger.info("Glue job committed successfully.")
except Exception as e:
    logger.error("Job commit failed.", exc_info=True)
    raise e