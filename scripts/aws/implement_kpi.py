import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

import boto3

from pyspark.sql import SparkSession
import pyspark.sql.functions as F 
from pyspark.sql.types import *


# Define expected parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

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
BEGIN;

DELETE FROM presentation.hourly_stream_insights target
USING staging.hourly_stream_insights staging
WHERE target.listen_time = staging.listen_time;

INSERT INTO presentation.hourly_stream_insights
(listen_time, artists, unique_listeners, unique_tracks, total_plays, track_diversity)
SELECT listen_time, artists, unique_listeners, unique_tracks, total_plays, track_diversity
FROM staging.hourly_stream_insights;

TRUNCATE TABLE staging.hourly_stream_insights;

COMMIT;
"""


def convert_to_dynamicFrame(df, cntx, name):
    return DynamicFrame.fromDF(
        dataframe = df,
        glue_ctx = cntx,
        name = name
    )
    
# Write to S3
def write_to_s3(dyf, cntx, s3_path, name):
    glueContext.write_dynamic_frame.from_options(
    dyf,
    connection_type='s3',
    connection_options={'path': s3_path},
    format='csv',
    transformation_ctx=name
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
    
def unique_listners_per_hour(spark):
    return spark.sql("""
            WITH hour_stream AS (
                SELECT
                    user_id,
                    track_id,
                    artists,
                    listen_time
                FROM streamed_music_tb
            ),
            listener_counts AS (
                SELECT 
                    COUNT(DISTINCT user_id) AS unique_listeners,
                    listen_time
                FROM hour_stream
                GROUP BY listen_time
              ),
            artists_rankings AS (
                SELECT 
                    track_id,
                    listen_time,
                    ROW_NUMBER() OVER (PARTITION BY date_trunc('hour', listen_time) ORDER BY COUNT(*) DESC) AS artist_rank
                FROM hour_stream
                GROUP BY listen_time, track_id
              ),
              track_artists AS (
                SELECT 
                    DISTINCT track_id, listen_time, artists
                FROM hour_stream
            ),
            track_diversity AS (
                SELECT
                    listen_time,
                    COUNT(DISTINCT track_id) AS unique_tracks,
                    COUNT(track_id) AS total_plays
                FROM hour_stream
                GROUP BY listen_time
              )
            SELECT
                lc.listen_time,
                ta.artists,
                lc.unique_listeners,
                td.unique_tracks, 
                td.total_plays,
                ROUND(td.unique_tracks * 1.0 / td.total_plays, 5) AS track_diversity_index
              FROM listener_counts lc
              JOIN artists_rankings ar ON lc.listen_time = ar.listen_time
              JOIN track_artists ta ON ar.track_id = ta.track_id AND ar.listen_time = ta.listen_time
              JOIN track_diversity td ON lc.listen_time = td.listen_time
              WHERE ar.artist_rank=1
              ORDER BY lc.listen_time;
            """)
    
# Load data
transformed_df = spark.read \
        .format("parquet") \
        .schema(transformed_schema) \
        .load("s3a://transformed-music-gke.amalitech/staging/")
        
transformed_df.show()
# transformed_df.createOrReplaceTempView("streamed_music_tb")

# most_popular_per_genre_df = most_popular_track_per_genre(spark)

# unique_listners_df = unique_listners_per_hour(spark)


# most_popular_per_genre_dyf = convert_to_dynamicFrame(most_popular_per_genre_df, glueContext, "most_popular_per_genre_dyf" )
# unique_listners_dyf = convert_to_dynamicFrame(most_popular_per_genre_df, glueContext, "unique_listners_dyf")

# # glueContext.write_dynamic_frame.from_jdbc_conf(
# #     frame=unique_listners_dyf,
# #     catalog_connection=args['Redshift connection'],
# #     connection_options={
# #         "dbtable": "staging.hourly_stream_insights",
# #         "database": "music_stream_db"
# #     },
# #     redshift_tmp_dir="s3://awsservice-temp/redshift-s3/"
# # )

# # response = boto3.client.execute_statement(
# #     ClusterIdentifier='redshift-cluster-mucis-stream',
# #     Database='music_stream_db',
# #     DbUser='awsuser',
# #     Sql=HOURLY_INSIGHT_UPSERT,
# #     SecretArn='arn:aws:secretsmanager:eu-north-1:309797288544:secret:glue!309797288544-Redshiftconnection-1749830096604-SbsCzs'
# # )



# write_to_s3(most_popular_per_genre_dyf, 
#     glueContext, 
#     "s3://transformed-music-gke.amalitech/presentation/popular_track_per_genre/", 
#     "write_to_s3_1")

# write_to_s3(unique_listners_dyf, 
#     glueContext, 
#     "s3://transformed-music-gke.amalitech/presentation/unique_listeners_per_hour/", 
#     "write_to_s3_2")

# Finish Glue job
job.commit()
