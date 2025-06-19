from pyspark.sql import SparkSession
import pyspark.sql.functions as F 
from pyspark.sql.types import *
import os
from datetime import datetime
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from helper_functions import load_from_s3, read_from_db, create_spark_session, load_data_to_db
import logging

#Load env vairables
RDS_POSTGRES_DB = os.getenv("RDS_POSTGRES_DB")
RDS_POSTGRES_USER = os.getenv("RDS_POSTGRES_USER")
RDS_POSTGRES_PASSWORD = os.getenv("RDS_POSTGRES_PASSWORD")
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")



streaming_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("track_id", StringType(), True),
    StructField("listen_time", TimestampType(), True)
])

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






def most_popular_track_per_gener(spark):
    try:
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
    except Exception as e:
        logging.error(f"Unable to Implement KPI\n{e}") 


def unique_listners_per_hour(spark):
    try:
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
    except Exception as e:
        logging.error(f"Unable to Implement KPI\n{e}") 
    

# def load_data_to_db(df, schema):
#     df.write \
#     .format("jdbc") \
#     .option("url", f"jdbc:postgresql://rds_postgres:5432/{RDS_POSTGRES_DB}") \
#     .option("dbtable", schema) \
#     .option("user", RDS_POSTGRES_USER) \
#     .option("password", RDS_POSTGRES_PASSWORD) \
#     .option("driver", "org.postgresql.Driver") \
#     .mode("append") \
#     .save()

    

HOURLY_INSIGHT_UPSERT = """
INSERT INTO presentation.hourly_stream_insights AS target
(listen_time, artists, unique_listeners, unique_tracks, total_plays, track_diversity)
SELECT listen_time, artist, unique_listeners, unique_tracks, total_plays, track_diversity
FROM staging.hourly_stream_insights
ON CONFLICT (listen_time)
DO UPDATE SET
    artists = EXCLUDED.artists
    unique_listeners = EXCLUDED.unique_listeners,
    unique_tracks = EXCLUDED.unique_tracks,
    total_plays = EXCLUDED.total_plays,
    track_diversity = EXCLUDED.track_diversity;
"""


GENRE_INSIGHT_UPSERT = """
INSERT INTO analytics.hourly_stream_insights AS target
(listen_time, artist, unique_listeners, unique_tracks, total_plays, track_diversity)
SELECT listen_time, artist, unique_listeners, unique_tracks, total_plays, track_diversity
FROM analytics.hourly_stream_insights_staging
ON CONFLICT (listen_time, artist)
DO UPDATE SET
    unique_listeners = EXCLUDED.unique_listeners,
    unique_tracks = EXCLUDED.unique_tracks,
    total_plays = EXCLUDED.total_plays,
    track_diversity = EXCLUDED.track_diversity;
"""


if __name__=="__main__":
    spark = create_spark_session()
    print(f"hello world {spark}")


    most_popular_track_properties = {
        "db_name":RDS_POSTGRES_DB,
        "user": RDS_POSTGRES_USER,
        "password": RDS_POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
        "table":"presentation.most_popular_track_per_genre"
    }

    unique_listners_properties = {
        "db_name":RDS_POSTGRES_DB,
        "user": RDS_POSTGRES_USER,
        "password": RDS_POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
        "table":"presentation.hourly_stream_insights"
    }

    transformed_properties = {
        "db_name":RDS_POSTGRES_DB,
        "user": RDS_POSTGRES_USER,
        "password": RDS_POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
        "table":"staging.transformed_data"
    }


    transformed_df = read_from_db(spark, transformed_properties)


    transformed_df = load_from_s3(spark, transformed_schema, f"s3a://music-staging-gke/transformed_streamed_music/", is_header=False)
    transformed_df.createOrReplaceTempView("streamed_music_tb")

    most_popular_track_df = most_popular_track_per_gener(spark)    
    
    unique_listners_df = unique_listners_per_hour(spark)

    # unique_listners_df.printSchema()
    # most_popular_track_df.printSchema()
unique_listners_df = unique_listners_df.select(
    F.col("listen_time_hour").cast("timestamp"),
    F.col("artists").cast("string"),
    F.col("unique_listeners").cast("int"),
    F.col("unique_tracks").cast("int"),
    F.col("total_plays").cast("int"),
    F.col("track_diversity_index").cast("double")
)

# Step 2: Handle any possible NULLs — fill with safe defaults
unique_listners_df = unique_listners_df.fillna({
    "listen_time_hour": "1970-01-01 00:00:00",  # Placeholder date
    "artists": "Unknown",
    "unique_listeners": 0,
    "unique_tracks": 0,
    "total_plays": 0,
    "track_diversity_index": 0.0
})


most_popular_per_genre_df = most_popular_track_df.select(
    F.col("track_genre").cast("string"),
    F.col("listen_count").cast("int"),
    F.col("avg_track_duration").cast("double"),
    F.col("most_popular_track").cast("string"),
    F.col("popularity").cast("int")
)

# Step 2: Fill NULLs to satisfy NOT NULL constraint
most_popular_per_genre_df = most_popular_per_genre_df.fillna({
    "track_genre": "Unknown",
    "listen_count": 0,
    "avg_track_duration": 0.0,
    "most_popular_track": "N/A",
    "popularity": 0
})

# Load to readshift/postgresql
# load_data_to_db(unique_listners_df, unique_listners_properties)
load_data_to_db(most_popular_per_genre_df, most_popular_track_properties)
# most_popular_per_genre_df.show()
# unique_listners_df.write \
# .format("jdbc") \
# .option("url", f"jdbc:postgresql://rds_postgres:5432/{unique_listners_properties["db_name"]}") \
# .option("dbtable", unique_listners_properties["table"]) \
# .option("user", unique_listners_properties["user"]) \
# .option("password", unique_listners_properties["password"]) \
# .option("driver", unique_listners_properties["driver"]) \
# .save()


# most_popular_per_genre_df.write \
# .format("jdbc") \
# .option("url", f"jdbc:postgresql://rds_postgres:5432/{most_popular_track_properties["db_name"]}") \
# .option("dbtable", most_popular_track_properties["table"]) \
# .option("user", most_popular_track_properties["user"]) \
# .option("password", most_popular_track_properties["password"]) \
# .option("driver", most_popular_track_properties["driver"]) \
# .save()




