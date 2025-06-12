-- Active: 1749556361603@@127.0.0.1@5436@music_db
CREATE SCHEMA IF NOT EXISTS presentation;
CREATE SCHEMA IF NOT EXISTS staging;


CREATE TABLE staging.transformed_data (
    user_id INT,
    track_id VARCHAR(100),
    listen_time TIMESTAMP,
    id INT,
    artists TEXT,
    album_name TEXT,
    track_name TEXT,
    popularity INT,
    duration_ms INT,
    instrumentalness DECIMAL(12,10),
    track_genre VARCHAR(50),
    user_name VARCHAR(100) NOT NULL
);

DROP TABLE IF EXISTS presentation.hourly_stream_insights;
CREATE TABLE presentation.hourly_stream_insights (
    listen_time      TIMESTAMP     NOT NULL,    -- The hour (e.g. '2024-06-25 01:00:00')
    artists          TEXT          NOT NULL,    -- A top artist for that hour
    unique_listeners INT           NOT NULL,    -- Distinct users in that hour
    unique_tracks    INT           NOT NULL,    -- Distinct tracks played
    total_plays      INT           NOT NULL,    -- Total play events
    track_diversity_index  FLOAT         NOT NULL,    -- unique_tracks / total_plays
    PRIMARY KEY (listen_time)
);

DROP TABLE IF EXISTS staging.hourly_stream_insights;
CREATE TABLE staging.hourly_stream_insights (
    listen_time      TIMESTAMP     NOT NULL,    -- The hour (e.g. '2024-06-25 01:00:00')
    artists          TEXT          NOT NULL,    -- A top artist for that hour
    unique_listeners INT           NOT NULL,    -- Distinct users in that hour
    unique_tracks    INT           NOT NULL,    -- Distinct tracks played
    total_plays      INT           NOT NULL,    -- Total play events
    track_diversity_index  FLOAT         NOT NULL,    -- unique_tracks / total_plays
    PRIMARY KEY (listen_time)
);



