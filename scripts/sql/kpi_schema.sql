CREATE DATABASE music_stream_db;

CREATE SCHEMA IF NOT EXISTS presentation;

CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE presentation.hourly_stream_insights (
    listen_time_hour      TIMESTAMP     NOT NULL,    -- The hour (e.g. '2024-06-25 01:00:00')
    artists          TEXT          NOT NULL,    -- A top artist for that hour
    unique_listeners INT           NOT NULL,    -- Distinct users in that hour
    unique_tracks    INT           NOT NULL,    -- Distinct tracks played
    total_plays      INT           NOT NULL,    -- Total play events
    track_diversity_index  FLOAT         NOT NULL,    -- unique_tracks / total_plays
    PRIMARY KEY (listen_time_hour)
);


CREATE TABLE staging.hourly_stream_insights (
    listen_time_hour      TIMESTAMP     NOT NULL,    -- The hour (e.g. '2024-06-25 01:00:00')
    artists          TEXT          NOT NULL,    -- A top artist for that hour
    unique_listeners INT           NOT NULL,    -- Distinct users in that hour
    unique_tracks    INT           NOT NULL,    -- Distinct tracks played
    total_plays      INT           NOT NULL,    -- Total play events
    track_diversity_index  FLOAT         NOT NULL,    -- unique_tracks / total_plays
    PRIMARY KEY (listen_time_hour)
);


CREATE TABLE presentation.most_popular_track_per_genre (
    track_genre                 VARCHAR(50)         NOT NULL,
    listen_count                INT                 NOT NULL,
    avg_track_duration          INT                 NOT NULL,
    most_popular_track          TEXT                NOT NULL,
    popularity                  INT                 NOT NULL,
    
    PRIMARY KEY (track_genre)
);

CREATE TABLE staging.most_popular_track_per_genre (
    track_genre                 VARCHAR(50)         NOT NULL,
    listen_count                INT                 NOT NULL,
    avg_track_duration          INT                 NOT NULL,
    most_popular_track          TEXT                NOT NULL,
    popularity                  INT                 NOT NULL,
    
    PRIMARY KEY (track_genre)
);