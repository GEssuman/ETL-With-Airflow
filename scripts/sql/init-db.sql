-- Active: 1749556361603@@127.0.0.1@5436@music_db@public
-- Create table for music tracks with audio features
CREATE TABLE songs (
    id INTEGER PRIMARY KEY,
    track_id VARCHAR(50) NOT NULL UNIQUE,
    artists TEXT,
    album_name TEXT,
    track_name TEXT,
    popularity INTEGER ,
    duration_ms INTEGER,
    explicit BOOLEAN,
    danceability DECIMAL(5,4),
    energy DECIMAL(5,4),
    key INTEGER,
    loudness DECIMAL(8,3),
    mode INTEGER,
    speechiness DECIMAL(8,6),
    acousticness DECIMAL(8,6),
    instrumentalness DECIMAL(12,10),
    liveness DECIMAL(5,4),
    valence DECIMAL(5,4),
    tempo DECIMAL(8,3),
    time_signature INTEGER,
    track_genre VARCHAR(50)
);


COPY songs FROM '/opt/sql-data/songs/songs.csv' WITH DELIMITER ',' CSV HEADER;


-- Create users table
CREATE TABLE users (
    user_id INTEGER PRIMARY KEY,
    user_name VARCHAR(100) NOT NULL,
    user_age INTEGER,
    user_country VARCHAR(100),
    created_at DATE
);


COPY users FROM '/opt/sql-data/users/users.csv' WITH DELIMITER ',' CSV HEADER;