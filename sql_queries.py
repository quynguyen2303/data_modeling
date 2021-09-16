# DROP TABLES
songplays_table_drop = "DROP TABLE IF EXISTS songplays"
users_table_drop = "DROP TABLE IF EXISTS users"
songs_table_drop = "DROP TABLE IF EXISTS songs"
artists_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
# song_id and artist_id haven't constraints NOT NULL because the sample data to test ETL doesn't have all the
# combinations and most are null in both cases.
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays 
    (songplay_id SERIAL PRIMARY KEY,
    start_time bigint NOT NULL,
    user_id int NOT NULL,
    level varchar,
    song_id varchar,
    artist_id varchar,
    session_id int,
    location text,
    user_agent text)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
    (user_id int PRIMARY KEY,
    first_name varchar,
    last_name varchar,
    gender varchar(1),
    level varchar)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
    (song_id varchar PRIMARY KEY,
    title text,
    artist_id varchar,
    year int,
    duration numeric)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
    (artist_id varchar PRIMARY KEY,
    name varchar,
    location text,
    latitude decimal,
    longitude decimal)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
    (start_time bigint PRIMARY KEY,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday varchar)
""")

# INSERT RECORDS

# songplay_table_insert = 

# user_table_insert = 

# song_table_insert = 

# artist_table_insert = 

# time_table_insert = 

# FIND SONGS

# song_select = 

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create,
                        artist_table_create, time_table_create]
drop_table_queries = [songplays_table_drop, users_table_drop, songs_table_drop, artists_table_drop, time_table_drop]