# DROP TABLES


# CREATE TABLES

# song_id and artist_id haven't constraints NOT NULL because the sample data to test ETL doesn't have all the
# combinations and most are null in both cases.
songplay_table_create = 

user_table_create = 

song_table_create = 

artist_table_create = 

time_table_create = 

# INSERT RECORDS

songplay_table_insert = 

user_table_insert = 

song_table_insert = 

artist_table_insert = 


time_table_insert = 

# FIND SONGS

song_select = 

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create,
                        artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop,
                      time_table_drop]