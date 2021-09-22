# Project data modeling with PostgreeSQL

This project is to practice data modeling from json logs on user activity and json metadata on songs.

### Project Description
In this project, we will create database and data modeling with Postgres and build an ETL pipeline using Python. To complete the project, we will need to define fact and dimension tables for a star schema for a 
particular analytic focus, and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.

### Data Model
The data model we implemented is a star model. It is the typical schema for a Data Warehouse. The tables are:

#### Fact Table

**songplays**

| COLUMN  	| TYPE  	| CONSTRAINT  	|
|---	|---	|---	|	
|   songplay_id	| SERIAL  	|   PRIMARY KEY	| 
|   start_time	|   bigint	|   NOT NULL	| 
|   user_id	|   int	|   NOT NULL	| 
|   level	|   varchar |   	| 
|   song_id	|   varchar	|   	| 
|   artist_id	|   varchar	|   	| 
|   session_id	|   int	|   	| 
|   location	|   text	|   	| 
|   user_agent	|   text	|   	| 

The songplay_id field is the primary key and it is an auto-incremental value with SERIAL data type.

The query to insert data on this table is:

``INSERT INTO songplays (start_time, user_id, level,song_id, artist_id, session_id, location, user_agent) \
 VALUES ( %s, %s, %s, %s, %s, %s, %s, %s)``
 
 #### Dimensions Tables
 We create one table for each dimension of the **Fact Table**
 
 **users**
 
 | COLUMN  	| TYPE  	| CONSTRAINT  	|
|---	|---	|---	|	
|   user_id	| int  	|   PRIMARY KEY	| 
|   first_name	|   varchar	|  	| 
|   last_name	|   varchar	|  	| 
|   gender	|   varchar(1) |   	| 
|   level	|   varchar	|   	| 

 
 The query to insert data on this table is:
 
 ``INSERT INTO users (user_id, first_name, last_name, gender, level)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (user_id)
        DO NOTHING``

An alternative is change the target of *ON CONFLICT*. I've supposed the info about users don't change. But it 
could be probably a better way DO UPDATE action in order to get the latest info about users with a reduction of 
performance for the UPDATE. DO NOTHING is faster. 

**songs**

 | COLUMN  	| TYPE  	| CONSTRAINT   	|
|---	|---	|---	|	
|   song_id	| varchar  	|   PRIMARY KEY	| 
|   title	|   text	|  	| 
|   artist_id	|   varchar	|   	| 
|   year	|   int |   	| 
|   duration	|   numeric	|   	| 

 The query to insert data on this table is:
 
``INSERT INTO songs (song_id, title, artist_id, year, duration)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (song_id)
        DO NOTHING ``

**artists**

 | COLUMN  	| TYPE  	| CONSTRAINT   	|
|---	|---	|---	|	
|   artist_id	| varchar  	|   PRIMARY KEY	| 
|   name	|   varchar	|   	| 
|   location	|   text	|   	| 
|   latitude	|   decimal	|   	| 
|   longitude	|   decimal |   	| 


 The query to insert data on this table is:
 
``INSERT INTO artists (artist_id, name, location, latitude, longitude)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id)
        DO NOTHING``

**time**
 
 | COLUMN  	| TYPE  	| CONSTRAINT   	|
|---	|---	|---	|	
|   start_time	| bigint  	|   PRIMARY KEY	| 
|   hour	|   int	|   	| 
|   day	|   int	|   	| 
|   week	|   int	|   	| 
|   month	|   int	|   	| 
|   year	|   int	|   	| 
|   weekday	|   varchar	|   	| 

 The query to insert data on this table is:
 
``INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (start_time)
        DO NOTHING``

### Files in Python
#### ETL Pipeline

The ETL is in the file **etl.py** and is divided in the next sections:

1. Connect to the database.
2. Process **song files**.
    1. Insert song data into **songs** table. 
    2. Insert artist data into **artists** table. 
3. Process **log_files**.
    1. Insert ts (unix timestamp) in **time** table.
        1. from the field **ts** we can extract year, day, hour, week, month and day of the week.
    2. Insert user info in **users** table.
    3. Insert songpplay records into **songplays** table. In this case we need an additional select to get the 
    artist_id and the artist_id. This is very important for the star schema will successful. We improved this 
    query using an additional INDEX in song table for the artist_id field to make the JOIN with artists table.
4. Disconnect and finish.
    
#### sql_queries.py

This file contains all the queries to the database. 
 
 In this file are:
 1. All the CREATE sentences for all the tables.
 2. All the INSERT sentences for all the tables.
 3. The select to get artist_id and song_id in order to fill the songplays table.


### Example Queries 

Here there are some examples of querying this data model

- Get the users that have most activity in songlist:

``select U.first_name, U.last_name, count(1) as num_plays from songplays S
join users U on U.user_id = S.user_id
group by U.first_name, U.last_name
order by num_plays desc``

- Get the list of songs and artists most listened:

``select U.title as song, A.name as artist, count(1) as num_plays from songplays S
join songs U on U.song_id = S.song_id
join artists A on A.artist_id = S.artist_id
group by U.title , A.name 
order by num_plays desc``

### Local Execution

For local execution you can use PostgreeSQL docker:

- Download docker image:

`` docker pull postgres ``

- Execute docker:

``docker run --name postgres --rm  -p 5432:5432 -e -d postgres``

- Connect string:

``"host=127.0.0.1 dbname=postgres user=postgres password=postgres port=5432" ``


