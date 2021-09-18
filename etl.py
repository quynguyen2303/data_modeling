import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def insert_record(cur, insert_query, df, fields):
    """
    Insert a record into a DB table.
    :param cur: connection cursor to insert the data in DB.
    :param insert_query: query SQL for Insert.
    :param df: dataframe with the record.
    :param fields: array of fields of the data to insert.
    """



def insert_dataframe(cur, df, insert_query):
    """
    Insert a pandas dataframe into a DB table
    :param cur: connection cursor to insert the data in DB.
    :param df: dataframe with the record.
    :param insert_query: query SQL for Insert.
    """



def process_song_file(cur, filepath):
    """
    Process the songs files and insert data into dimension tables: songs and artists.
    :param cur: connection cursor to insert the data in DB.
    :param filepath: path/to/the/song/file.
    """

    # open song file

    # insert song record
    
    # insert artist record



def expand_time_data(df, ts_field):
    """
    Add more time elements to a dataframe from a UNIX timestamp in milliseconds.
    :param df: pandas dataframe to add time fields.
    :param ts_field: name of the timestamp field field.
    :return: pandas dataframe with more time fields.
    """



def get_songid_artistid(cur, song, artist, length):
    """
    Gets the song_id and the artist_id from song tittle, artist name and gon duration.
    :param cur: connection cursor to query the data in DB.
    :param song: song tittle
    :param artist: artist name
    :param length: song duration
    :return: returns song_id and artist_id
    """

    # get songid and artistid from song and artist tables



def insert_facts_songplays(cur, df):
    """
    Insert songplays fact table
    :param cur: connection cursor to insert the data in DB.
    :param df: dataframe with song plays data.
    """

    # insert songplay records



def process_log_file(cur, filepath):
    """
    Process the log files and insert data into dimension tables: time and users.
    Insert data into the facts table songplays.
    :param cur: connection cursor to insert the data in DB.
    :param filepath: path/to/the/log/file.
    """

    # open log file

    # filter by NextSong action

    # convert timestamp column to datetime

    # insert time data records


    # load user table

    # insert user records

    # insert songplay records


def get_all_files_matching_from_directory(directorypath, match):
    """
    Get all the files that match into a directory recursively.
    :param directorypath: path/to/directory.
    :param match: match expression.
    :return: array with all the files that match.
    """
    # get all files matching extension from directory



def process_data(cur, conn, filepath, func):
    """
    Process all the data, either songs files or logs files
    :param cur: connection cursor to insert the data in DB.
    :param conn: connection to the database to do the commit.
    :param filepath: path/to/data/type/directory
    :param func: function to process, transform and insert into DB the data.
    """


    # get total number of files found


    # iterate over files and process



def main():
    #conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=123456")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()