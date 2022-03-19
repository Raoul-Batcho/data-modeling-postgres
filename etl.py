
"""
This file develops the ETL processes for each of the tables and load the whole datasets.
"""
import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

"""Process song_data:

In this first part, we perform ETL on the first dataset, song_data, to create the songs and artists dimensional tables.

Let's perform ETL on a single song file and load a single record into each table to start.

- We use the process_song_file() function provided below to get a list of all song JSON files in data/song_data
- Select the first song in this list
- Read the song file and view the data 
- Insert artist record into artist table
- Insert song record into song table
"""

def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True )
    df.head()

    # insert artist record
    artist_data = df[["artist_id","artist_name","artist_location","artist_latitude","artist_longitude"]].values.tolist()
    artist_data = tuple(artist_data[0])
    cur.execute(artist_table_insert, artist_data)

    # insert song record
    song_data = df[["song_id","title","artist_id","year","duration"]].values.tolist()
    song_data = tuple(song_data[0])
    cur.execute(song_table_insert, song_data)
    
"""Process log_data:

In this second part, we perform ETL on the second dataset, log_data, to create the time and users dimensional tables, as well as the songplays fact table.

We perform ETL on a single log file and load a single record into each table.

- We use the process_log_file() function to get a list of all song JSON files in data/log_data
- Select the first log file in this list
- Read the log file and view the data
- Insert time record into time table
- Insert user record into users table
- Insert songplay record into songplay table

"""  

def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)
    df.head()

    # filter by NextSong action
    df = df[df["page"]=="NextSong"]
    df.head()

    # convert timestamp column to datetime
    t = pd.to_datetime(df["ts"],unit='ms')
    t.head()
    
    # insert time data records
    ts=df["ts"]
    hour = t.dt.hour
    day = t.dt.day
    month = t.dt.month
    year = t.dt.year
    week_number = t.dt.weekofyear
    weekday = t.dt.dayofweek

    time_data = (ts, hour, day, month, year, week_number, weekday)
    column_labels = ('start_time', 'hour', 'day', 'month', 'year', 'week', 'weekday')
    time_df = pd.DataFrame(dict(zip(column_labels,time_data)))
    time_df.head()

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)
        
"""Process data:

In this third part, 
The process_data() function takes in argument the filepath and either the process_song_file or process_log_file function, 
and according to the arguments provided, ETL is performed on the whole dataset.

- Select all the files in the whole dataset provided
- lists all the files 
- iterate through the list
- Insert record into database table according to the dataset provided.

"""  


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))

#The main() function runs the process_data() function for both datasets and close the connection.

def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()