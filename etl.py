import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def process_song_file(cur, filepath):
    df=pd.DataFrame([pd.read_json(filepath,typ='series',convert_dates=False)])

    for value in df.values:
        num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year = value

        artist_data =(artist_id,artist_name,artist_location,artist_latitude,artist_longitude)

        cur.execute(artist_table_insert, artist_data)

        song_data = (song_id, title, artist_id, year, duration)
        cur.execute(song_table_insert, song_data)

    print(f"Records inserted for file {filepath}")


def process_log_file(cur, filepath):
    df = pd.read_json(filepath, lines=True)

    df = df[df['page'] == "NextSong"].astype({'ts': 'datetime64[ms]'})

    t = pd.Series(df['ts'], index=df.index)

    column_labels = ["timestamp", "hour", "day", "weelofyear", "month", "year", "weekday"]

    time_data = []

    for data in t:
        time_data.append([data,data.hour, data.day, data.weekofyear, data.month, data.year, data.day_name()])

    time_df=pd.DataFrame(data=time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

