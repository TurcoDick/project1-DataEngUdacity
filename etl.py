import os
import glob
import psycopg2
import pandas as pd
import sql_queries as que
import create_tables as create

def dropCreateDataBaseTables(cur):
    ''' 
    Delete and recreate all tables.
    
    Parameters: 
    cursor database
    
    '''
    try: 
        cur.execute(que.song_table_drop)
        print("Sucess: Dropping song table")
    except psycopg2.Error as e: 
        print("Error: Dropping table song")
        print (e)

    try: 
        cur.execute(que.artist_table_drop)
        print("Sucess: Dropping artist table")
    except psycopg2.Error as e: 
        print("Error: Dropping table artist")
        print (e) 

    try: 
        cur.execute(que.songplay_table_drop)
        print("Sucess: Dropping songplay table")
    except psycopg2.Error as e: 
        print("Error: Dropping table songplay")
        print (e) 

    try: 
        cur.execute(que.user_table_drop)
        print("Sucess: Dropping user table")
    except psycopg2.Error as e: 
        print("Error: Dropping table")
        print (e) 

    try: 
        cur.execute(que.time_table_drop)
        print("Sucess: Dropping time table")
    except psycopg2.Error as e: 
        print("Error: Dropping table time")
        print (e)     

    try:
        cur.execute(que.song_table_create)
        print("Sucess: Creating song table")
    except psycopg2.Error as e: 
        print("Error: Creating song table")
        print (e)

    try:
        cur.execute(que.artist_table_create)
        print("Sucess: Creating artists table")
    except psycopg2.Error as e: 
        print("Error: Creating artists table")
        print (e) 

    try:
        cur.execute(que.time_table_create)
        print("Sucess: Creating time table")
    except psycopg2.Error as e: 
        print("Error: Creating time table")
        print (e)    

    try:
        cur.execute(que.user_table_create)
        print("Sucess: Creating user table")
    except psycopg2.Error as e: 
        print("Error: Creating user table")
        print (e)

    try:
        cur.execute(que.songplay_table_create)
        print("Sucess: Creating songplay table")
    except psycopg2.Error as e: 
        print("Error: Creating songplay table")
        print (e)   
        
def get_files(filepath):
    '''
    Get all files in the folder.
    
    Parameters:
    filepath
    
    Return: 
    A list of files 
    
    '''
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
            
    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))    
    return all_files  

def getDataframeComplet(filepath):
    '''
    Converts a list of files to a dataframe.
    
    Parameters:
    filepath
    
    Return:
    a dataframe containing the files.
    '''
    all_files = get_files(filepath=filepath)
    li = []

    for filename in all_files:
        df = pd.read_json(filename, lines=True)
        li.append(df)
    return pd.concat(li, axis=0, ignore_index=True)

# inserir cada linha do dataframe na tabela

def loopInsert(cur, conn, dfInsert, stringInsert):
    '''
    A for loop to save small amounts of data to the database.
    
    Parameters:
    cursor of database
    conection of database
    dataframe with data.
    A string for insert
    '''
    num_files = len(dfInsert.index)
    for index, row in dfInsert.iterrows():
        cur.execute(stringInsert, row)
        print('{}/{} files processed.'.format(index, num_files))
        conn.commit()

def process_song_file(cur, conn, filepath):
    # open song file
    dfSongComplet = getDataframeComplet(filepath)
    dfSongComplet.rename(columns={'title': 'song'}, inplace=True)

    # insert song record
    dfSongClear = dfSongComplet[['song_id', 'song', 'artist_id', 'year', 'duration']]
    loopInsert(cur, conn, dfSongClear, que.song_table_insert)
    
    # insert artist record
    dfArtistClear = dfSongComplet[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    loopInsert(cur, conn, dfArtistClear, que.artist_table_insert)


def process_log_file(cur, conn, filepath):
    # open log file
    dfLogComplet = getDataframeComplet(filepath)
    
    # change columns name
    dfLogComplet.rename(columns={'userId':'user_id'}, inplace=True)
    dfLogComplet.rename(columns={'firstName':'first_name'}, inplace=True)
    dfLogComplet.rename(columns={'lastName':'last_name'}, inplace=True)
    dfLogComplet.rename(columns={'length':'duration'}, inplace=True)
    dfLogComplet.rename(columns={'artist':'artist_name'}, inplace=True)

    # filter by NextSong action
    dfLogCompletClear = dfLogComplet[dfLogComplet['page']=='NextSong'].reset_index()

    # convert timestamp column to datetime
    dfLogCompletClear['date'] = pd.to_datetime(dfLogCompletClear['ts'], unit='ns')
    dfLogCompletClear['start_time'] = [d.time() for d in dfLogCompletClear['date']]
    dfLogCompletClear['hour'] = [d.time().hour for d in dfLogCompletClear['date']]
    dfLogCompletClear['day'] = [d.date().day for d in dfLogCompletClear['date']]
    dfLogCompletClear['week'] = [d.week for d in dfLogCompletClear['date']]
    dfLogCompletClear['month'] = [d.month for d in dfLogCompletClear['date']]
    dfLogCompletClear['year'] = [d.year for d in dfLogCompletClear['date']]
    dfLogCompletClear['weekday'] = [d.weekday() for d in dfLogCompletClear['date']]  

    # insert time data records
    dfTimeClear = dfLogCompletClear[['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']]
    loopInsert(cur, conn, dfTimeClear, que.time_table_insert)

    # load user table
    dfUserClear = dfLogCompletClear[['user_id', 'first_name', 'last_name', 'gender', 'level']] 

    # insert user records
    loopInsert(cur, conn, dfUserClear, que.user_table_insert)

    # insert songplay records
    for index, row in dfLogCompletClear.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(que.song_select, (row.song, row.artist_name, row.duration))
        conn.commit()
        results = cur.fetchone()
    
        if results:
            songid, artistid = results
            print(songid, artistid) 
        else:
            songid, artistid = None, None

    # insert songplay record
    songplay_data = (row.ts, row.user_id, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
    cur.execute(que.songplay_table_insert, songplay_data)
    conn.commit()


def process_data(cur, conn, filepathSong, filepathLog):
    process_log_file(cur, conn, filepathLog)
    process_song_file(cur, conn, filepathSong)
    



def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    dropCreateDataBaseTables(cur)
    
    process_data(cur, conn, filepathSong= 'data/song_data', filepathLog= 'data/log_data')
    
    conn.close()


if __name__ == "__main__":
    main()