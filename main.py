import sqlite3
import time


def define_scheme():
    unique_tracks_table = 'unique_tracks'
    triplets_sample_table = 'triplets_sample_20p'

    connection = sqlite3.connect("rank.DB")
    create_table_unique = '''
    CREATE TABLE {}(
        id_wyk VARCHAR (18) PRIMARY KEY,
        id_song VARCHAR (18),
        artist_name TEXT,
        song_title TEXT
        ) '''.format(unique_tracks_table)

    create_table_triplets = '''
    CREATE TABLE {}(
        id_song VARCHAR (18),
        id_user VARCHAR (40),
        date TEXT,
        foreign key (id_song) references unique_tracks (id_song)
    )'''.format(triplets_sample_table)

    db_cursor = connection.cursor()

    db_cursor.execute('PRAGMA table_info({})'.format(unique_tracks_table))
    rows = db_cursor.fetchall()
    if not any(rows):
        db_cursor.execute(create_table_unique)

    db_cursor.execute('PRAGMA table_info({})'.format(triplets_sample_table))
    rows = db_cursor.fetchall()
    if not any(rows):
        db_cursor.execute(create_table_triplets)

    db_cursor.close()

    return connection


def fill_database_rank(conn):
    unique_tracks = open("data/unique_tracks.txt", "r", encoding="ISO-8859-1")
    triplets_sample = open("data/triplets_sample_20p.txt", "r", encoding="ISO-8859-1")

    while True:
        line = unique_tracks.readline()

        if not line:
            break

        line = line.replace('\n', '').replace('"', '')
        unique_track = line.split("<SEP>")
        conn.cursor().execute(
             'insert into unique_tracks (id_wyk, id_song, artist_name, song_title) values("{}","{}","{}",'
             '"{}");'.format(
                 unique_track[0], unique_track[1],
                 unique_track[2], unique_track[3]))
    conn.commit()
    while True:
        line_2 = triplets_sample.readline()

        if not line_2:
            break

        line_2 = line_2.replace('\n', '').replace('"', '')
        triplet_sample = line_2.split("<SEP>")
        conn.cursor().execute(
            'insert into triplets_sample_20p (id_user, id_song, date) values("{}","{}","{}");'.format(
                triplet_sample[0], triplet_sample[1],
                triplet_sample[2]))

    conn.commit()
    unique_tracks.close()
    triplets_sample.close()


def get_top_five_songs(conn):

    db_cursor = conn.cursor()

    start_time = time.time()

    db_cursor.execute('''SELECT ut.song_title, COUNT(tsp.[date]) as 'count' FROM triplets_sample_20p tsp 
        JOIN unique_tracks ut ON tsp.id_song = ut.id_song 
        GROUP BY tsp.id_song 
        ORDER BY 'count' DESC 
        LIMIT 5''')

    end_time = time.time()
    total_time = end_time - start_time
    print('Total execution time: {}'.format(total_time))

    result = db_cursor.fetchall()

    db_cursor.close()
    print(result)


def get_top_artist(conn):
    db_cursor = conn.cursor()

    start_time = time.time()

    db_cursor.execute('''SELECT ut.id_wyk, ut.artist_name, COUNT(tsp.[date]) AS 'count' FROM triplets_sample_20p tsp
        JOIN unique_tracks ut ON tsp.id_song = ut.id_song
        GROUP BY ut.id_wyk
        ORDER BY 'count' DESC
        LIMIT 1
        ''')

    end_time = time.time()
    total_time = end_time - start_time
    print('Total execution time: {}'.format(total_time))

    result = db_cursor.fetchall()
    db_cursor.close()
    print(result)
    conn.close()


if __name__ == '__main__':
    conn = define_scheme()
    fill_database_rank(conn)
    get_top_five_songs(conn)
    get_top_artist(conn)
