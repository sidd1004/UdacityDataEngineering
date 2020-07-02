import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
IAM_ROLE = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
SONG_DATA = config['S3']['SONG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
        CREATE TABLE staging_events (
            artist          TEXT NULL,
            auth            TEXT, 
            first_name      TEXT,
            gender          TEXT,   
            item_in_session  INT,
            last_name       TEXT,
            length          DECIMAL(10) NULL,
            level           TEXT, 
            location        TEXT,
            method          TEXT,
            page            TEXT,
            registration    TEXT,
            session_id      INT NOT NULL SORTKEY DISTKEY,
            song            TEXT NULL,
            status          INT,
            ts              BIGINT,
            user_agent      TEXT,
            user_id         INT);
            """)

staging_songs_table_create = ("""
        CREATE TABLE staging_songs (
        num_songs INT,
        artist_id TEXT,
        artist_latitude DECIMAL(9) ,
        artist_longitude DECIMAL(9),
        artist_location TEXT,
        artist_name TEXT,
        song_id TEXT,
        title TEXT,
        duration DECIMAL(9),
        year INT);
        """)

songplay_table_create = ("""
        CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INT IDENTITY(0,1) NOT NULL PRIMARY KEY,
        start_time  TIMESTAMP REFERENCES time(start_time) SORTKEY,
        user_id     INT NOT NULL REFERENCES users (user_id) DISTKEY,
        level       TEXT NOT NULL,
        song_id     TEXT REFERENCES songs(song_id),
        artist_id   TEXT REFERENCES artists(artist_id),
        session_id  TEXT,
        location    TEXT,
        user_agent  TEXT);
        """)

user_table_create = ("""
        CREATE TABLE IF NOT EXISTS users (
        user_id       INT  PRIMARY KEY SORTKEY,
        first_name    TEXT,
        last_name     TEXT,
        gender        TEXT,
        level         TEXT)
        diststyle ALL;
        """)

song_table_create = ("""
        CREATE TABLE IF NOT EXISTS songs (
        song_id   TEXT PRIMARY KEY SORTKEY,
        title     TEXT NOT NULL,
        artist_id TEXT NOT NULL,
        year       INT,
        duration   DECIMAL(10));
        """)

artist_table_create = ("""
        CREATE TABLE IF NOT EXISTS artists (
        artist_id   TEXT PRIMARY KEY NOT NULL SORTKEY,
        name        TEXT,
        location    TEXT,
        latitude    DECIMAL(9),
        longitude   DECIMAL(9)) 
        diststyle all;
        """)

time_table_create = ("""
        CREATE TABLE IF NOT EXISTS time (
        start_time    TIMESTAMP PRIMARY KEY SORTKEY,
        hour          INT,
        day           INT,
        week          INT,
        month         INT,
        year          INT,
        weekday       INT);
        """)

# STAGING TABLES

staging_events_copy = ("""
        COPY staging_events 
        FROM {}
        iam_role {}
        timeformat 'auto'
        json {};
        """).format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""
        copy staging_songs 
        from {} 
        iam_role {}
        timeformat 'auto'
        json 'auto';
        """).format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
        INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time, se.user_id, se.level, ss.song_id,
                         ss.artist_id, se.session_id, se.location, 
                         se.user_agent
                         FROM staging_events se, staging_songs ss
                         WHERE se.page = 'NextSong' AND
                         se.song = ss.title AND
                         se.artist = ss.artist_name AND
                         se.length = ss.duration
        """)

user_table_insert = ("""
        INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT distinct  user_id, first_name, last_name, gender, level
        FROM staging_events
        WHERE page = 'NextSong'
        """)

song_table_insert = ("""
        INSERT INTO songs (song_id, title, artist_id, year, duration)
        SELECT song_id, title, artist_id, year, duration
        FROM staging_songs
        WHERE song_id IS NOT NULL
        """)

artist_table_insert = ("""
        INSERT INTO artists (artist_id, name, location, latitude, longitude)
        SELECT distinct artist_id, artist_name, artist_location , artist_latitude, artist_longitude 
        FROM staging_songs
        WHERE artist_id IS NOT NULL
        """)

time_table_insert = ("""
        INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
        EXTRACT(hour FROM start_time) AS hour,
        EXTRACT(day FROM start_time) AS day,
        EXTRACT(week FROM start_time) AS week,
        EXTRACT(month FROM start_time) AS month,
        EXTRACT(year FROM start_time) AS year,
        EXTRACT(week FROM start_time) AS weekday
        FROM staging_events AS se
        WHERE se.page = 'NextSong';
        """)

# QUERY LISTS

select_table_queries = [error_select]
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
