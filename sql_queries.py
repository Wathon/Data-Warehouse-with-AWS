import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABle IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE  IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events
    (
        artist VARCHAR,
        auth VARCHAR NOT NULL,
        firstName VARCHAR(50),
        gender CHAR,
        itemInSession INTEGER NOT NULL,
        lastName VARCHAR(50),
        length NUMERIC,
        level VARCHAR NOT NULL,
        location VARCHAR,
        method VARCHAR NOT NULL,
        page VARCHAR NOT NULL,
        registration NUMERIC,
        sessionId INTEGER NOT NULL,
        song VARCHAR,
        status INTEGER NOT NULL,
        ts BIGINT NOT NULL,
        userAgent VARCHAR,
        userId INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
    (
        num_songs INTEGER NOT NULL,
        artist_id VARCHAR NOT NULL,
        artist_latitude NUMERIC,
        artist_longitude NUMERIC,
        artist_location VARCHAR,
        artist_name VARCHAR NOT NULL,
        song_id VARCHAR NOT NULL,
        title VARCHAR NOT NULL,
        duration NUMERIC NOT NULL,
        year NUMERIC
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays
    (
        songplay_id INTEGER IDENTITY (1, 1) PRIMARY KEY ,
        start_time TIMESTAMP NOT NULL,
        user_id INTEGER NOT NULL,
        level VARCHAR NOT NULL,
        song_id VARCHAR,
        artist_id VARCHAR,
        session_id INTEGER NOT NULL,
        location VARCHAR,
        user_agent VARCHAR NOT NULL
    )
    DISTSTYLE KEY
    DISTKEY ( start_time )
    SORTKEY ( start_time );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
    (
        userId INTEGER PRIMARY KEY,
        firsname VARCHAR(50) NOT NULL,
        lastname VARCHAR(50) NOT NULL,
        gender CHAR(1) ENCODE BYTEDICT NOT NULL,
        level VARCHAR ENCODE BYTEDICT NOT NULL
    )
    SORTKEY (userId);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs
    (
        song_id VARCHAR PRIMARY KEY,
        title VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        year INTEGER ENCODE BYTEDICT NOT NULL,
        duration NUMERIC NOT NULL
    )
    SORTKEY (song_id);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists
    (
        artist_id VARCHAR PRIMARY KEY ,
        name VARCHAR NOT NULL,
        location VARCHAR,
        latitude NUMERIC,
        longitude NUMERIC
    )
    SORTKEY (artist_id);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
    (
        start_time  TIMESTAMP PRIMARY KEY ,
        hour INTEGER NOT NULL,
        day INTEGER NOT NULL,
        week INTEGER NOT NULL,
        month INTEGER NOT NULL,
        year INTEGER ENCODE BYTEDICT NOT NULL,
        weekday VARCHAR(9) ENCODE BYTEDICT NOT NULL
    )
    DISTSTYLE KEY
    DISTKEY ( start_time )
    SORTKEY (start_time);
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM {}
    iam_role {}
    FORMAT AS json {}
    REGION 'us-west-2';
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    iam_role {}
    FORMAT AS json 'auto'
    REGION 'us-west-2';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (START_TIME, USER_ID, LEVEL, SONG_ID, ARTIST_ID, SESSION_ID, LOCATION, USER_AGENT)
    SELECT DISTINCT
        TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' as start_time,
                    se.userId,
                    se.level,
                    ss.song_id,
                    ss.artist_id,
                    se.sessionId,
                    se.location,
                    se.userAgent
    FROM staging_songs ss
    INNER JOIN staging_events se
    ON (ss.title = se.song AND se.artist = ss.artist_name)
    AND se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users
    SELECT DISTINCT userId, firstName, lastName, gender, level
    FROM staging_events
    WHERE userId IS NOT NULL
    AND page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs
    SELECT
        DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists
    SELECT
        DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs;
""")

time_table_insert = ("""
    INSERT INTO time
    SELECT DISTINCT
        TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' as start_time,
        EXTRACT(HOUR FROM start_time) AS hour,
        EXTRACT(DAY FROM start_time) AS day,
        EXTRACT(WEEKS FROM start_time) AS week,
        EXTRACT(MONTH FROM start_time) AS month,
        EXTRACT(YEAR FROM start_time) AS year,
        to_char(start_time, 'Day') AS weekday
    FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
