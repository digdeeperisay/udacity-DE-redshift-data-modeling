import configparser

# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')

# READ IN THE VARIABLES FROM THE CONFIG FILE

LOG_DATA = config.get("S3","LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
SONG_DATA_SUBSET = config.get("S3", "SONG_DATA_SUBSET") #to test out the process with sample since the full files takes exceedingly long to run
IAM_ROLE = config.get("IAM_ROLE","ARN")

# DROP TABLES - these queries will be used in the 'create_tables.py' file

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES - these queries will be used in the 'create_tables.py' file
# Creating 2 staging tables, 1 fact table, and 4 dimension tables

# Staging table 1 - this table will be the dump of all records from the 'log data' files
staging_events_table_create= ("""
    CREATE TABLE staging_events 
    (
    artist          VARCHAR,
    auth            VARCHAR, 
    firstName       VARCHAR,
    gender          VARCHAR,   
    itemInSession   INTEGER,
    lastName        VARCHAR,
    length          FLOAT,
    level           VARCHAR, 
    location        VARCHAR,
    method          VARCHAR,
    page            VARCHAR,
    registration    BIGINT,
    sessionId       INTEGER,
    song            VARCHAR,
    status          INTEGER,
    ts              TIMESTAMP,
    userAgent       VARCHAR,
    userId          INTEGER
    );
""")

# Staging table 2- this table will be the dump of all records from the 'song data' files
staging_songs_table_create = ("""
    CREATE TABLE staging_songs
    (
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_location VARCHAR,
    artist_longitude FLOAT,
    artist_name VARCHAR,
    duration FLOAT,
    num_songs INTEGER,
    song_id VARCHAR,
    title VARCHAR, 
    year INTEGER
    );
""")

# Fact table 1 - holds details on the song plays
songplay_table_create = ("""
    CREATE TABLE songplays
    (
    songplay_id INTEGER IDENTITY(1,1) PRIMARY KEY sortkey,
    start_time TIMESTAMP NOT NULL, 
    user_id INTEGER NOT NULL, 
    level VARCHAR, 
    song_id VARCHAR, 
    artist_id VARCHAR distkey, 
    session_id VARCHAR, 
    location VARCHAR, 
    user_agent VARCHAR
    );
""")

# Dimension table 1 - User information
user_table_create = ("""
    CREATE TABLE users
    (
    user_id INTEGER PRIMARY KEY sortkey, 
    first_name VARCHAR, 
    last_name VARCHAR, 
    gender VARCHAR, 
    level VARCHAR
    );
""")

# Dimension table 2 - Song information
song_table_create = ("""
    CREATE TABLE songs (
    song_id VARCHAR PRIMARY KEY sortkey, 
    title VARCHAR, 
    artist_id VARCHAR distkey, 
    year INTEGER, 
    duration FLOAT
    );
""")

# Dimension table 3 - Artist information
artist_table_create = ("""
    CREATE TABLE artists (
    artist_id VARCHAR PRIMARY KEY sortkey distkey, 
    name VARCHAR, 
    location VARCHAR, 
    latitude FLOAT, 
    longitude FLOAT
    );
""")

# Dimension table 4 - Time information
time_table_create = ("""
    CREATE TABLE time (
    start_time TIMESTAMP PRIMARY KEY sortkey, 
    hour INTEGER NOT NULL, 
    day INTEGER NOT NULL, 
    week INTEGER NOT NULL, 
    month INTEGER NOT NULL,  
    year INTEGER NOT NULL, 
    weekday INTEGER NOT NULL
    );
""")

# STAGING TABLES INSERT

# The region is us-west-2 because the S3 bucket is there. We get an error running it on us-east-1
staging_events_copy = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    FORMAT AS JSON {}
    TIMEFORMAT as 'epochmillisecs';
""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    FORMAT AS JSON 'auto';
""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES INSERT

songplay_table_insert = ("""
    INSERT INTO songplays 
    (
    start_time, 
    user_id, 
    level, 
    song_id, 
    artist_id, 
    session_id, 
    location, 
    user_agent
    ) 
    SELECT DISTINCT TO_TIMESTAMP(se.ts,'YYYY-MM-DD HH24:MI:SS') as start_time,
    se.userId as user_id,
    se.level as level,
    ss.song_id as song_id,
    ss.artist_id as artist_id,
    se.sessionId as session_id,
    se.location as location,
    se.userAgent as user_agent
    FROM staging_events se
    JOIN staging_songs ss 
        ON se.song = ss.title 
        AND se.artist = ss.artist_name
    WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users 
    (
    user_id, 
    first_name, 
    last_name, 
    gender, 
    level
    )
    SELECT DISTINCT userId as user_id,
    firstName as first_name,
    lastName as last_name,
    gender as gender,
    level as level
    FROM staging_events
    where userId IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO songs 
    (
    song_id, 
    title, 
    artist_id, 
    year, 
    duration
    ) 
    SELECT DISTINCT song_id as song_id,
    title as title,
    artist_id as artist_id,
    year as year,
    duration as duration
    FROM staging_songs
    where song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists 
    (
    artist_id, 
    name, 
    location, 
    latitude, 
    longitude
    ) 
    SELECT DISTINCT artist_id as artist_id,
    artist_name as name,
    artist_location as location,
    artist_latitude as latitude,
    artist_longitude as longitude
    FROM staging_songs
    where artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time 
    (
    start_time, 
    hour, 
    day, 
    week, 
    month, 
    year, 
    weekday
    ) 
    SELECT DISTINCT TO_TIMESTAMP(ts,'YYYY-MM-DD HH24:MI:SS') as start_time,
    EXTRACT(hour from ts) as hour,
    EXTRACT(day from ts) as day,
    EXTRACT(week from ts) as week,
    EXTRACT(month from ts) as month,
    EXTRACT(year from ts) as year,
    EXTRACT(weekday from ts) as weekday
    FROM staging_events;
""")

# QUERY LISTS - these lists are passed into the 'create_tables.py' and 'etl.py' files

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
