import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_song"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""

    CREATE TABLE staging_events(
    
    songplay_id INT IDENTITY(0,1), 
    artist VARCHAR,
    auth VARCHAR(120),
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession INT,
    lastName VARCHAR, 
    length float,
    level VARCHAR NOT NULL, 
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration float,
    session_id FLOAT NOT NULL, 
    song VARCHAR,
    status VARCHAR,
    ts VARCHAR NOT NULL,
    user_agent VARCHAR,
    user_id INT 
    
    )

""")

staging_songs_table_create = ("""

CREATE TABLE staging_song(

    num_songs VARCHAR,
    artist_id VARCHAR NOT NULL, 
    artist_lattitude FLOAT, 
    artist_longitude FLOAT,
    artist_location VARCHAR, 
    artist_name VARCHAR,
    song_id VARCHAR NOT NULL, 
    title VARCHAR NOT NULL, 
    duration FLOAT NOT NULL,
    year INT 
    
     



)

""")

songplay_table_create = ("""

    CREATE TABLE songplay(
    
   
    songplay_id INT NOT NULL, 
    start_time VARCHAR NOT NULL, 
    user_id INT, 
    level VARCHAR, 
    song_id VARCHAR NOT NULL, 
    artist_id VARCHAR NOT NULL, 
    session_id FLOAT NOT NULL, 
    location VARCHAR, 
    user_agent VARCHAR
    
    )
""")

user_table_create = ("""

    CREATE TABLE user_table(

    user_id INT, 
    first_name VARCHAR, 
    last_name VARCHAR, 
    gender VARCHAR, 
    level VARCHAR NOT NULL
    
    )
""")

song_table_create = ("""

    CREATE TABLE song(
    
    song_id VARCHAR NOT NULL, 
    title VARCHAR NOT NULL, 
    artist_id VARCHAR NOT NULL, 
    year INT, 
    duration FLOAT
    
    )
""")

artist_table_create = ("""

    CREATE TABLE artist(
    
    artist_id VARCHAR NOT NULL, 
    name VARCHAR, 
    location VARCHAR, 
    lattitude float, 
    longitude float
       
    )

""")

time_table_create = ("""

    CREATE TABLE time(
    
    start_time VARCHAR NOT NULL, 
    hour INT, 
    day INT, 
    week INT, 
    month INT, 
    year INT, 
    weekday INT
    
    )
""")

# STAGING TABLES

staging_events_copy = ("""
                        copy staging_events
                        from {}
                        credentials 'aws_iam_role={}'
                        json {}
                        compupdate off
                        region 'us-west-2';
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH']) 


staging_songs_copy = ("""

    copy staging_song 
    from {}
    credentials 'aws_iam_role={}'
    json 'auto' truncatecolumns
    compupdate off 
    region 'us-west-2';
    
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = (""" INSERT INTO songplay (SELECT songplay_id, ts as start_time, 
user_id, level, song_id, artist_id, session_id, artist_location as location, user_agent FROM staging_events se JOIN staging_song ss ON se.artist=ss.artist_name AND ss.title=se.song)
""")

user_table_insert = (""" INSERT INTO user_table (SELECT CAST(user_id AS INT) as user_id, firstName as first_name, lastName as last_name, 
    gender, level FROM staging_events)
""")

song_table_insert = ("""INSERT INTO song (SELECT song_id, title, artist_id, year, 
    duration FROM staging_song)


""")

artist_table_insert = (""" INSERT INTO artist (SELECT artist_id, artist_name as name, artist_location as location, 
    artist_lattitude as lattitude, artist_longitude as longitude FROM staging_song)
""")

time_table_insert = (""" INSERT INTO time (SELECT  TIMESTAMP 'epoch' + CAST(ts AS FLOAT) * INTERVAL '1 Second ' AS start_time, 
    EXTRACT(hour from TIMESTAMP 'epoch' + CAST(ts AS FLOAT) * INTERVAL '1 Second ') as hour, 
    EXTRACT(day from TIMESTAMP 'epoch' + CAST(ts AS FLOAT) * INTERVAL '1 Second ') as day, 
    EXTRACT(week from TIMESTAMP 'epoch' + CAST(ts AS FLOAT) * INTERVAL '1 Second ') as week, 
    EXTRACT(month from TIMESTAMP 'epoch' + CAST(ts AS FLOAT) * INTERVAL '1 Second ') as month, 
    EXTRACT(year from TIMESTAMP 'epoch' + CAST(ts AS FLOAT) * INTERVAL '1 Second ') as year, 
    EXTRACT(weekday from TIMESTAMP 'epoch' + CAST(ts AS FLOAT) * INTERVAL '1 Second ') as weekday FROM staging_events)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
