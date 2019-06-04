import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS
        staging_events ( 
	  	  artist TEXT
		, auth TEXT
		, fristName TEXT
		, gender TEXT
		, iteminSession INT
		, lastName TEXT
		, length NUMERIC
		, level TEXT
		, location TEXT
	    , method TEXT
		, page TEXT
		, registration NUMERIC
		, sessionId INT
		, song TEXT
		, status INT
		, ts TIMESTAMP
		, userAgent TEXT
		, userId INT
		);
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS
	  staging_songs (
	      num_songs INT
		, artist_id INT
		, artist_latitude NUMERIC
		, artist_longitude NUMBERIC
		, artist_location TEXT
		, artist_name TEXT
		, song_id VARCHAR
		, title TEXT
		, duration NUMERIC
		, year INT
		);
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS 
      songplays ( 
	      songplay_id IDENTITY(0,1) 
        , start_time VARCHAR NOT NULL 
        , user_id INT NOT NULL 
        , level TEXT 
        , song_id VARCHAR 
        , artist_id VARCHAR 
        , session_id INT 
        , location VARCHAR 
        , user_agent VARCHAR 
        , PRIMARY KEY (songplay_id)
		);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS 
      users ( 
          user_id INT 
        , first_name TEXT 
        , last_name TEXT 
        , gender TEXT 
        , level TEXT 
        , PRIMARY KEY (user_id)
		);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS 
      songs ( 
          song_id VARCHAR 
        , title VARCHAR NOT NULL 
        , artist_id VARCHAR NOT NULL 
        , year INT 
        , duration REAL 
        , PRIMARY KEY (song_id)
		);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS 
      artists ( 
          artist_id VARCHAR 
        , name VARCHAR NOT NULL 
        , location VARCHAR 
        , lattitude NUMERIC 
        , longitude NUMERIC 
        , PRIMARY KEY (artist_id)
		);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS 
      time ( 
          start_time VARCHAR 
        , hour INT 
        , day INT 
        , week INT 
        , month INT 
        , year INT 
        , weekday TEXT 
        , PRIMARY KEY (start_time)
		);
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events 
	FROM {}
	IAM_ROLE {}
	JSON {}
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    COPY staging_songs
	FROM {}
	IAM_ROLE {}
	JSON 'auto'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
	SELECT e.start_time, e.user_id, e.level, s.song_id, s.artist_id, e.session_id, e.location, e.user_agent
	FROM (SELECT TIMESTAMP 'epoch' + ts / 1000 * interval '1 second' AS start_time, *
	      FROM staging_events
		  WHERE page = 'NextSong') e
	LEFT JOIN staging_songs s
	ON e.song = s.title AND e.artist = s.artist_name AND e.length = s.duration;
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
	SELECT userId, firstName, lastName, gender, level
	FROM staging_events
	WHERE page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
	SELECT song_id, title, artist_id, year, duration
	FROM staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
	SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
	FROM staging_songs;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
	SELECT DISTINCT start_time, EXTRACT(hour FROM start_time), EXTRACT(day FROM start_time),
	    EXTRACT(week FROM start_time), EXTRACT(month FROM start_time), EXTRACT(year from start_time),
		EXTRACT(dayofweek FROM start_time)
    FROM songplays;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
