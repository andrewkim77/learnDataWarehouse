import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = " DROP TABLE IF EXISTS staging_events "
staging_songs_table_drop = " DROP TABLE IF EXISTS staging_songs "
songplay_table_drop = " DROP TABLE IF EXISTS songplays "
user_table_drop = " DROP TABLE IF EXISTS users "
song_table_drop = " DROP TABLE IF EXISTS songs "
artist_table_drop = " DROP TABLE IF EXISTS artists "
time_table_drop = " DROP TABLE IF EXISTS time "

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events 
(
  artist         VARCHAR ,
  auth           VARCHAR ,
  firstname      VARCHAR ,
  gender         VARCHAR ,
  iteminsession  INTEGER ,
  lastname       VARCHAR ,
  length         FLOAT ,
  level          VARCHAR ,
  location       VARCHAR ,
  method         VARCHAR ,  
  page           VARCHAR ,
  registration   VARCHAR ,
  sessionid   INTEGER ,
  song        VARCHAR ,
  status      VARCHAR ,
  ts          BIGINT ,
  useragent   VARCHAR ,
  userid      INTEGER 
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs
(
  artist_id     VARCHAR ,
  artist_latitude        VARCHAR ,
  artist_location        VARCHAR ,
  artist_longitude    VARCHAR ,
  artist_name      VARCHAR ,
  duration       FLOAT ,
  num_songs        INTEGER ,
  song_id        VARCHAR ,
  title   VARCHAR ,
  year   INTEGER 
)
""")

songplay_table_create = ("""
CREATE TABLE songplays 
(
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY, 
    start_time int NOT NULL, 
    user_id varchar NOT NULL, 
    level varchar NOT NULL, 
    song_id varchar, 
    artist_id varchar, 
    session_id varchar NOT NULL, 
    location varchar NOT NULL,
    user_agent varchar NOT NULL
)
""")

user_table_create = ("""
CREATE TABLE users
(
    user_id varchar PRIMARY KEY, 
    first_name  varchar NOT NULL, 
    last_name  varchar NOT NULL, 
    gender  varchar NOT NULL, 
    level varchar NOT NULL
)
""")

song_table_create = ("""
CREATE TABLE songs
(
    song_id varchar PRIMARY KEY, 
    title varchar NOT NULL, 
    artist_id varchar NOT NULL, 
    year int , 
    duration float NOT NULL
)
""")

artist_table_create = ("""
CREATE TABLE artists
(
    artist_id varchar PRIMARY KEY, 
    name varchar NOT NULL, 
    location varchar , 
    latitude float , 
    longitude float
)
""")

time_table_create = ("""
CREATE TABLE time
(
    start_time int PRIMARY KEY, 
    hour int NOT NULL, 
    day int NOT NULL, 
    week int NOT NULL, 
    month int NOT NULL, 
    year int NOT NULL, 
    weekday int NOT NULL
)
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from 's3://udacity-dend/log_data/2018/11'
    credentials 'aws_iam_role={}'
    compupdate off region 'us-west-2' format as json 'auto' ;
""").format(DWH_ROLE_ARN)

staging_songs_copy = ("""
    copy staging_songs from 's3://udacity-dend/song_data/'
    credentials 'aws_iam_role={}'
    compupdate off region 'us-west-2' format as json 'auto' ;
""").format(DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
insert into songplays (
  user_id      ,
  level         ,
  song_id       ,
  artist_id     ,
  session_id    ,
  location      ,
  user_agent )
select 
  userid as user_id      ,
  level         ,
  song_id       ,
  artists.artist_id     ,
  sessionid as session_id    ,
  staging_events.location      ,
  useragent as user_agent    
from staging_events
join songs on ( staging_events.song = songs.title and staging_events.length = songs.duration )
join artists on staging_events.artist = artists.name
""")

user_table_insert = ("""
insert into users 
( select 
  userid as user_id      ,
  firstname as first_name    ,
  lastname as last_name     ,
  gender       ,
  level         
from staging_events )
ON CONFLICT(user_id) DO UPDATE SET level = excluded.level
""")

song_table_insert = ("""
insert into songs 
( select 
  song_id         ,
  title           ,
  artist_id       ,
  year            ,
  duration            
from staging_songs )
ON CONFLICT( song_id ) DO NOTHING
""")

artist_table_insert = ("""
insert into artists 
( select 
  artist_id         ,
  artist_name as name              ,
  artist_location as location          ,
  artist_latitude as latitude          ,
  artist_longitude as longitude              
from staging_songs )
ON CONFLICT( artist_id ) DO NOTHING
""")

time_table_insert = ("""
insert into time 
( select 
  ts/1000         ,
  extract(hour from timestamp ts) as hour ,
  extract(day from timestamp ts) as day          ,
  extract(week from timestamp ts) as week          ,
  extract(month from timestamp ts) as month,
  extract(year from timestamp ts) as year,
  extract(weekday from timestamp ts) as  weekday                
from staging_events )
ON CONFLICT( start_time ) DO NOTHING
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]