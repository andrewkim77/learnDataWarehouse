ETL TO AWS Redshift
===================

### Caution
1. Start AWS redshift cluster before this project starting.
2. Use region 'us-west-2', it does not work at other region.
        
Etl process consists of two part, 'load_staging_tables' and 'insert_tables'.

load_staging_tables explanation
===============================
using 'copy_table_queries' load data from S3 to stage

### Copy to Redshift from json file data
code of it is like this

    qry = """
        copy staging_songs from 's3://udacity-dend/song_data/'
        credentials 'aws_iam_role={}'
        compupdate off region 'us-west-2' format as json 'auto' ;
    """.format(DWH_ROLE_ARN)

insert_tables explanation
=========================
using 'insert_table_queries' insert redshift database from stage

It can be duplicated, So insert queries avoid duplicate data like followings

### conflict data execution
when there is conflict data, using following example.
case users table

    insert into users 
    ( select 
       user_id
       ....        
    from staging_events )
    ON CONFLICT(user_id) DO UPDATE SET level = excluded.level
    
case songs, artists, time table
    
    INSERT INTO songs (song_id, title, artist_id, year, duration)
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs

