## Description

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of  JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.


## Structure


Star scheme enables the company to view the user behaviour over several dimensions. 
The fact table is used to store all user song activities that contain the category "NextSong".   Using this table, the company can relate and analyze the dimensions users, songs, artists and time.

   #### Fact Table 

    songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    
   #### Dimension Tables

    users    (user_id, first_name, last_name, gender, level)
    songs    (song_id, title, artist_id, year, duration)
    artists  (artist_id, name, location, lattitude, longitude)
    time     (start_time, hour, day, week, month, year, weekday)


## Dataset 


The data is stored in s3 buckets hosten at AWS:

    Song data: s3://udacity-dend/song_data
    Log data: s3://udacity-dend/log_data
    
    
## Execution


The processes can be executed on AWS notebook for EMR instance.


## Files


    etl.py: Pipeline steps
    dl.cfg: Configuration file
    EMRNotebook.jpynp: EMR notebook file
    

