# Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As they are growing a lot, they want their data to be processed using Amazon Redshift.

# Project Description
In this project, the main goal is to move two data sources from public S3 buckets to AWS Redshift.

## S3 Buckets:
-Song data: s3://udacity-dend/song_data
-Log data: s3://udacity-dend/log_data
-Log data json path: s3://udacity-dend/log_json_path.json

## AWS Redshift:
The main goal is to pick the data from the S3 buckets and load them into Redshift, which is a Data Warehouse with columnar storage. Moving the data to this cloud service will help retrieve data faster and store large amounts of it.

### Data
- `Song datasets`: Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example:

```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

- `Log datasets`: The second dataset consists of log files in JSON format. The log files in the dataset are partitioned by year and month

To improve the performance we will be use a STAR schema. This schema consists of one fact table referencing any number of dimension tables.
- **Fact Table:** songplays: attributes referencing to the dimension tables
- **Dimension Tables:** users, songs, artists and time tables.

### Fact Table
`songplays` - records the log data for each of song plays
- songplay_id (IDENTITY) PRIMARY KEY: ID of each song played
- start_time (start_time) NOT NULL: Timestamp of start of user activity also the SORTKEY and REFERENCES time(start_time).
- user_id (INT) NOT NULL: ID of user REFERENCES users (user_id) DISTKEY
- level (TEXT) NOT NULL: Level of the user
- song_id (TEXT): ID Of the song played, REFERENCES songs(song_id),
- artist_id (TEXT): ID of the artist of the song played, REFERENCES artists(artist_id)
- session_id (TEXT): ID of the user session 
- location (TEXT): User location
- user_agent (TEXT): Agent used by user to connect to the application

### Dimension Tables
`users` - user list
- user_id (INT) PRIMARY KEY: ID of the user, and SORTKEY
- first_name (TEXT): First name of the user
- last_name (TEXT): Last name of user
- gender (TEXT): Gender of the user
- level (TEXT): Level of the user
- diststyle ALL - To be replicated on all nodes for faster processing and less shuffling

`songs` - songs list in the app
- song_id (TEXT) PRIMARY KEY: ID of the song, and SORTKEY
- title (TEXT) NOT NULL: Title of the song
- artist_id (TEXT) NOT NULL: ID of the artist
- year (INT): Year of the song release date
- duration (DECIMAL(10)): Song duration in ms

`artists` - artists in music database
- artist_id (TEXT) PRIMARY KEY: Artist id and SORTKEY
- name (TEXT): Name of the artist
- location (TEXT): Location of the artist
- lattitude (DECIMAL(9)): Lattitude info of the artist
- longitude (DECIMAL(9)): Longitude info of the artist
- diststyle ALL - To be replicated on all nodes for faster processing and less shuffling

`time` - timestamps of records in songplays broken down into specific units for convenience
- start_time (TIMESTAMP) PRIMARY KEY: Timestamp info and SORTKEY
- hour (INT): Hour of start_time
- day (INT): Day of start_time
- week (INT): Week number of start_time
- month (INT): Month number of start_time
- year (INT): Year number of start_time
- weekday (INT): Number of the week of start_time

# How to run process
1. Start up a AWS Redshift Cluster
- Make sure to setup the IAM role to AmazonS3ReadOnlyAccess.
- Create a dc2.large cluster with 4 nodes.
- Copy the necessary credentials and enter it into the dwh.cfg file.

2. Open up a terminal.

3. Run 'python create_tables.py'
- This will create the tables

4. Run 'python etl.py'
- This will execute the ETL transformations.

## Files and folders
* `dwh.cfg` configuration file. Contains the necessary credentials to connect to AWS Redshift and IAM Role details. 
* `sql_queries.py` script file that contains the SQL queries that are used to create, insert, delete and query information.
* `create_tables.py` script file that creates or drops the tables.
* `etl.py` script file that executes the ETL process for AWS Redshift.



 


 
 

