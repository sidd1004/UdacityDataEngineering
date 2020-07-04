# Project: Data Lake

## Introduction

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Dataset from S3 buckets:

### Source Data
- **Song datasets**: all files are found in S3 under *s3a://udacity-dend/song_data*. Sample data:

```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

- **Log datasets**: all files are found in S3 under *s3a://udacity-dend/log_data*.

## Tables

### Fact Table

<b>songplays_table<b> - records in event logs based on song plays. Contains data with page NextSong and columns songplay_id, start_time, month, year, user_id, level, song_id, artist_id, session_id, location, user_agent

<b>Dimension Tables</b>

<b>users_table</b> - users data based on user_id, first_name, last_name, gender, level

<b>songs_table</b> - songs data based on song_id, title, artist_id, year, duration

<b>artists_table</b> - artists data based on artist_id, name, location, lattitude, longitude

<b>date_time_table</b> - date and time info split and stored as start_time, hour, day, week, month, year, weekday

## Files

The files are as follows:

- dl_config.cfg: File with AWS credentials (To be filled with your credentials and renamed as `dl.cfg` not to be committed to Github).
- etl.py: File that contains the script to extract the songs and log data from S3 and transforms using Spark, and to load it to the dimensional tables created in parquet format to S3.
- README.md: Information about the project.

## How to Run the project: 
1.Insert your AWS IAM credentials in dl_config.cfg file and rename to dl.clg.
2.Run `python etl.py` in the terminal.
