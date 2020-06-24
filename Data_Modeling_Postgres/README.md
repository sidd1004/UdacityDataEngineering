### AIM OF THE PROJECT 
To build a Data Model for a startup called "Sparkify" which is an online music streaming platform. 
The goal of this project was to analyse the data and report to Sparkify the kind of music their customers were listening to.

### DATASETS
1. Song Dataset - Details about each song provided in a JSON format
2. Log Dataset - Describes the user activity on the application. Consisits of .log files.

### Database Schema 
The Star Schema model is used in this project. We split the database into facts and dimensions.
FACT Table: songplays: attributes referencing to the dimension tables.
DIMENSION Tables: users, songs, artists and time table.

#### Song Plays table

- Name: songplays
- Type: Fact table

| Column | Type |
| ------ | ---- |
| songplay_id | SERIAL 
| start_time | TIMESTAMP 
| user_id | INTEGER NOT NUL 
| level | TEXT 
| song_id | TEXT 
| artist_id | TEXT 
| session_id | INTEGER 
| location | TEXT 
| user_agent | TEXT 

#### Users table

- Name: users
- Type: Dimension table

| Column | Type
| ------ | ----
| user_id | INTEGER NOT NULL PRIMARY KEY 
| first_name | TEXT NOT NULL
| last_name | TEXT
| gender | TEXT
| level | TEXT


#### Songs table

- Name: songs
- Type: Dimension table

| Column | Type 
| ------ | ----
| song_id | TEXT PRIMARY KEY NOT NULL
| title | TEXT NOT NULL
| artist_id | TEXT NOT NULL
| year | INTEGER 
| duration | FLOAT NOT NULL 

#### Artists table

- Name: artists
- Type: Dimension table

| Column | Type
| ------ | ---- 
| artist_id | TEXT PRIMARY KEY NOT NULL
| name | TEXT NOT NULL
| location | TEXT 
| latitude | FLOAT 
| longitude | FLOAT
#### Time table

- Name: time
- Type: Dimension table

| Column | Type 
| ------ | ---- 
| start_time | TIMESTAMP PRIMARY KEY NOT NULL 
| hour | INT 
| day | INT  
| week | INT
| month | INT  
| year | INT
| weekday | TEXT

### FILE STRUCTURE

1. data - This is the folder that contains the datasets.
2. create_tables.py - This file is responsibe for running the CREATE and DROP queries.
3. etl.ipynb - This is a jupyter notebook which explains and acts as a guide for the `etl.py` file.
4. etl.py - This file that contains the complete ETL code for this project.
5. sql_queries.py - This is a python script contains the create and insert contains for the database.
3. test.ipynb - This jupyter notebook contains tests for the CREATE and DROP queries.

### RUNNING THE PROJECT

1. Go to terminal and run the `create_tables.py script` by typing `python create_tables.py` in the terminal. This will ensure that all the old tables are dropped and new ones are created.
2. Next run the `etl.py` script by typing `python etl.py`. This will run the ETL queries and read data from the log files and insert them into the necessary tables.