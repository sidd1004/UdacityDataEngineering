import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries, select_table_queries

#Loop through and drop the tables

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

#Loop through and create the tables

def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    #Connect to the Postgres dB created on AWS
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print("Connection ", conn)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    #close the connection
    conn.close()


if __name__ == "__main__":
    main()