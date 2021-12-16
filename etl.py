import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    """
    Description: 
        This function executes the queries in the 'copy_table_queries' list.
        This is imported from the sql_queries.py file as shown above.
        This function is responsible for loading the raw JSON files (log/song data) 
        into their respective staging tables.
    
    Arguments:
        cur: the cursor object.
        conn: connection to the database.

    Returns:
        None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    """
    Description: 
        This function executes the queries in the 'insert_table_queries' list.
        This is imported from the sql_queries.py file as shown above.
        This function is responsible for loading the fact table (songplays) and dimension tables
        (songs, artists, users, time) from the staging tables that are populated in the function above.
    
    Arguments:
        cur: the cursor object.
        conn: connection to the database.

    Returns:
        None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
    Description:
        Reads the config file and obtains the AWS Redshift cluster details and login details.
        Establishes a connection to the Redshift cluster.
        Drops all the tables.  
        Creates all tables needed. 
        Finally, closes the connection. 
        
    Arguments:
        None
    
    Returns:
        None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()