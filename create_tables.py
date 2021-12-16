import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    """
    Description: 
        Drops each table using the queries in `drop_table_queries` list.
        This is imported from the sql_queries.py file as shown above
    
    Arguments:
        cur: the cursor object
        conn: connection to the database
    
    Returns:
        None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    """
    Description:
        Creates each table using the queries in `create_table_queries` list. 
        This is imported from the sql_queries.py file as shown above
        
    Arguments:
        cur: the cursor object
        conn: connection to the database
        
    Returns:
        None
    """
    for query in create_table_queries:
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

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()