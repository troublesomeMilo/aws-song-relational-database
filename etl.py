import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
	Executes insert statements imported from sql_queries.py to load
	JSON data from S3 bucket into staging tables.
	
	Arguments: cur=cursor object, conn=connection object
	Returns: None
	"""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
	Executes insert statements imported from sql_queries.py to load
	the fact and dimensional tables from the staging tables.
	
	Arguments: cur=cursor object, conn=connection object
	Returns: None
	"""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
	When the script is directly called, the main method is executed.
	It parses the config file and reads it. It then creates a connection to
	the redshift cluster using infomation provided in the config. It then
	generates a cursor object. It executes the previously defined 
	load_staging_tables() function and insert_tables() function. Finally it 
	closes the connection.
	
	Arguments: None
	Returns: None
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