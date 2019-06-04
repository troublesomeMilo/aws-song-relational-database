import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
	Executes the set of queries stored in drop_table_queries to drop existing tables.
	
	Arguments: cur=cursor object, conn=connection object
	Returns: None
	"""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
	Executes the set of queries stored in create_table_queries to create new tables.
	
	Arguments: cur=cursor object, conn=connection object
	Returns: None
	"""
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
	When the script is directly called, the main method parses and read the config file,
	creates a connection to the database and a cursor object, runs the created drop_tables() 
	function and create_tables() function, and then disconnects.
	
	Arguments: None
	Returns: None
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