import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from time import time


def load_staging_tables(cur, conn):
    """Load staging tables from S3 files
    
    Parameters
    ----------
    cur : cursor
        cursor of psycopg2 database connection
    conn : connection
        connection of psycopg2
    """
    for query in copy_table_queries:
        print("======= LOADING Staging TABLE =======")
        print(query)
        t0 = time()
        cur.execute(query)
        conn.commit()
        loadTime = time()-t0
        print("=== DONE IN: {0:.2f} sec\n".format(loadTime))


def insert_tables(cur, conn):
    """Load final tables from staging tables
    
    Parameters
    ----------
    cur : cursor
        cursor of psycopg2 database connection
    conn : connection
        connection of psycopg2
    """
    for query in insert_table_queries:
        print("======= LOADING Final TABLE =======")
        print(query)
        t0 = time()
        cur.execute(query)
        conn.commit()
        loadTime = time()-t0
        print("=== DONE IN: {0:.2f} sec\n".format(loadTime))


def main():
    """
        - Read DB credential from config file
        - Loading staging tables from S3 files
        - Loading final tables from staging tables
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