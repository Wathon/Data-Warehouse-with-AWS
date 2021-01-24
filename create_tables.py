import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Dropping staging and final tables if exists
    
    Parameters
    ----------
    cur : cursor
        cursor of psycopg2 database connection
    conn : connection
        connection of psycopg2
    """
    # drop tables
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        print('Done!')


def create_tables(cur, conn):
    """Creating staging and final tables
    
    Parameters
    ----------
    cur : cursor
        cursor of psycopg2 database connection
    conn : connection
        connection of psycopg2
    """
    # create tables
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        print('Done!')


def main():
    """
        - Read DB credential from config file
        - Dropping staging and final tables if exists
        - Creating staging and final tables
    """
    # read config from dwh.cfg
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # connect to database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print('Dropping Tables:')
    drop_tables(cur, conn)
    print('Creating Tables:')
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()