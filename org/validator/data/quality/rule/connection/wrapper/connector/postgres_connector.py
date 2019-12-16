#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
   PostgressConnector creates and excecutes postgressql query
   Author : Balaji Shankar
   Status : Development
"""

from sqlalchemy import create_engine

from utility.utils import RuleUtility
from .. import connection_interface
import logging


# Class to initiate PostgresConnector

class PostgresConnector(connection_interface.ConnectionInterface):

    def __init__(self):
        self.connect_str = ''

    # Read YAML file
    def execute_query(self, qry, parms={}):
        try:
            cursor = _get_connection()
            result = cursor.execute(qry).fetchall()
            _close_connection(cursor)
            return result

        except Exception as exception:
            _close_connection(cursor)
            logging.exception(exception)
            print('Excpetion executing query in Postgres - Rule DB')


# Create postgres Connection

def _get_connection():
    try:
        connect_str = RuleUtility().read_connection_params('postgres')
        connect_url = "postgresql+psycopg2://" + connect_str['username'] + ":" + connect_str['password'] + "@" + \
                      connect_str['hostname'] + "/" + connect_str['database']
        engine = create_engine(connect_url)
        connect = engine.connect()
        return connect

    except Exception as exception:
        logging(exception)
        print(exception)
        print('Excpetion connecting Postgres - Rule DB')


# Close Postgres connection
def _close_connection(conn):
    try:
        conn.close()
    except Exception as exception:
        logging(exception)
        print(exception)
        print('Excpetion closing connection to  Postgres - Rule DB')

# Test if the postgres connetion is working.
# execute_postgres_query("SELECT * from action")
