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


# Log connector Connector logs to statement execution status to Rule Execution log table

class LogConnector(connection_interface.ConnectionInterface):

    def __init__(self):
        self.connect_str = ''

    # Read YAML file
    def execute_query(self, qry):
        try:
            from ..connection_factory import ConnectionFactory
            from utility.utils import RuleUtility

            cursor = _get_connection()
            result = cursor.execute(qry)
            _close_connection(cursor)
            return result
        except Exception as exception:
            logging.exception(exception)
            print(exception)
            print('Excpetion executing query in Postgres - Rule DB')


# Creates connection for Rule Statement Execution Log

def _get_connection():
    try:
        connect_str = RuleUtility().read_connection_params('postgres')
        connect_url = "postgresql+psycopg2://" + connect_str['username'] + ":" + connect_str['password'] + "@" + \
                      connect_str['hostname'] + "/" + connect_str['database']
        engine = create_engine(connect_url)
        connect = engine.connect()
        return connect
    except Exception as exception:
        logging.exception(exception)
        print(exception)
        print('Excpetion connecting Postgres - Rule DB')


# Close Log table connection
def _close_connection(conn):
    try:
        conn.close()
    except Exception as exception:
        logging.exception(exception)
        print(exception)
        print('Excpetion closing connection to  Postgres - Rule DB')

# Test if the postgres connetion is working.
# execute_postgres_query("SELECT * from action")
