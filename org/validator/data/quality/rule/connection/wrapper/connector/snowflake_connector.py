#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
   SnowflakeConnector fecilitates connection and  executes
    snowflake query
   Author : Balaji Shankar
   Status : Development
"""

import snowflake.connector
import logging
import dynamodb as dynamo
from utility.utils import RuleUtility
from ..connection_interface import ConnectionInterface
import logging
# Snowflake connector

class SnowflakeConnector(ConnectionInterface):

    def __init__(self):
        self.connect_str = ''

    # Executes Snowflake Rule Statement

    def execute_query(self, qry, parms={}):
        try:
            connection = _get_connection(1)
            cursor = connection.cursor()
            result = cursor.execute(qry).fetchone();
            _close_connection(connection, cursor)
            return result
        except Exception as exception:
            _close_connection(cursor)
            logging.exception(exception)
            print('Excpetion executing query in Snowflake DB')


# Create fetches connection

def _get_connection():
    try:
        connect_str = RuleUtility().read_connection_params('snowflake')
        dynamo_db_cred = dynamo.get_cred_from_ddb(connect_str['dyanmodb_table'],
                                                  connect_str['dynamodb_envname'])
        db_user, db_passwd = dynamo_db_cred['db_user'], dynamo.get_passwd(dynamo_db_cred['db_passwd'])
        conn = snowflake.connector.connect(
            user=db_user,
            password=db_passwd,
            account=connect_str['account'],
            autocommit=True
        )
        cur = conn.cursor()
        cur.execute("alter session set timezone = 'UTC'")
        cur.execute("use role {}".format(connect_str['role']))
        cur.execute("use database {}".format(connect_str['database']))
        if connect_str['schema']:
            cur.execute("use schema {}".format(connect_str['schema']))
        cur.execute("use warehouse {}".format(connect_str['datawarehouse']))
        return conn
    except Exception as exception:
        logging.exception(exception)
        print('Excpetion connecting Snowflake DB')


# Close Snowflake connection
def _close_connection(conn,cursor):
    try:
        conn.close()
        cursor.close()
    except Exception as exception:
        logging.exception(exception)
        print(exception)
        print('Excpetion closing connection to  Snowflake DB')

# Execute Postgres query


# Test if the postgres connetion is working.
