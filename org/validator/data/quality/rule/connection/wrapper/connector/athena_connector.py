#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
   AthenaConnector creates and excecutes athena query
   Author : Balaji Shankar
   Status : Development
"""

from pyathena import connect

from utility.utils import RuleUtility
from .. import connection_interface
import logging

# Class for connecting to Athena

class AthenaConnector(connection_interface.ConnectionInterface):

    def __init__(self):
        self.connect_str = ''
        # Read YAML file

    # Execute Athena query
    def execute_query(self, qry, parms={}):
        try:
            cursor = _get_connection()
            result = cursor.execute(qry).fetchall()
            _close_connection(cursor)
            return result
        except Exception as exception:
            logging.exception(exception)
            print(exception)
            print('Excpetion executing query in Athena DB')


# Create Athena connection

def _get_connection():
    try:
        connect_str = RuleUtility().read_connection_params('athena')
        cursor = connect(
            aws_access_key_id=connect_str['aws_access_key_id'],
            aws_secret_access_key=connect_str['aws_secret'],
            s3_staging_dir=connect_str['s3_staging_dir'],
            region_name=connect_str['region_name']).cursor()
        return cursor
    except Exception as exception:
        logging.exception(exception)
        print(exception)
        print('Excpetion connecting Athena DB')


# Close Athena connection
def _close_connection(conn):
    try:
        conn.close()
    except Exception as exception:
        logging.exception(exception)
        print(exception)
        print('Exception closing connection to  Athena DB')

# Test if the postgres connetion is working.
