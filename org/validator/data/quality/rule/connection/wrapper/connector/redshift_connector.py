#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
   RedshiftConnector creates and excecutes redshift query
   Author : Balaji Shankar
   Status : Development
"""
import psycopg2
import logging

from utility.utils import RuleUtility
from .. import connection_interface


# Redhsift connector to feciliate connection and query execution for Redshift
class RedshiftConnector(connection_interface.ConnectionInterface):

    def __init__(self):
        self.connect_str = ''

    # Read YAML file
    def execute_query(self, qry, parms={}):
        try:
            connect = _get_connection()
            cursor = connect.cursor()
            cursor.execute(qry)
            result = cursor.fetchall()
            for row in result:
                print("sub_row :", row)

            _close_connection(cursor)
            return result
        except Exception as exception:
            logging.exception(exception)
            print(exception)
            print('Excpetion executing query in Redshift -  DB')


# Initiates connection to reshift database
def _get_connection():
    try:

        connect_str = RuleUtility().read_connection_params('redshift')
        connect = psycopg2.connect(dbname=connect_str['database'], host=connect_str['hostname'],
                                   port=connect_str['port'], user=connect_str['username'],
                                   password=connect_str['password'])
        return connect
    except Exception as exception:
        print(exception)
        print('Excpetion connecting  Redshift DB')


# Close Redshift connection
def _close_connection(cursor):
    try:
        cursor.close()


    except Exception as exception:
        logging(exception)
        print(exception)
        print('Excpetion closing connection to  Redshift -  DB')

# Test if the postgres connetion is working.
# execute_query("SELECT hardwaretype  FROM bidevdb.conviva.hbogo_err_streaming_stg_20180804 LIMIT 10;")
