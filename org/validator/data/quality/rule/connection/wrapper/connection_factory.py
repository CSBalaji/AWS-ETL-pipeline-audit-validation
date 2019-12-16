#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
   Statement_Wrapper creates and excecutes postgressql query
   Author : Balaji Shankar
   Status : Development
"""
from __builtin__ import staticmethod
import logging
from connector.athena_connector import AthenaConnector
from connector.log_connector import LogConnector
from connector.postgres_connector import PostgresConnector
from connector.redshift_connector import RedshiftConnector
from connector.snowflake_connector import SnowflakeConnector



# Connection Factory to initiate respective connectors based on parameters

class ConnectionFactory(object):

    # Execute Rule Statement with rules as part of the parameter

    # Create based on class name:
    @staticmethod
    def execute(type, qry):

        try:
            if type.lower() == 'postgres':  return PostgresConnector().execute_query(qry)
            if type.lower() == 'athena':    return AthenaConnector().execute_query(qry)
            if type.lower() == 'redshift':  return RedshiftConnector().execute_query(qry)
            if type.lower() == 'snowflake': return SnowflakeConnector().execute_query(qry)
            if type.lower() == 'log':       return LogConnector().execute_query(qry)

        except Exception as exception:
            logging.exception(exception)
            print('ConnectionFactory: No connectors found')

# Test if the postgres connetion is working.
# execute_postgres_query("SELECT * from action")
