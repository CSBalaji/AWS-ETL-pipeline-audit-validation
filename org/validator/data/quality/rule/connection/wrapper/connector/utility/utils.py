#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
   Statement_Wrapper creates and excecutes postgressql query
   Author : Balaji Shankar
   Status : Development
"""
import string
import logging
import yaml


# Utility class

class RuleUtility:

    def __init__(self):
        self.connect_str = ''


    # Read connection parameters from yaml file
    @staticmethod
    def read_connection_params( params):
        try:
            with open("./resources/connectionparams.yaml", 'r') as stream:
                connect_params = yaml.load(stream)
            return connect_params[params]
        except Exception as exception:
            logging.exception(exception)
            print(exception)
            print('Invalid Yaml File/Connection parameter')

    def construct_query(self, param, param_key, param_value):
        try:
            _str1 = '{' + param_key + '}'
            _str2 = "'" + param_value + "'"
            query = string.replace(param, _str1, _str2)
            return query
        except Exception as exception:
            logging.exception(exception)
            print('Invalid Query parameter key/value')

    def construct_query_with_params(self, prop, params={}):
        try:
            query = _fetch_queries_with_params(self, prop)
            for param_key, param_value in params.iteritems():
                _str1 = '{' + param_key + '}'
                _str2 = "'" + str(param_value) + "'"
                _str3 = string.replace(query, _str1, _str2)
                query = _str3
            return query
        except Exception as exception:
            logging.exception(exception)
            print('Invalid Query parameter key/value')


# Fetches queries from yaml file

def _fetch_queries_with_params(self, param):
    try:
        with open("./resources/queries.yaml", 'r') as stream:
            connect_params = yaml.load(stream)
        return connect_params[param]['qry']
    except Exception as exception:
        logging.exception(exception)
        print(exception)
        print('Invalid Yaml File/Connection parameter')

# Test if the postgres connetion is working.
# execute_postgres_query("SELECT * from action")
