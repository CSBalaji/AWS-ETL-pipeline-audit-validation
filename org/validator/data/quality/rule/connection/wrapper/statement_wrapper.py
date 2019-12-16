#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
   Statement_Wrapper creates and excecutes postgressql query
   Author : Balaji Shankar
   Status : Development
"""
import datetime
import string
import logging

from ..wrapper.connection_factory import ConnectionFactory
from ..wrapper.connector.utility.utils import RuleUtility


# Wrapper for Statement execution

class Statement_Wrapper(object):

    def __init__(self):
        self.connect_str = ''


# Prepare data for inserting to rule_execution_log

def _insert_log(rule_id, status, metrics, error):
    try:
        # _replaced_query_with_param = Rule_Utility().construct_query('rule','rule_id',rule_id)
        liststr = "".join(str(x) for x in metrics)
        # escaped_str= liststr.translate(string.maketrans("-", " "))
        escaped_str = string.replace(liststr, "-", "")
        jsonstr = "{data: {" + escaped_str + "}}"
        print(jsonstr)
        params = {}
        params['rule_id1'] = str(rule_id)
        params['rule_id2'] = str(rule_id)
        params['status'] = str(status)
        params['exec_time'] = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M"))
        params['exec_date'] = str(datetime.datetime.now())
        # _params['metrics'] = json.dumps(jsonstr)
        params['metrics'] = {}
        params['error'] = str(error)
        _replaced_query_with_param = RuleUtility().construct_query_with_params('log', params)
        connect = ConnectionFactory()
        return connect.execute('log', _replaced_query_with_param)

    except Exception as exception:
        logging.exception(exception)
        print(exception)
        print('Exception while Inserting statement Execution Log')


# Fetch
def _insert_log(rule_id, status, metrics, error):
    try:
        # _replaced_query_with_param = Rule_Utility().construct_query('rule','rule_id',rule_id)
        liststr = "".join(str(x) for x in metrics)
        # escaped_str= liststr.translate(string.maketrans("-", " "))
        escaped_str = string.replace(liststr, "-", "")
        jsonstr = "{data: {" + escaped_str + "}}"
        print(jsonstr)
        params = {}
        params['rule_id1'] = str(rule_id)
        params['rule_id2'] = str(rule_id)
        params['status'] = str(status)
        params['exec_time'] = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M"))
        params['exec_date'] = str(datetime.datetime.now())
        # _params['metrics'] = json.dumps(jsonstr)
        params['metrics'] = {}
        params['error'] = str(error)
        _replaced_query_with_param = RuleUtility().construct_query_with_params('log', params)
        connect = ConnectionFactory()
        return connect.execute('log', _replaced_query_with_param)

    except Exception as exception:
        logging.exception(exception)
        print(exception)
        print('Exception while Inserting statement Execution Log')


def _fetch_statement(type, query, row):

    connect = ConnectionFactory()

    sub_result = connect.execute('postgres', query)
    for sub_row in sub_result:
        print("sub_row :", sub_row)
        replaced_query_with_param = RuleUtility().construct_query(row['statement'],
                                                                  sub_row['name'],
                                                                  sub_row['value'])
        sub_result = connect.execute(type, replaced_query_with_param)
    for sub_row in sub_result:
        print("sub_row :", sub_row)
    _insert_log(row['id'],
                'Execution Successful',
                sub_result,
                "")

# Executes the Rule Statement

def execute_statement(result):
    try:
        from connection_factory import ConnectionFactory
        connect = ConnectionFactory()

        for row in result:
            params = {}
            params['rule_id'] = row['id']
            replaced_query_with_param = RuleUtility().construct_query_with_params('rule_params', params)
            if row['compute_type'].lower() == 'postgres':
                try:
                    _fetch_statement('postgres', replaced_query_with_param, row)
                except Exception as exception:
                    print('Exception while executing Rule Statement' + exception)
                    logging.exception(exception)
                    _insert_log(row['rule_id'],
                                'ERROR',
                                row['statement'],
                                'Error running rule ' + row['id'])
                    print('Exception while executing Rule Statement')
                    continue
            elif row['compute_type'].lower() == 'redshift':
                try:
                    _fetch_statement('redshift', replaced_query_with_param, row)

                except Exception as exception:
                    logging.exception(exception)
                    print('Exception while executing Rule Statement')
                    _insert_log(row['rule_id'],
                                'ERROR',
                                '',
                                'Error running rule ' + row['rule_id'])
                    print('Exception while executing Rule Statement')
                    continue

            elif row['compute_type'].lower() == 'athena':
                try:
                    _fetch_statement('athena', replaced_query_with_param, row)
                except Exception as exception:
                    logging.exception(exception)
                    print('Exception while executing Rule Statement')
                    _insert_log(row['rule_id'],
                                'ERROR',
                                '',
                                'Error running rule ' + row['rule_id'])
                    print('Exception while executing Rule Statement')
                    continue

            elif row['compute_type'].lower() == 'snowflake':
                try:
                    logging.exception(exception)
                    _fetch_statement('snowflake', replaced_query_with_param, row)
                except Exception as exception:
                    logging.exception(exception)
                    print('Exception while executing Rule Statement')
                    _insert_log(row['rule_id'],
                                'ERROR',
                                '',
                                'Error running rule ' + row['rule_id'])
                    print('Exception while executing Rule Statement')
                    continue

    except Exception as exception:
        logging.exception(exception)
        _insert_log(result['rule_id'],
                    'ERROR',
                    '',
                    'Error running rule ' + result['rule_id'])
        print('Exception while executing Rule Statement')

# Test if the postgres connetion is working.
# execute_postgres_query("SELECT * from action")
