# !/usr/bin/python
# -*- coding: utf-8 -*-

"""
   Rule Executes statement based on the startemnt type
   Author : Balaji Shankar
   Status : Development
"""

from connection.wrapper.connection_factory import ConnectionFactory
from connection.wrapper.connector.utility.utils import RuleUtility
from connection.wrapper.statement_wrapper import execute_statement
import logging


def _get_rule(rule_id):
    try:
        _params = {}
        _params['rule_id'] = rule_id
        _replaced_query_with_param = RuleUtility().construct_query_with_params('rule', _params)
        connect = ConnectionFactory()
        rslt = connect.execute('postgres', _replaced_query_with_param)
        return rslt
    except Exception as exception:
        logging.exception(exception)
        print('No rules Found')


def _get_rules(rule_grp_id):
    try:
        params = {}
        params['rule_group_id'] = rule_grp_id
        replaced_query_with_param = RuleUtility().construct_query_with_params('rule_group', params)
        connect = ConnectionFactory()
        rslt = connect.execute('postgres', replaced_query_with_param)
        return rslt
    except Exception as exception:
        logging.exception(exception)
        print('No rules Found')

class Rule(object):

    def __init__(self):
        self.connect_str = ''

    def execute_rule( rule_id):
        try:
            result = _get_rule(rule_id)
            execute_statement(result)
        except Exception as exception:
            logging.exception(exception)
            print('Error executing rule')

    def execute_rule_group( rule_group_id):
        try:
            from connection.wrapper.statement_wrapper import execute_statement
            result = _get_rules(rule_group_id)
            execute_statement(result)
        except Exception as exception:
            logging.exception(exception)
            print(exception)
            print('Error executing rule group')

    logging.basicConfig(filename='rule.log', level=logging.DEBUG)
    logging.info('Started')

    print("\nPostgres Statement with rule parameter ")
    print("---------------------------------------")
    execute_rule("<rule ID>")

    print("\n\nRedshift Statement without rule parameter")
    print("---------------------------------------")
    execute_rule("<rule_id>")

    print("\nAthena Statement without rule parameter")
    print("---------------------------------------")
    execute_rule("<rule id>")

    print("\nRule Groups (Postgress with rule parameter, Redshift statmeent with no rule parameter)")
    print("--------------------------------------------------------------------------------------")
    execute_rule_group("d6d38544-1040-11e9-ab14-d663bd873d73")

    print("\nSnowflake Statement without rule parameter")
    print("---------------------------------------")
    execute_rule("<rule id>")
    logging.info('Finished')


