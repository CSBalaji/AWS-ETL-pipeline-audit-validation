#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
   Statement_Wrapper creates and excecutes postgressql query
   Author : Balaji Shankar
   Status : Development
"""
from abc import abstractmethod


# Common connection interface


class ConnectionInterface:

    def __init__(self):
        self.connect_str = ''

    # Create based on class name:
    @abstractmethod
    def execute_query(self, query): pass

# Test if the postgres connetion is working.
# execute_postgres_query("SELECT * from action")
