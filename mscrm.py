#!/usr/bin/env python
################################################
# Wayne - an osCommerce to OpenERP import tool #
################################################
# Filename: mscrm.py
# Description: Handler for osCommerce database transactions
# Author: Brendan Clune
# Date: 2012-11-29

import pymssql
import ConfigParser
import os

class MsCrmDb:
    """ Provides methods to get data from the CRM database
    """
    def __init__(self):
        os.environ['TDSVER'] = '7.0'
        config = ConfigParser.ConfigParser()
        config.read('config.cfg')
        self.host = config.get('CRM', 'host')
        self.user = config.get('CRM', 'user')
        self.password = config.get('CRM', 'password')
        self.database = config.get('CRM', 'database')

    def _getCursor(self):
        self._connection = pymssql.connect(host=self.host,
                                           user=self.user,
                                           password=self.password,
                                           database=self.database,
                                           as_dict=True
                                           )
        return self._connection.cursor()

    def _closeCursor(self, cursor):
        cursor.close()
        self._connection.close()

    def getQueryResult(self, query):
        cursor = self._getCursor() 
        cursor.execute(query)
        result = cursor.fetchall()
        self._closeCursor(cursor)
        return result
