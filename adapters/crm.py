import pymssql
import ConfigParser
import os
import logging

_logger = logging.getLogger(__name__)

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
                                           as_dict=True,
                                           charset='utf8',
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

def getCrmInformation(query):
    """ Gets query information from CRM.

    >>> getCrmInformation('''
    ...     SELECT Name FROM LogicSupplyMSCRM.dbo.RoleBase
    ...     WHERE RoleId='3C7CB75A-842D-42DA-A192-E4FE1E1195C9'
    ... ''')
    [{'Name': u'Sales'}]
    """
    _logger.debug("Retreiving records from CRM...")
    msCrm = MsCrmDb()
    return msCrm.getQueryResult(query)
