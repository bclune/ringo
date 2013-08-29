#!/usr/bin/env python
############################################
# Ringo - a functional OpenERP import tool #
############################################
# Filename: openerp.py
# Description: Bindings for erppeek to interface with OpenERP
# Author: Brendan Clune
# Date: 2013-06-21

import erppeek
import logging

_logger = logging.getLogger(__name__)

openErpHandler = erppeek.Client('http://localhost:17069',
                                db='bc_user_testing',
                                user='inc',
                                password='inc')

####################
## Data functions ##
####################

def writeRecord(model, vals, handler=openErpHandler):
    """ Create an OpenERP object with the given model and values.

    >>> handler = erppeek.Client('http://localhost:17069',
    ...                         db='bc_connector_test',
    ...                         user='inc',
    ...                         password='inc')
    >>> vals = {'name': 'Testing'}
    >>> id = writeRecord('crm.lead', vals, handler)
    >>> handler.read('crm.lead', id).get('name')
    'Testing'
    >>> handler.unlink('crm.lead', id)
    True
    """
    _logger.debug("Creating object %s with values %s", model, vals)
    return handler.create(model, vals)

def updateRecord(model, id, vals, handler=openErpHandler):
    """ Create an OpenERP object with the given model and values.

    >>> handler = erppeek.Client('http://localhost:17069',
    ...                         db='bc_connector_test',
    ...                         user='inc',
    ...                         password='inc')
    >>> vals = {'name': 'Toasting'}
    >>> id = writeRecord('crm.lead', vals, handler)
    >>> newVals = {'name': 'Testing'}
    >>> updateRecord('crm.lead', id, vals, handler)
    >>> handler.read('crm.lead', id).get('name')
    'Testing'
    >>> handler.unlink('crm.lead', id)
    True
    """
    _logger.debug("Updating object %s with id = %s and values %s", model, id,
                  vals)
    return handler.write(model, id, vals)

def searchRecord(model, domain, handler=openErpHandler):
    """ Create an OpenERP object with the given model and values.

    >>> handler = erppeek.Client('http://localhost:17069',
    ...                         db='bc_connector_test',
    ...                         user='inc',
    ...                         password='inc')
    >>> domain = [('login','=','admin')]
    >>> searchRecord('res.users', domain, handler)
    [1]
    """
    _logger.debug("Searching for object %s with domain %s", model, domain)
    return handler.search(model, domain)

def deleteRecord(model, id, handler=openErpHandler):
    """ Delete an OpenERP object with the given model and ids.
    """
    _logger.debug("deleting object %s with id %s", model, id)
    return handler.unlink(model, id)

def readRecord(model, id, fields=None, handler=openErpHandler):
    """ Read values from an OpenERP object with the given model and id
    """
    _logger.debug("Reading object %s with id %s", model, id)
    return handler.read(model, id, fields=None)
