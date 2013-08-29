#!/usr/bin/env python

import ringo
from adapters.openerp import (deleteRecord,
                              searchRecord,
                              readRecord)
from xmlrpclib import Fault
import logging

_logger = logging.getLogger(__name__)

if __name__ == "__main__":
    # Find attached records
    partnerIdsWithInvoices = readRecord('account.invoice', [], fields='partner_id')

    # Find all records
    allIds = searchRecord('res.partner', [('customer','=','True')])

    # And destroy the unattached records
    unattachedIds = [ x for x in allIds if 
                      x not in partnerIdsWithInvoices ]
    for id in unattachedIds:
        try:
            deleteRecord('res.partner', id)
        except Fault:
            _logger.warn("Error deleting partner with id %s.", id)
