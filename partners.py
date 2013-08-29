#!/usr/bin/env python

from ringo import (ObjectNotFoundError,
                   dataMap,
                   keyMap,
                   dictGlob,
                   dictFilter,
                   memoized)
from adapters.crm import getCrmInformation
from adapters.openerp import (writeRecord,
                              updateRecord,
                              searchRecord)
import logging
import sys

_logger = logging.getLogger(__name__)

def getQuery():
    """ Return the MsCrm query. """
    return """
        SELECT DISTINCT
            account_name AS 'name', 
            delivery_street_address as 'street',
            delivery_city as 'city',
            delivery_state as 'state',
            delivery_country as 'country',
            delivery_postcode as 'zip',
            customers_telephone as 'phone',
            customers_email_address as 'email',
            employees_name as 'user_id'
        FROM LogicSupplyMSCRM.dbo.orders_margin_crm_openerp
        WHERE (YEAR(invoice_date) > 2009 and active = 1)
        """ 

def getPaymentCheckId():
    """ Return the id for payment type 'Check'. """
    return searchRecord('lgx.res.paypref', [('name', '=', 'Check')])[0]

def getCustomerDepositAcctId():
    """ Return the id for the customer deposit account. """
    return searchRecord('account.account', [('code', '=', '21511-02')])[0]

@memoized
def translateValue(key, value):
    """ Translate the value based on the given key. The function_lookup dict
        returns a reference to an inner function given a key, which then
        operates on the value. If the key does not exist in function_lookup,
        use the dummy function idem(x) = x.

        See 'testMemoizedFunctions()' for doctests.
    """
    def _doctest_false(value):
        return False

    def getCountryId(name):
        """ Return the OpenERP id of the country with the given name. See
            'testMemoizedFunctions()' for doctests.
        """
        ids = searchRecord('res.country',[('name', '=', name)])
        return ids[0] if ids else 0

    def getStateId(name):
        """ Return the OpenERP id of the state with the given name. See
            'testMemoizedFunctions()' for doctests.
        """
        ids = searchRecord('res.country.state',[('name', '=', name)])
        return ids[0] if ids else 0

    def fixEncoding(name):
        return name

    def getUserId(name):
        """ Search for an OpenERP user with the given name and return his or 
            her uid.
        >>> searchUser('Administrator')
        [1]
        >>> searchUser('The Hamburgerlar')
        """
        uid = searchRecord('res.users', [('name','=',name)])
        return uid[0] if uid else False

    idem = lambda x: x
    function_lookup = {
        '_doctest_false': _doctest_false,
        'country': getCountryId,
        'state': getStateId,
        'name': fixEncoding,
        'user_id': getUserId
        }
    return function_lookup.get(key, idem)(value)

def getAdditionalFields():
    """ Return constant fields for OpenERP. """
    return {
        'customer': True,
        'supplier': False,
        'type': 'default',
        'lgx_payment_preference': getPaymentCheckId(),
        'unearned_revenue_id': getCustomerDepositAcctId(),
        'is_company': True,
    }

@memoized
def getPartnerIdForName(name):
    ids = searchRecord('res.partner', [('name','=',name)])
    return ids[0] if ids else False

@memoized
def partnerExists(name):
    """ Return True if partner exists and False otherwise. 

        See 'testMemoizedFunctions()' for doctests.
    """
    try:
        pid = getPartnerIdForName(name)
    except Exception, e:
        _logger.error('Error searching for partner with name %s: %s', name, e)
        return False
    return bool(pid)

def testMemoizedFunctions():
    """ Run doctests for memoized functions. Doctest will not run tests
        directly from decorated functions, but we can run them here.

    >>> translateValue('_doctest_false', True)
    False
    >>> translateValue('some_other_key', 'value')
    'value'

    >>> partnerExists('Logic Supply, Inc.')
    True
    >>> partnerExists('The Hamburgerlar')
    False
    """
    pass

def splitFilter(predicate, items):
    trueList = []
    falseList = []
    for item in items:
        if predicate(item):
            trueList.append(item)
        else:
            falseList.append(item)
    return trueList, falseList

if __name__ == '__main__':
    _logger.info("Beginning import of CRM partners...")
    records = getCrmInformation(getQuery())
    additional_fields = getAdditionalFields()

    # translate keys and data from the query
    translateToIds = lambda d: dataMap(d, translateValue)
    records = map(translateToIds, records)

    # Filter records corresponding to existing OpenERP partners
    noPartnerExists = lambda d: not partnerExists(d.get('name'))
    recordsToCreate, recordsToUpdate = splitFilter(noPartnerExists, records)

    """
    total = len(recordsToCreate)
    processed = 0
    skipped = 0
    seen = set()
    _logger.info("Creating %d records in OpenERP...", len(records))
    for record in recordsToCreate:
        processed += 1
        print "{}Creating {} of {} ({} duplicates)".format("\r", processed, total, skipped),
        sys.stdout.flush()
        if record['name'] not in seen:
            # Add extra fields for OpenERP
            record.update(additional_fields)
            # Write record to OpenERP 
            writeRecord('res.partner', record)
            # Add name to the cache to avoid duplicate records
            seen.add(record['name'])
        else:
            skipped += 1
    """

    total = len(recordsToUpdate)
    processed = 0
    skipped = 0
    seen = set()
    _logger.info("Updating %d records in OpenERP...", len(records))
    for record in recordsToUpdate:
        processed += 1
        print "{}Updating {} of {} ({} duplicates)".format("\r", processed, total, skipped),
        sys.stdout.flush()
        if record['name'] not in seen:
            # Add extra fields for OpenERP
            record.update(additional_fields)
            # Write record to OpenERP 
            id = searchRecord('res.partner',[('name','=',record['name'])])
            updateRecord('res.partner', id, record)
            # Add name to the cache to avoid duplicate records
            seen.add(record['name'])
        else:
            skipped += 1
           

