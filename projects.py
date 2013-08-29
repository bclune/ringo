#!/usr/bin/env python

from ringo import (ObjectNotFoundError,
                   dataMap,
                   keyMap,
                   dictGlob,
                   memoized)
from adapters.crm import getCrmInformation
from adapters.openerp import (writeRecord,
                              searchRecord)
import logging

_logger = logging.getLogger(__name__)

################
## Exceptions ##
################

class UserNotFoundError(ObjectNotFoundError):
    pass
class PartnerNotFoundError(ObjectNotFoundError):
    pass

####################
## Data functions ##
####################

def getQuery():
    """ Return the MsCrm query. """
    return """
        SELECT AccountId,
               AccountIdName,
               ActualCloseDate,
               ActualValue,
               ActualValue_Base,
               CloseProbability,
               CustomerIdName,
               Description,
               EstimatedCloseDate,
               EstimatedValue,
               EstimatedValue_Base,
               CASE WHEN Name IS NOT NULL THEN Name ELSE 'Unknown' END AS Name,
               New_AdditionalHW1IdName,
               New_AdditionalHW2IdName,
               New_AdditionalHW3IdName,
               New_AmbientTemperatures,
               New_CaseCostPrice,
               new_caseidName,
               New_ContactIdName,
               New_CustomerApplication,
               New_CustomersEndUser,
               New_Developments,
               New_Enclosure,
               New_Environment,
               New_FannedFanless,
               New_HWSpecs,
               New_HardwareRequired,
               New_IORequirements,
               New_InputVoltage,
               New_MainboardCostPrice,
               New_Model,
               New_OperatingSystem,
               New_PerUnitCost,
               new_perunitcost_Base,
               New_PerformanceRequirementDetails,
               New_PotentialRevenue,
               new_potentialrevenue_Base,
               New_ProductClass,
               New_ProjectDescription,
               New_Purchasing,
               New_PurchasingClassification,
               New_SoftwareDetails,
               New_TotalQTY,
               OpportunityRatingCode,
               OriginatingLeadIdName,
               OriginatingLeadIdYomiName,
               OwnerIdName,
               PriceLevelIdName,
               Opportunity.StatusCode,
               Opportunity.StateCode,
               TransactionCurrencyIdName
          FROM LogicSupplyMSCRM.dbo.Opportunity
     LEFT JOIN LogicSupplyMSCRM.dbo.New_mainboard
            ON Opportunity.new_mainboardid = New_mainboard.New_mainboardId
         WHERE ActualCloseDate IS NULL and Opportunity.StateCode = 0
        """

def getAdditionalFields():
    """ Gets additional data to add to each OpenERP record. """
    return {'type': 'opportunity'}


######################
## Functions to map ##
######################

def translateKey(key, value):
    """ Translate the key from MsCRM to OpenERP. The key_lookup dict returns an
        OpenERP key given a MsCRM key.

    >>> translateKey('mscrm_key', 'some_value')
    'openerp_key'

    >>> translateKey('some_other_key', 'some_value')
    False
    """
    key_lookup = {
        'mscrm_key' : 'openerp_key',
        'New_AdditionalHW1IdName' : 'needs_3_storage',
        'New_AdditionalHW2IdName' : 'needs_4_processor',
        'New_AdditionalHW3IdName' : 'needs_5_psu',
        'new_caseidName' : 'needs_2_case',
        'New_ContactIdName' : 'contact_name',
        'OwnerIdName' : 'user_id',
        'CustomerIdName' : 'partner_id',
        'Name' : 'name',
        'Description' : 'needs_z_other',
        'EstimatedValue' : 'planned_revenue',
        'New_AmbientTemperatures' : 'needs_9_temp',
        'New_Purchasing' : 'needs_a_purchasing',
        'New_InputVoltage' : 'needs_b_voltage',
        'New_PerformanceRequirementDetails' : 'needs_e_performance',
        'New_IORequirements' : 'needs_7_io',
        'New_SoftwareDetails' : 'needs_8_software',
        'New_PerUnitCost' : 'needs_f_cost',
        'New_ProjectDescription' : 'description',
        'New_HWSpecs' : 'needs_c_add_hw',
        'New_Developments' : 'needs_d_developments',
        'New_CaseCostPrice' : 'needs_2_case_cost',
        'New_Model':  'needs_1_mainboard',
        'New_MainboardCostPrice' : 'needs_1_mainboard_cost',
        'New_TotalQTY' : 'needs_6_qty',
        'OpportunityRatingCode': 'probability',
        'StatusCode' : 'stage_id'
    }
    return key_lookup.get(key, False)

@memoized
def translateValue(key, value):
    """ Translate the value based on the given key. The function_lookup dict
        returns a reference to an inner function given a key, which then
        operates on the value. If the key does not exist in function_lookup,
        use the dummy function idem(x) = x.

    >>> translateValue('_doctest_false', True)
    False
    >>> translateValue('some_other_key', 'value')
    'value'
    """
    def _doctest_false(value):
        return False

    def searchUser(name):
        """ Search for an OpenERP user with the given name and return his or 
            her uid.
        >>> searchUser('Administrator')
        [1]
        >>> searchUser('The Hamburgerlar')
        """
        uid = searchRecord('res.users', [('name','=',name)])
        return uid[0] if uid else False

    def searchPartner(name):
        """ Search for an OpenERP partner with the given name and return his or 
            her uid.
        """
        pid = searchRecord('res.partner', [('name','=',name)])
        return pid[0] if pid else False

    def formatCost(cost):
        """ Format the cost (given as a float) and return a string with two
            decimals. 

        >>> formatCost(325.000000)
        '$325.00'
        """
        return '$%.2f' % cost

    def translateStageId(code):
        """ Translate Status id in CRM to a crm.case.stage id in OpenERP. """
        status_codes = {
            200000 : # Pre Sale
                searchRecord('crm.case.stage', [('name','=','Pre Sale')]),
            200001 : # Prototyping
                searchRecord('crm.case.stage', [('name','=','Prototyping')]),
            200005 : # Mass Production
                searchRecord('crm.case.stage', [('name','=','Forecasted')]),
            200008 : # One Time
                searchRecord('crm.case.stage', [('name','=','Forecasted')]),
            2      : # On Hold
                searchRecord('crm.case.stage', [('name','=','On Hold')]),
        }
        return (status_codes.get(code)[0] if status_codes.get(code) else
                searchRecord('crm.case.stage', [('name','=','New')])[0])
    
    def translateProbability(code):
        """ Translate probability from OpportunityRatingCode id in CRM to a
            numerical probability. """
        probability_codes = {
            200053: 90,
            200054: 80,
            200055: 65,
            200056: 40,
            200057: 20
        }
        return probability_codes.get(code) or 0

    def sanitizeName(name):
        return name or "Unknown"

    def addLabel(value, label):
        """ Prefix a value with the given label. """
        return "{}: {}".format(label, value)

    idem = lambda x: x
    function_lookup = {
        '_doctest_false': _doctest_false,
        'name': sanitizeName,
        'needs_3_storage': lambda x: addLabel(x,'Storage'),
        'needs_4_processor': lambda x: addLabel(x,'Processor'),
        'needs_5_psu': lambda x: addLabel(x,'PSU'),
        'needs_2_case': lambda x: addLabel(x,'Case'),
        'needs_z_other': lambda x: addLabel(x,'Other'),
        'needs_9_temp': lambda x: addLabel(x,'Temperature'),
        'needs_a_purchasing': lambda x: addLabel(x,'Purchasing'),
        'needs_b_voltage': lambda x: addLabel(x,'Voltage'),
        'needs_e_performance': lambda x: addLabel(x,'Performance'),
        'needs_7_io': lambda x: addLabel(x,'Input/Output'),
        'needs_8_software': lambda x: addLabel(x,'Software/OS'),
        'needs_f_cost': lambda x: addLabel(x,'Cost'),
        'needs_c_add_hw': lambda x: addLabel(x,'Additional Hardware'),
        'needs_d_developments': lambda x: addLabel(x,'Developments'),
        'needs_1_mainboard': lambda x: addLabel(x,'Mainboard'),
        'needs_6_qty': lambda x: addLabel(x,'Quantity'),
        'needs_1_mainboard_cost': formatCost,
        'needs_2_case_cost': formatCost,
        'needs_f_cost': lambda x: addLabel(formatCost(x), 'Cost'),
        'planned_revenue': lambda x: float(x),
        'user_id': searchUser,
        'partner_id': searchPartner,
        'stage_id' : translateStageId,
        'probability' : translateProbability,
        }
    return function_lookup.get(key, idem)(value)

def isNeed(key):
    """ Returns True if the key should be globbed into the description of needs
        field, and False otherwise.

    >>> isNeed('needs_one')
    True
    >>> isNeed('description_one')
    False
    """
    return key.startswith('needs')

def isCase(key):
    """ Returns True if the key should be globbed into the case field,
        and False otherwise.

    >>> isCase('needs_2_case_cost')
    True
    >>> isCase('needs_1_mainboard')
    False
    """
    return key.startswith('needs_2_case')

def isMainboard(key):
    """ Returns True if the key should be globbed into the mainboard field,
        and False otherwise.

    >>> isMainboard('needs_1_mainboard_cost')
    True
    >>> isMainboard('needs_2_case')
    False
    """
    return key.startswith('needs_1_mainboard')


##########
## Main ##
##########

if __name__ == '__main__':

    _logger.info("Beginning import of CRM projects...")
    records = getCrmInformation(getQuery())
    additional_fields = getAdditionalFields()

    # translate keys and data from the query
    translateDict = lambda d: dataMap(keyMap(d,translateKey), translateValue)
    records = map(translateDict, records)

    # Concatenate mainboard model and price
    globMainboard = lambda d: dictGlob(d, isMainboard, 'needs_1_mainboard',
                                       separator=": ")
    records = map(globMainboard, records)

    # Concatenate case model and price
    globCase = lambda d: dictGlob(d, isCase, 'needs_2_case',
                                       separator=": ")
    records = map(globCase, records)

    # Concatenate description of needs field
    globNeeds = lambda d: dictGlob(d, isNeed, 'description_of_needs')
    records = map(globNeeds, records)


    _logger.info("Writing records to OpenERP...")
    total = len(records)
    processed = 0
    for record in records:
        processed += 1
        print "{}Importing {} of {}".format("\r", processed, total),
        # Add extra fields for OpenERP
        record.update(additional_fields)
        writeRecord('crm.lead', record)

