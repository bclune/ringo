#!/usr/bin/env python

from ringo import (ObjectNotFoundError,
                   dataMap,
                   keyMap,
                   dictGlob,
                   dictFilter,
                   memoized)
from adapters.crm import getCrmInformation
from adapters.openerp import (writeRecord,
                              searchRecord,
                              updateRecord)
import logging
import sys

_logger = logging.getLogger(__name__)

def getQuery():
    """ Return the MsCrm query. """
    return """
        SELECT 
            AccountBase.Name,
            AnnotationBase.Subject,
            AnnotationBase.NoteText
        FROM LogicSupplyMSCRM.dbo.AnnotationBase
        JOIN LogicSupplyMSCRM.dbo.AccountBase 
            ON LogicSupplyMSCRM.dbo.AnnotationBase.ObjectId = LogicSupplyMSCRM.dbo.AccountBase.AccountId
        """ 

def extractUniqueNames(records):
    names = set()
    for record in records:
        if record.get('Name') not in names:
            names.add(record.get('Name'))
    return names

@memoized
def getPartnerIdForName(name):
    ids = searchRecord('res.partner', [('name','=', name)])
    return ids[0] if ids else False

def notesWithName(name, records):
    notes = [ x.get('note') for x in records if x.get('Name') == name ]
    return "\n\n---------------------\n\n".join(notes)

if __name__ == '__main__':
    _logger.info("Beginning import of CRM notes...")
    records = getCrmInformation(getQuery())

    # Concatenate Subject and NoteText fields
    shouldConcatenate = lambda k: k in ['Subject', 'NoteText']
    concatenateFields = lambda d: dictGlob(d, shouldConcatenate, 'note',
                                           separator="\n\n")
    records = map(concatenateFields, records)

    # Group and fold notes field by name
    uniqueNames = extractUniqueNames(records)

    translatedRecords = [ {'partner_id': getPartnerIdForName(name), 
                            'internal_notes': notesWithName(name, records)} 
                          for name in uniqueNames 
                          if getPartnerIdForName(name)]

    total = len(translatedRecords)
    processed = 0
    skipped = 0
    _logger.info("Writing %d records to OpenERP...", len(records))
    for record in translatedRecords:
        processed += 1
        print "{}Importing {} of {} ({} duplicates)".format("\r", processed, total, skipped),
        sys.stdout.flush()
        updateRecord(
            'res.partner', 
            record.get('partner_id'), 
            {'comment': record.get('internal_notes')})
