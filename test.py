#!/usr/bin/env python
import doctest
import logging
import ringo
import projects
import partners
import adapters.openerp as openerp
import adapters.crm as crm

_logger = logging.getLogger(__name__)

def runAllTests():
    _logger.info('Running entire test suite.')
    _logger.info('Testing ringo:')
    doctest.testmod(ringo)
    _logger.info('Testing projects:')
    doctest.testmod(projects)
    _logger.info('Testing openerp:')
    doctest.testmod(openerp)
    _logger.info('Testing crm:')
    doctest.testmod(crm)
    _logger.info('Testing partners:')
    doctest.testmod(partners, verbose=True)

if __name__ == "__main__":
    runAllTests()
