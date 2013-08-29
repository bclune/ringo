#!/usr/bin/env python
############################################
# Ringo - a functional OpenERP import tool #
############################################
# Filename: ringo.py
# Description: A basic functional framework for translating data
# Author: Brendan Clune
# Date: 2013-06-21

import erppeek
import logging
import collections
import functools

FORMAT='%(asctime)-14s%(levelname)-6s: %(name)s: %(message)s'
DATEFORMAT='%(asctime)-14s%(name)s: %(levelname)s %(message)s'
logging.basicConfig(format=FORMAT, datefmt='%m-%d %H:%M', level='DEBUG')
_logger = logging.getLogger(__name__)


################
## Exceptions ##
################

class ObjectNotFoundError(Exception):
    pass

#############
## Mappers ##
#############

def keyMap(data, function):
    """ Apply the transformation function to each key of the data dict and
        return the result.

        The transformation function should accept a key, value pair as input
        and return keys of the new dict as output.

    >>> fruits = {'apples': 3, 'oranges': 7, 'bananas': 9}
    >>> crabify = lambda key, value: 'crab' + key
    >>> (keyMap(fruits, crabify) ==
    ...  {'crabapples': 3, 'craboranges': 7, 'crabbananas': 9})
    True
    """
    return {function(key, data[key]): data[key]
            for key in data
            if data[key] and function(key, data[key])}

def dataMap(data, function):
    """ Apply the transformation function to each value of the data dict and
        return the result.

        The transformation function should accept a key, value pair as input
        and return values of the new dict as output.

    >>> fruits = {'apples': 3, 'oranges': 7, 'bananas': 9}
    >>> squarify = lambda key, value: value * value
    >>> (dataMap(fruits, squarify) ==
    ...  {'apples': 9, 'oranges': 49, 'bananas': 81})
    True
    """
    return {key: function(key, data[key]) for key in data}

def dictGlob(data, function, globbed_key, separator="\n"):
    """ Concatenate selected fields into a new field.

        Keys for which function(key) returns True will be concatenated into
        data[globbed_key] in key-lexicographical order.

    >>> ingredients = {'bread': 'rye', 'filling_1' : 'lettuce',
    ...                'filling_2': 'tomato', 'filling_3': 'bacon'}
    >>> isFilling = lambda key: key.startswith('filling')
    >>> sandwich = dictGlob(ingredients, isFilling, 'toppings', separator=',')
    >>> sandwich == {'bread': 'rye', 'toppings': 'lettuce,tomato,bacon'}
    True
    """
    result = {}
    glob_keys = []
    glob = None
    for key in data:
        if function(key):
            glob_keys.append(key)
        else:
            result[key] = data[key]
    for key in sorted(glob_keys):
            if data[key]:
                glob = (glob + separator if glob else "") + data[key].encode('utf-8')
    result[globbed_key] = glob
    return result

def dictFilter(data, function):
    """ Filter a dictionary based on a filtering function.
    
        Returns a dictionary composed of key, value pairs for which 
        function(key, value) is True.

    >>> fruits = {'apple': 'red', 'tomato': 'red', 'banana': 'yellow'}
    >>> isRed = lambda key, value: value == 'red'
    >>> result = dictFilter(fruits, isRed)
    >>> result == {'tomato': 'red', 'apple': 'red'}
    True
    """
    return {key: data[key] for key in data if function(key, data[key])}


#####################
## Utility classes ##
#####################

class memoized(object):
    """ Decorator. Caches the result of the function passed to it to speed up
        database queries. """
    def __init__(self, function):
        assert hasattr(function, '__call__')
        self.function = function
        self._cache = {}

    def __call__(self, *args):
        if not isinstance(args, collections.Hashable):
            # if we can't hash args, just call function
            return self.function(*args)
        if args in self._cache:
            return self._cache[args]
        else:
            value = self.function(*args)
            self._cache[args] = value
            return value

    def __repr__(self):
        """ Return the function's docstring. """
        return self.function.__doc__

    def __get__(self, obj, objtype):
        """ Support instance methods. """
        return functools.partial(self.__call__, obj)
        
    
