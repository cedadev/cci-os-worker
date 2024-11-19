# encoding: utf-8
__author__ = 'Daniel Westwood'
__date__ = '05 Nov 2024'
__copyright__ = 'Copyright 2024 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'daniel.westwood@stfc.ac.uk'

import logging
from cci_os_worker import logstream

logger = logging.getLogger(__name__)
logger.addHandler(logstream)
logger.propagate = False

class HandlerError(Exception):
    """A handler could not be identified for this file"""
    def __init__(self, filename = '<Not Given>', verbose = 0):
        self.message = f"A handler could not be identified for this file: {filename}"
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def get_str(self):
        return 'HandlerError'

class DocMetadataError(Exception):
    """Unable to retrieve metadata from the handler"""
    def __init__(self, filename = '<Not Given>', verbose = 0):
        self.message = f"Unable to retrieve metadata from the handler for this file: {filename}"
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def get_str(self):
        return 'DocMetadataError'