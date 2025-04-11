# encoding: utf-8
__author__ = 'Daniel Westwood'
__date__ = '11 Apr 2025'
__copyright__ = 'Copyright 2024 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'daniel.westwood@stfc.ac.uk'

from pathlib import Path
from pathlib import _ignore_error as pathlib_ignore_error
from cci_os_worker import logstream
import logging
import asyncio
import os

import aiofiles.os as aos

logger = logging.getLogger(__name__)
logger.addHandler(logstream)
logger.propagate = False

def check_timeout():

    async def path_exists(path) -> bool:
        try:
            await aos.stat(str(path))
        except OSError as e:
            if not pathlib_ignore_error(e):
                raise
            return ''
        except ValueError:
            # Non-encodable path
            return ''
        return True

    async def listfile():
        async with asyncio.timeout(10):
            await path_exists('/neodc/esacci/esacci_terms_and_conditions.txt')
    try:
        status = asyncio.run(listfile())
    except TimeoutError:
        logger.error('TIMEOUT: ESACCI Directories inaccessible')
        return True

    # If we didn't get a timeout error, can now perform a standard check.
    if not os.path.isfile('/neodc/esacci/esacci_terms_and_conditions.txt'):
        logger.error('INACCESSIBLE: ESACCI Directories inaccessible')
        return True
    return False