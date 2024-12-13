# encoding: utf-8
__author__ = 'Daniel Westwood'
__date__ = '05 Nov 2024'
__copyright__ = 'Copyright 2024 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'daniel.westwood@stfc.ac.uk'

"""
Much of the following was extracted from:
https://github.com/cedadev/rabbit-fbi-indexer

Please see this repository for further details
"""

import argparse
import logging
import hashlib

from ceda_elasticsearch_tools.elasticsearch import CEDAElasticsearchClient

from typing import Tuple, Dict
import os

from .utils import load_config, UpdateHandler
from .path_tools import PathTools
from .errors import HandlerError, DocMetadataError

from cci_os_worker import logstream
from cci_os_worker.filehandlers import NetCdfFile, GenericFile
from cci_os_worker.filehandlers.util import LDAPIdentifier

logger = logging.getLogger(__name__)
logger.addHandler(logstream)
logger.propagate = False

def get_file_header(filename):
    """
    :param filename : The file to be read.
    :returns: First line of the file.
    """
    with open(filename, 'r') as fd:
        first_line = fd.readline()

    return first_line.replace("\n", "")

def get_bytes_from_file(filename, num_bytes):
    """
    :param filename : The file to be read.
    :param num_bytes : number of bytes to read.
    :returns: The first num_bytes from the file.
    """

    try:
        fd = open(filename, 'r')
        bytes_read = fd.read(num_bytes)
        fd.close()
    except IOError:
        return None

    return bytes_read

def get_id_from_path(path):
    return hashlib.sha1(path.encode(errors="ignore")).hexdigest()

class FBIUpdateHandler(UpdateHandler):
    """
    Class to handle the updates to the FBI from newly ingested datasets.
    """

    def __init__(self, conf: dict, dryrun: bool = False, test: bool = False):

        logger.info('Loading FBI Updater')

        super().__init__(conf, dryrun=dryrun, test=test)

        if self._test:
            self._index = self._conf['fbi_files_test_index']
        else:
            self._index = self._conf['fbi_files_index']

        esconf = {
            'headers': {
                'x-api-key': self._conf['elasticsearch']['x-api-key']
            },
                'retry_on_timeout': True,
                'timeout': 30
        }

        # Initialise the Elasticsearch connection
        self.es = CEDAElasticsearchClient(headers=esconf['headers'])

        ldap_hosts = self._conf['ldap_configuration']['hosts']
        self.ldap_interface = LDAPIdentifier(server=ldap_hosts, auto_bind=True)

        self.pt = PathTools()

    def _single_process_file(self, path, index,**kwargs) -> None:
        """
        Take the given file path and add it to the FBI index
        """

        logger.info(f'Depositing {path}')

        extension = os.path.splitext(path.split('/')[-1])
        extension = extension.lower()

        if extension == '.nc':
            handler = NetCdfFile
        else:
            handler = GenericFile

        calculate_md5 = self._conf.get('calculate_md5',False)

        if handler is None:
            raise HandlerError(filename=path)

        handler_instance = handler(path, 3, calculate_md5=calculate_md5)

        # FutureDetail: Remove manifest from 'doc' if unneeded (no indexing required.)
        doc, phenomena, spatial = handler_instance.get_metadata()

        if doc is None:
            raise DocMetadataError(filename=path)

        if phenomena:
            doc[0]['info']['phenomena'] = phenomena
        if spatial:
            doc[0]['info']['spatial'] = spatial

        spot = self.pt.spots.get_spot(path)

        if spot is not None:
            doc[0]['info']['spot_name'] = spot

        # Replace the UID and GID with name and group
        uid = doc[0]['info']['user']
        gid = doc[0]['info']['group']

        doc[0]['info']['user'] = self.ldap_interface.get_user(uid)
        doc[0]['info']['group'] = self.ldap_interface.get_group(gid)

        self.es.update(
            index=str(self._index),
            id=get_id_from_path(path),
            body={'doc': {'info':doc[0]['info']}, 'doc_as_upsert': True}
        )

    def _process_deletions(self, path: str) -> None:
        """
        Take the given file path and delete it from the Opensearch index

        :param path: File path
        """

        id = get_id_from_path(path)
        pass
        
def _get_command_line_args():
    """
    Get the command line arguments for the facet scan
    """
    parser = argparse.ArgumentParser(description='Entrypoint for the CCI OS Worker on the CMD Line')
    parser.add_argument('datafile_path', type=str, help='Path to the "datasets.txt" file')
    parser.add_argument('conf', type=str, help='Path to Yaml config file for Elasticsearch')

    parser.add_argument('-d','--dryrun', dest='dryrun', action='store_true', help='Perform in dryrun mode')
    parser.add_argument('-t','--test', dest='test', action='store_true', help='Perform in test/staging mode')

    args = parser.parse_args()

    return {
        'datafile_path': args.datafile_path,
        'conf': args.conf,
        'dryrun': args.dryrun,
        'test': args.test
    }

def fbi_main(args: dict = None):
    if args is None:
        args = _get_command_line_args()
    if isinstance(args['conf'], str):
        conf = load_config(args['conf'])

    fb = FBIUpdateHandler(conf, dryrun=args['dryrun'], test=args['test'])
    fail_list = fb.process_deposits(args['datafile_path'])

if __name__ == '__main__':
    fbi_main()
