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
from fbi_directory_check.utils import check_timeout

from typing import Tuple, Dict
import os

from .utils import load_config, UpdateHandler, set_verbose
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

        self._spot_file = conf.get('spot_file',None)

        super().__init__(conf, dryrun=dryrun, test=test)

        if self._test:
            self._index = self._conf['facet_files_test_index']['name']
        else:
            self._index = self._conf['facet_files_index']['name']

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

        self.pt = PathTools(spot_file=self._spot_file)

    def _single_process_file(self, path, index: int = 0, total: int = 0, **kwargs) -> None:
        """
        Take the given file path and add it to the FBI index
        """

        logger.info('--------------------------------')
        if index is None:
            logger.info(f'Processing {path.split("/")[-1]}')
        else:
            logger.info(f'Processing {path.split("/")[-1]} ({index}/{total})')

        extension = os.path.splitext(path.split('/')[-1])[-1]
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
        if len(doc) > 1:
            doc = doc[0]

        if phenomena:
            doc['info']['phenomena'] = phenomena
        if spatial:
            doc['info']['spatial'] = spatial

        spot = self.pt.spots.get_spot(path)

        if spot is not None:
            doc['info']['spot_name'] = spot

        # Replace the UID and GID with name and group
        uid = doc['info']['user']
        gid = doc['info']['group']

        doc['info']['user'] = self.ldap_interface.get_user(uid)
        doc['info']['group'] = self.ldap_interface.get_group(gid)

        # Send facets to elasticsearch
        if not self._dryrun:
            self.es.update(
                index=str(self._index),
                id=get_id_from_path(path),
                body={'doc': {'info':doc['info']}, 'doc_as_upsert': True}
            )
        else:
            logger.info(f'DRYRUN: Skipped updating for {path.split("/")[-1]}')

            self._local_cache(
                filename=f'cache/{path.split("/")[-1]}-cache.json',
                contents=doc,
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
    parser.add_argument('-v','--verbose', action='count', default=2, help='Set level of verbosity for logs')
    parser.add_argument('-f','--file-count', dest='file_count', type=int, help='Add limit to number of files to process.')
    parser.add_argument('-o','--output', dest='output', default=None, help='Send fail list to an output file')

    args = parser.parse_args()

    return {
        'datafile_path': args.datafile_path,
        'conf': args.conf,
        'dryrun': args.dryrun,
        'test': args.test,
        'verbose': args.verbose,
        'file_count': args.file_count,
        'output': args.output
    }

def fbi_main(args: dict = None):
    if args is None:
        args = _get_command_line_args()
    if isinstance(args['conf'], str):
        conf = load_config(args['conf'])

    if conf is None:
        logger.error('Config file could not be loaded')
        return
    if not os.path.isfile(args['datafile_path']):
        logger.error(f'Inaccessible Datafile - {args["datafile_path"]}')
        return
    
    if check_timeout():
        logger.error('Check-timeout failed')
        return

    file_limit = conf.get('file_limit', None) or args.get('file_limit', None)

    set_verbose(args['verbose'])

    fb = FBIUpdateHandler(conf, dryrun=args['dryrun'], test=args['test'])
    fail_list = fb.process_deposits(args['datafile_path'], file_limit=file_limit)

    logger.info('Failed items:')
    for f in fail_list:
        logger.info(f)

    if args['output'] is not None and fail_list != []:
        with open(args['output'],'w') as f:
            f.write('\n'.join(fail_list))

if __name__ == '__main__':
    fbi_main()
