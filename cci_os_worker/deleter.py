# encoding: utf-8
__author__ = 'Daniel Westwood'
__date__ = '05 Nov 2024'
__copyright__ = 'Copyright 2024 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'daniel.westwood@stfc.ac.uk'

import logging
import hashlib
import argparse
import os

from typing import Union

from cci_facet_scanner.core.facet_scanner import FacetScanner
from ceda_elasticsearch_tools.elasticsearch import CEDAElasticsearchClient
from fbi_directory_check.utils import check_timeout

from .utils import load_config, UpdateHandler, set_verbose

from cci_os_worker import logstream

logger = logging.getLogger(__name__)
logger.addHandler(logstream)
logger.propagate = False

class ElasticsearchDeleter(UpdateHandler):

    def __init__(self, conf: dict, dryrun: bool = False, test: bool = False):
        """
        Initialise this class with the correct connections to 
        establish an elasticsearch client.
        """
        logger.info('Loading ES Deleter')

        super().__init__(conf, dryrun=dryrun, test=test)

    def _remove_file(self, filepath):
        """
        Delete all records for a specific dataset
        """

        index = self._conf['facet_files_index']['name']

        id = hashlib.sha1(filepath.encode(errors="ignore")).hexdigest()

        if not self._dryrun:
            try:
                self.es.delete(
                    index=index,
                    id=id,
                )
                return 0
            except:
                return 1
        else:
            logger.info(f'DRYRUN: Skipped deleting {filepath.split("/")[-1]}')
            return 0
        
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
    parser.add_argument('-o','--output', dest='output', default=None, help='Send fail list to an output file')

    args = parser.parse_args()

    return {
        'datafile_path': args.datafile_path,
        'conf': args.conf,
        'dryrun': args.dryrun,
        'test': args.test,
        'verbose': args.verbose-1,
        'output': args.output
    }

def main(args: dict = None):
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

    set_verbose(args['verbose'])

    deleter = ElasticsearchDeleter(conf, dryrun=args['dryrun'], test=args['test'])
    fail_list = deleter.process_removals(args['datafile_path'])

    logger.info('Failed items:')
    for f in fail_list:
        logger.info(f)

    if args['output'] is not None and fail_list != []:
        with open(args['output'],'w') as f:
            f.write('\n'.join(fail_list))

if __name__ == '__main__':
    main()