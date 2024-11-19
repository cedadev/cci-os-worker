# encoding: utf-8
__author__ = 'Daniel Westwood'
__date__ = '05 Nov 2024'
__copyright__ = 'Copyright 2024 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'daniel.westwood@stfc.ac.uk'

# Opens the file provided in the datasets folder.
# Iterate over all provided dataset paths.

# facet_scanner.get_handler('filepath')
# facets = handler.get_facets('filepath')

import logging
import hashlib
import argparse

from facet_scanner.core.facet_scanner import FacetScanner
from ceda_elasticsearch_tools.elasticsearch import CEDAElasticsearchClient

from .utils import load_config, UpdateHandler, set_verbose

from cci_os_worker import logstream

logger = logging.getLogger(__name__)
logger.addHandler(logstream)
logger.propagate = False

class FacetUpdateHandler(UpdateHandler):

    def __init__(self, conf: dict, dryrun: bool = False, test: bool = False):
        """
        Initialise this class with the correct connections to 
        establish an elasticsearch client.
        """
        logger.info('Loading Facet Updater')

        super().__init__(conf, dryrun=dryrun, test=test)

        self.facet_scanner = FacetScanner()

        api_key = conf['elasticsearch']['x-api-key']

        self.es = CEDAElasticsearchClient(headers={'x-api-key': api_key})

    def _single_process_file(self, filepath: str, index: int = None, total: int = None):
        """
        Perform facet scanning for a specific filepath
        """

        logger.info('--------------------------------')
        if index is None:
            logger.info(f'Processing {filepath.split("/")[-1]}')
        else:
            logger.info(f'Processing {filepath.split("/")[-1]} ({index+1}/{total})')

        # Get the handler for this filepath
        handler = self.facet_scanner.get_handler(filepath, json_files=None)

        # Extract the facets
        facets = handler.get_facets(filepath)

        # Build the project dictionary using the handlers project name attr
        project = {
            'projects': {
                handler.project_name: facets
            }
        }

        index = self._conf['facet_files_index']['name']

        id = hashlib.sha1(filepath.encode(errors="ignore")).hexdigest()

        # Send facets to elasticsearch
        if not self._dryrun:
            self.es.update(
                index=index,
                id=id,
                body={'doc': project, 'doc_as_upsert': True}
            )
        else:
            logger.info(f'DRYRUN: Skipped updating for {filepath.split("/")[-1]}')

def _get_command_line_args():
    """
    Get the command line arguments for the facet scan
    """
    parser = argparse.ArgumentParser(description='Entrypoint for the CCI OS Worker on the CMD Line')
    parser.add_argument('datafile_path', type=str, help='Path to the "datasets.txt" file')
    parser.add_argument('conf', type=str, help='Path to Yaml config file for Elasticsearch')

    parser.add_argument('-d','--dryrun', dest='dryrun', action='store_true', help='Perform in dryrun mode')
    parser.add_argument('-t','--test', dest='test', action='store_true', help='Perform in test/staging mode')
    parser.add_argument('-p','--prefix', dest='prefix', default='', help='Prefix to apply to all filenames')
    parser.add_argument('-v','--verbose', action='count', default=2, help='Set level of verbosity for logs')
    parser.add_argument('-f','--file-count', dest='file_count', type=int, help='Add limit to number of files to process.')

    args = parser.parse_args()

    return {
        'datafile_path': args.datafile_path,
        'conf': args.conf,
        'dryrun': args.dryrun,
        'test': args.test,
        'prefix': args.prefix,
        'verbose': args.verbose-1,
        'file_count': args.file_count
    }

def facet_main(args: dict = None):
    if args is None:
        args = _get_command_line_args()
    if isinstance(args['conf'], str):
        conf = load_config(args['conf'])

    set_verbose(args['verbose'])

    fs = FacetUpdateHandler(conf, dryrun=args['dryrun'], test=args['test'])
    fail_list = fs.process_deposits(args['datafile_path'], args['prefix'], file_limit=args['file_count'])

    # Register the fail_list with logs so it can be picked up in reruns.

if __name__ == '__main__':
    facet_main()