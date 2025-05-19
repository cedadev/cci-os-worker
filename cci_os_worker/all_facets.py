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
import os
from datetime import datetime
import glob

import asyncio

from cci_facet_scanner.core.facet_scanner import FacetScanner
from elasticsearch import Elasticsearch

from cci_os_worker.filehandlers.util import LDAPIdentifier
from cci_os_worker.filehandlers import NetCdfFile, GenericFile
from cci_os_worker import logstream

from .path_tools import PathTools
from .directory import check_timeout
from .utils import load_config, UpdateHandler, set_verbose
from .errors import HandlerError, DocMetadataError

from slack_sdk import WebClient

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

        ldap_hosts = self._conf['ldap_configuration']['hosts']
        self.ldap_interface = LDAPIdentifier(server=ldap_hosts, auto_bind=True)

        self._spot_file = conf.get('spot_file',None)
        self.pt = PathTools(spot_file=self._spot_file)

    def _get_project_info(self, path):
        """
        Get project info for a specific path
        """

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

        return doc['info']

    def _single_process_file(self, filepath: str, index: int = None, total: int = None):
        """
        Perform facet scanning for a specific filepath
        """

        logger.info('--------------------------------')
        if index is None:
            logger.info(f'Processing {filepath.split("/")[-1]}')
        else:
            logger.info(f'Processing {filepath.split("/")[-1]} ({index}/{total})')

        # Get the handler for this filepath
        handler = self.facet_scanner.get_handler(filepath)

        # Extract the facets
        facets = handler.get_facets(filepath)

        # Build the project dictionary using the handlers project name attr
        project = {
            'info': self._get_project_info(filepath),
            'projects': {
                handler.project_name: facets
            }
        }

        if self._test:
            index = self._conf['facet_files_test_index']['name']
        else:
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

            self._local_cache(
                filename=f'cache/{filepath.split("/")[-1]}-cache.json',
                contents=project,
            )

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
    parser.add_argument('-v','--verbose', action='count', default=0, help='Set level of verbosity for logs')
    parser.add_argument('-f','--file-count', dest='file_count', type=int, help='Add limit to number of files to process.')
    parser.add_argument('-o','--output', dest='output', default=None, help='Send fail list to an output file')

    args = parser.parse_args()

    return {
        'datafile_path': args.datafile_path,
        'conf': args.conf,
        'dryrun': args.dryrun,
        'test': args.test,
        'prefix': args.prefix,
        'verbose': args.verbose,
        'file_count': args.file_count,
        'output': args.output
    }

def get_startup_slack(timestamp: str, file_count: int, is_sample: bool):
    """
    Get the startup message for the job.
    """
    job_summary = ['----------------------------------------------']
    job_summary.append(f'Starting CCI OS Worker Job ({timestamp})')
    job_summary.append('')
    job_summary.append('Json files:')
    jsons_new = os.environ.get("JSON_TAGGER_NEW")
    jsons = glob.glob(f'{jsons_new}/*.json')
    for js in jsons:
        job_summary.append(
            f' - {js.split("/")[-1]}'
        )
    job_summary.append('')

    ds_count = f'Datasets identified: {file_count}'
    if is_sample:
        ds_count += ' (sample run)'
    job_summary.append(ds_count)
    job_summary.append('----------------------------------------------')

    return '\n'.join(job_summary)

def get_completion_slack(timestamp: str, fail_list: list, num_jobs: int):
    """
    Get the completion message for the job"""
    job_complete = ['----------------------------------------------']
    job_complete.append(f'CCI OS Worker Job Complete! ({timestamp})')
    job_complete.append(f' - Datasets tagged: {num_jobs}')
    if len(fail_list) == 0: 
        job_complete.append(f' - No Failures detected! :)')
    else:
        job_complete.append(f' - {len(fail_list)} dataset(s) failed to scan :( ')

    job_complete.append('----------------------------------------------')
    return '\n'.join(job_complete)


def main(args: dict = None):
    if args is None:
        args = _get_command_line_args()
    if isinstance(args['conf'], str):
        conf = load_config(args['conf'])
    
    slack_cfg = conf.get('slack_cfg',None)
    slack_client = None

    timestamp = datetime.now().strftime("%H:%M:%S %d/%m/%y")

    is_sample = True
    file_count = conf.get('file_limit',None)
    if file_count is None:
        is_sample = False

        with open(args['datafile_path']) as f:
            file_count = len(f.readlines())

    if slack_cfg is not None:

        token = slack_cfg['token']
        channel = slack_cfg['channel']
        slack_client = WebClient(token=token)
        slack_client.chat_postMessage(
            channel=channel, 
            text=get_startup_slack(timestamp, file_count, is_sample),
            username=f'CCI Tag Bot - Startup'
        )
        logger.info('Message posted!')

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

    fs = FacetUpdateHandler(conf, dryrun=args['dryrun'], test=args['test'])
    fail_list = fs.process_deposits(args['datafile_path'], args['prefix'], file_limit=file_limit)

    print('Failed items:')
    print('\n'.join([f'`{i[0]}`: {i[1]}' for i in fail_list]))

    if slack_client is not None:

        slack_client.chat_postMessage(
            channel=channel, 
            text=get_completion_slack(timestamp, fail_list, file_count),
            username=f'CCI Tag Bot - Complete'
        )

        if len(fail_list) != 0:

            slack_client.chat_postMessage(
                channel=channel,
                text='\n'.join([f'`{i[0]}`: {i[1]}' for i in fail_list]),
                username='Failure reporter'
            )

if __name__ == '__main__':
    main()