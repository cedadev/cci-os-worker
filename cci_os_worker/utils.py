# encoding: utf-8
__author__ = 'Daniel Westwood'
__date__ = '05 Nov 2024'
__copyright__ = 'Copyright 2024 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'daniel.westwood@stfc.ac.uk'

import os
import yaml
import json
import logging

from cci_os_worker import logstream

from elasticsearch import Elasticsearch

logger = logging.getLogger(__name__)
logger.addHandler(logstream)
logger.propagate = False

def set_verbose(level: int):
    """
    Reset the logger basic config.
    """

    levels = [
        logging.WARN,
        logging.INFO,
        logging.DEBUG,
    ]

    if level >= len(levels):
        level = len(levels) - 1

    for name in logging.root.manager.loggerDict:
        lg = logging.getLogger(name)
        lg.setLevel(levels[level])

    lg = logging.getLogger('root')
    lg.setLevel(levels[level])

def load_config(conf: str):
    """
    Load a conf.yaml file to a dictionary
    """
    if os.path.isfile(conf):
        with open(conf) as f:
            config = yaml.safe_load(f)
        return config
    else:
        logger.error(f'Config file {conf} unreachable')
        return None

def load_datasets(datafile_path: str):
    if not os.path.isfile(datafile_path):
        raise ValueError(
            f'Filepath {datafile_path} is not accessible'
        )

    with open(datafile_path) as f:
        datasets = [r.strip() for r in f.readlines()]
    return datasets

class UpdateHandler:

    def __init__(self, conf: dict, dryrun: bool = False, test: bool = False):
        """
        Initialise this class with config and other switches.
        """

        self._dryrun = dryrun
        self._test = test
        self._conf = conf

        api_key = conf['elasticsearch']['x-api-key']

        self.es = Elasticsearch(
            hosts=conf['elasticsearch']['hosts'],
            headers={'x-api-key': api_key})

    def _local_cache(self, filename, contents):
        """
        Cache contents of Opensearch record locally
        for test purposes."""

        if os.path.isfile(filename):
            try:
                with open(filename) as f:
                    refs = json.load(f)
                contents = contents | refs
            except:
                pass

        with open(filename,'w') as f:
            f.write(json.dumps(contents))

    def _warn_tagger_root(self):
        """
        Display warning message for unset tagger root
        """

        if not os.environ.get('JSON_TAGGER_ROOT'):
            logger.warning('!!!')
            logger.warning('JSON_TAGGER_ROOT environment variable is not set')
            logger.warning('!!!')

    def _ensure_cache(self):
        """
        Ensure the cache directory exists
        """

        if not os.path.isdir('cache/'):
            os.makedirs('cache/')

    def process_deposits(self, datafile_path: str, prefix: str = '', file_limit = None):
        """
        Process deposits from the dataset file given to this function
        """

        logger.info(f'Processing deposits for {datafile_path}')
        self._warn_tagger_root()

        if not os.path.isfile(datafile_path):
            raise ValueError(
                f'Filepath {datafile_path} is not accessible'
            )
        
        if self._dryrun:
            self._ensure_cache()

        with open(datafile_path) as f:
            datasets = [prefix + r.strip() for r in f.readlines()]

        if file_limit != None:
            datasets = datasets[:file_limit]

        total = len(datasets)

        fail_list = []
        # Process all files individually.
        for idx, fp in enumerate(datasets):
            status = self._process_file(fp, index=idx+1, total=total)
            if status != 0:
                logger.error(status)
                fail_list.append([fp, status])

        self._warn_tagger_root()
        return fail_list

    def process_removals(self, datafile_path: str):
        """
        Process removals from the dataset file given to this function
        """

        if not os.path.isfile(datafile_path):
            raise ValueError(
                f'Filepath {datafile_path} is not accessible'
            )

        with open(datafile_path) as f:
            datasets = [r.strip() for r in f.readlines()]

        fail_list = []
        # Process all files individually.
        for fp in datasets:
            status = self._remove_file(fp)
            if status != 0:
                logger.error(status)
                fail_list.append((fp, str(status)))
        return fail_list

    def _process_file(self, filepath: str, index: int = None, total: int = None):
        """
        Safe processing of filepath
        """

        try:
            self._single_process_file(filepath, index=index, total=total)
            return 0
        except Exception as err:
            return err