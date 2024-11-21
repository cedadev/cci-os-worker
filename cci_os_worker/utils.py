# encoding: utf-8
__author__ = 'Daniel Westwood'
__date__ = '05 Nov 2024'
__copyright__ = 'Copyright 2024 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'daniel.westwood@stfc.ac.uk'

import os
import yaml
import logging

from cci_os_worker import logstream

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

    def process_deposits(self, datafile_path: str, prefix: str = '', file_limit = None):
        """
        Process deposits from the dataset file given to this function
        """

        logger.info(f'Processing deposits for {datafile_path}')

        if not os.path.isfile(datafile_path):
            raise ValueError(
                f'Filepath {datafile_path} is not accessible'
            )

        with open(datafile_path) as f:
            datasets = [prefix + r.strip() for r in f.readlines()]

        if file_limit != None:
            datasets = datasets[:file_limit]

        total = len(datasets)

        fail_list = []
        # Process all files individually.
        for idx, fp in enumerate(datasets):
            status = self._process_file(fp, index=idx, total=total)
            if status != 0:
                logger.error(status)
                fail_list.append(fp)
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
                fail_list.append((fp, status))
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