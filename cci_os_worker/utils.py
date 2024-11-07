# encoding: utf-8
__author__ = 'Daniel Westwood'
__date__ = '05 Nov 2024'
__copyright__ = 'Copyright 2024 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'daniel.westwood@stfc.ac.uk'

import os
import yaml
import logging

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

def load_config(conf: str):
    """
    Load a conf.yaml file to a dictionary
    """
    if os.path.isfile(conf):
        with open(conf) as f:
            config = yaml.safe_load(f)
        return config
    else:
        raise ValueError(f'Config file {conf} unreachable')

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

    def process_deposits(self, datafile_path: str):
        """
        Process deposits from the dataset file given to this function
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
            status = self._process_file(fp)
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

    def _process_file(self, filepath: str):
        """
        Safe processing of filepath
        """

        try:
            self.__process_file(filepath)
            return 0
        except Exception as err:
            return err