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

from facet_scanner.core.facet_scanner import FacetScanner
from ceda_elasticsearch_tools.elasticsearch import CEDAElasticsearchClient

class FacetUpdateHandler:

    def __init__(self, conf: dict, dryrun: bool = False, test: bool = False):
        """
        Initialise this class with the correct connections to 
        establish an elasticsearch client.
        """
        logger.info('Loading Facet Updater')

        self._dryrun = dryrun
        self._test = test

        self.facet_scanner = FacetScanner()

        api_key = ''

        self.es = CEDAElasticsearchClient(headers={'x-api-key': api_key})

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

        # Process all files individually.
        for fp in datasets:
            self._process_file(fp)

    def _process_file(self, filepath: str):
        """
        Perform facet scanning for a specific filepath
        """

        logger.info(f'Processing {filepath}')

        # Get the handler for this filepath
        handler = self.facet_scanner.get_handler(filepath)

        # Extract the facets
        facets = handler.get_facets(filepath)

        # Build the project dictionary using the handlers project name attr
        project = {
            'projects': {
                handler.project_name: facets
            }
        }

        index = self.conf.get('files_index', 'name')

        id = hashlib.sha1(filepath.encode(errors="ignore")).hexdigest()

        # Send facets to elasticsearch
        self.es.update(
            index=index,
            id=id,
            body={'doc': project, 'doc_as_upsert': True}
        )