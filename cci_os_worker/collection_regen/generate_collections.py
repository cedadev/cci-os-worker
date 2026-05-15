# encoding: utf-8
"""
Generate Opensearch Collections for the given root collection.
This is picked using an archive path e.g. /neodc/esacci
"""
__author__ = 'Richard Smith'
__date__ = '09 Jun 2020'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

import argparse
from cci_facet_scanner.core.facet_scanner import FacetScanner
import logging

es_logger = logging.getLogger('elasticsearch')
es_logger.setLevel(logging.WARNING)

urllib3_logger = logging.getLogger('urllib3.connectionpool')
urllib3_logger.setLevel(logging.WARNING)


def get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--collection-index',
        dest='COLLECTION_INDEX',
        required=True,
        help='Collection index name'
    )

    parser.add_argument(
        '--file-index',
        dest='FILE_INDEX',
        required=False,
        help='File index name',
        default='opensearch-files'
    )

    parser.add_argument(
        '--es-api-key',
        dest='ES_API_KEY',
        required=True,
        help='Elasticsearch API key to allow write'
    )

    parser.add_argument(
        '--collection-root',
        dest='COLLECTION_ROOT',
        required=True,
        help='Path to match correct handler to handle collection export'
    )
    parser.add_argument(
        '--host',
        dest='HOST',
        required=False,
        default='https://elasticsearch.ceda.ac.uk',
        help='Elasticsearch Host'
    )

    return parser.parse_args()


def main():
    args = get_args()

    scanner = FacetScanner()
    handler = scanner.get_handler(
        args.COLLECTION_ROOT,
        hosts=[args.HOST],
        api_key=args.ES_API_KEY
    )

    handler.export_collections(args.COLLECTION_INDEX, args.FILE_INDEX)


if __name__ == '__main__':
    main()
