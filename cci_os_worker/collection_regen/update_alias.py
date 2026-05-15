# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '09 Jun 2020'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

import argparse
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
import ssl

from cci_facet_scanner.utils.elasticsearch import es_connection_kwargs

def get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--alias',
        dest='ALIAS',
        required=True,
        help='Collection index name'
    )

    parser.add_argument(
        '--target-index',
        dest='TARGET_INDEX',
        required=True,
        help='The index to apply the alias to'
    )

    parser.add_argument(
        '--es-api-key',
        dest='ES_API_KEY',
        required=True,
        help='Elasticsearch API key to allow write'
    )

    parser.add_argument(
        '--output',
        dest='OUTPUT',
        required=True,
        help='Output file for old index name'
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
    kwargs = {}
    args = get_args()

    es = Elasticsearch(
        **es_connection_kwargs(
            hosts=[args.HOST],
            api_key=args.ES_API_KEY
        )
    )
    
    # If the alias doesn't already exist then just set the alias
    try:
        aliases = es.indices.get_alias(index=args.ALIAS)
        old_index = list(aliases.keys())[0]
        with open(args.OUTPUT, 'w') as writer:
            writer.write(old_index)
        es.indices.update_aliases(body={
            "actions": [
                {"remove": {"index": old_index, "alias": args.ALIAS}},
                {"add": {"index": args.TARGET_INDEX, "alias": args.ALIAS}}
            ]
        })

    except NotFoundError:
        es.indices.update_aliases(body={
            "actions": [
                {"add": {"index": args.TARGET_INDEX, "alias": args.ALIAS}}
            ]
        })


if __name__ == '__main__':
    main()
