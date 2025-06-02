# encoding: utf-8
__author__ = 'Daniel Westwood'
__date__ = '05 Nov 2024'
__copyright__ = 'Copyright 2024 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'daniel.westwood@stfc.ac.uk'

import sys
import logging
import hashlib
import argparse
from elasticsearch import Elasticsearch

from cci_os_worker import logstream
from .utils import load_config

logger = logging.getLogger(__name__)
logger.addHandler(logstream)
logger.propagate = False

def get_args():

    parser = argparse.ArgumentParser(description='Entrypoint for the CCI OS Worker on the CMD Line')
    parser.add_argument('conf', type=str, help='Path to Yaml config file for Elasticsearch')

    parser.add_argument('-d','--dryrun', dest='dryrun', action='store_true', help='Perform in dryrun mode')
    parser.add_argument('-t','--test', dest='test', action='store_true', help='Perform in test/staging mode')
    parser.add_argument('--fileset', dest='fileset',help='Fileset to add to fails index, concat multiple files with ","')
    parser.add_argument('-o','--output', dest='output', default=None, help='Send fail list to an output file')

    args = parser.parse_args()

    return {
        'conf': args.conf,
        'dryrun': args.dryrun,
        'test': args.test,
        'fileset': args.fileset,
        'output': args.output
    }

def dump_errors():
    """
    Dump all the failed files in the ES index into a local file
    """

    args = get_args()
    conf = load_config(args['conf'])
    outfile = args['output']

    esconf = {
        'headers': {
            'x-api-key': conf['elasticsearch']['x-api-key']
        },
            'retry_on_timeout': True,
            'timeout': 30
    }

    if args['test']:
        index = conf['failure_index_test']['name']
    else:
        index = conf['failure_index']['name']
    
    es = Elasticsearch(
        hosts=conf['elasticsearch']['hosts'],
        headers=esconf['headers'])

    hits = es.search(index=index, doc_type="_doc")['hits']['hits']

    logger.info(f'Discovered {len(hits)} previous files')

    output = []
    for hit in hits:
        file = hit['_source']['info']['filename']
        output.append(file)
        id = hashlib.sha1(file.encode(errors="ignore")).hexdigest()

        es.delete(
            index=index,
            id=id
        )

    logger.info(f'Sending failed filelist to {outfile}')
    with open(outfile,'w') as f:
        f.write('\n'.join(output))

def add_errors():
    """
    Add all the failed files to an ES index for later retrieval.
    """
    
    args = get_args()

    conf = load_config(args['conf'])

    filenames = args['fileset'].split(',')
    if len(filenames) == 1:
        filenames = [filenames]
    
    esconf = {
        'headers': {
            'x-api-key': conf['elasticsearch']['x-api-key']
        },
            'retry_on_timeout': True,
            'timeout': 30
    }
    if args['test']:
        index = conf['failure_test_index']['name']
    else:
        index = conf['failure_index']['name']
    
    es = CEDAElasticsearchClient(headers=esconf['headers'])
    
    for f in filenames:
        status = f.split('.')[0]

        with open(f) as g:
            content = [r.strip() for r in g.readlines()]

        for file in content:

            info = {
                'filename':file,
                'status':status
            }

            logger.info(f'{file}: {status} - uploaded')

            id = hashlib.sha1(file.encode(errors="ignore")).hexdigest()

            es.update(
                index=index,
                id=id,
                body={'doc': {'info':info}, 'doc_as_upsert': True}
            )
