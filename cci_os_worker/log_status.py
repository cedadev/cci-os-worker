# encoding: utf-8
__author__ = 'Daniel Westwood'
__date__ = '05 Nov 2024'
__copyright__ = 'Copyright 2024 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'daniel.westwood@stfc.ac.uk'

import sys
import logging
import hashlib
from ceda_elasticsearch_tools.elasticsearch import CEDAElasticsearchClient

from cci_os_worker import logstream
from .utils import load_config

logger = logging.getLogger(__name__)
logger.addHandler(logstream)
logger.propagate = False

def dump_errors():
    """
    Dump all the failed files in the ES index into a local file
    """

    conf = load_config(sys.argv[1])
    outfile = sys.argv[2]

    esconf = {
        'headers': {
            'x-api-key': conf['elasticsearch']['x-api-key']
        },
            'retry_on_timeout': True,
            'timeout': 30
    }
    index = 'os-tagger-failures'
    
    es = CEDAElasticsearchClient(headers=esconf['headers'])

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
    
    if len(sys.argv) <= 1:
        logger.error('No config file given')
        return

    conf = load_config(sys.argv[1])
    filenames = sys.argv[2:]
    if len(filenames) == 0:
        logger.error('Nothing to do, no files given')
        return
    
    esconf = {
        'headers': {
            'x-api-key': conf['elasticsearch']['x-api-key']
        },
            'retry_on_timeout': True,
            'timeout': 30
    }
    index = 'os-tagger-failures'
    
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