# encoding: utf-8
__author__ = 'Daniel Westwood'
__date__ = '05 Nov 2024'
__copyright__ = 'Copyright 2024 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'daniel.westwood@stfc.ac.uk'

from tag_scanner.tagger import ProcessDatasets
import logging

from .utils import load_config, load_datasets, UpdateHandler, ch

# Logger setup
logger = logging.getLogger(__name__)
logger.addHandler(ch)

class TagUpdateHandler:

    def __init__(self, conf: dict, dryrun: bool = False, test: bool = False, fc: int = None):
        
        logger.info('Loading Tag Updater')
        self._fc = fc

        super().__init__(conf, dryrun=dryrun, test=test)

    def process_deposits(self, datafile_path: str):

        datasets = load_datasets(datafile_path)

        pds = ProcessDatasets(json_files=json_file)
        pds.process_datasets(datasets, self._fc or len(datasets))

def _get_command_line_args():
    """
    Get the command line arguments for the facet scan
    """
    parser = argparse.ArgumentParser(description='Entrypoint for the CCI OS Worker on the CMD Line')
    parser.add_argument('datafile_path', type=str, help='Path to the "datasets.txt" file')
    parser.add_argument('conf', type=str, help='Path to Yaml config file for Elasticsearch')

    parser.add_argument('-d','--dryrun', dest=dryrun, action='store_true', help='Perform in dryrun mode')
    parser.add_argument('-t','--test', dest=test, action='store_true', help='Perform in test/staging mode')

    parser.add_argument(
            '--file_count',
            help='how many .nc files to look at per dataset',
            type=int, default=0
        )

    args = parser.parse_args()

    return {
        'datafile_path': args.datafile_path,
        'conf': args.conf,
        'file_count':args.file_count,
        'dryrun': args.dryrun,
        'test': args.test
    }

def tag_main(args: dict = None):
    if args is None:
        args = _get_command_line_args()
    if isinstance(args['conf'], str):
        conf = load_config(args['conf'])

    ts = TagUpdateHandler(conf, dryrun=args['dryrun'], test=args['test'], fc=args['file_count'])
    fail_list = ts.process_deposits(args['datafile_path'])

if __name__ == '__main__':
    tag_main()