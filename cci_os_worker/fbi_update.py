# encoding: utf-8
__author__ = 'Daniel Westwood'
__date__ = '05 Nov 2024'
__copyright__ = 'Copyright 2024 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'daniel.westwood@stfc.ac.uk'

"""
Much of the following was extracted from:
https://github.com/cedadev/rabbit-fbi-indexer

Please see this repository for further details
"""

import magic as magic_number_reader

from .utils import load_config, UpdateHandler, ch
from .path_tools import PathTools
from .errors import HandlerError, DocMetadataError

from ceda_fbs.src.fbs.proc.file_handlers.handler_picker import HandlerPicker

# Logger setup
logger = logging.getLogger(__name__)
logger.addHandler(ch)

def get_file_header(filename):
    """
    :param filename : The file to be read.
    :returns: First line of the file.
    """
    with open(filename, 'r') as fd:
        first_line = fd.readline()

    return first_line.replace("\n", "")


def get_bytes_from_file(filename, num_bytes):
    """
    :param filename : The file to be read.
    :param num_bytes : number of bytes to read.
    :returns: The first num_bytes from the file.
    """

    try:
        fd = open(filename, 'r')
        bytes_read = fd.read(num_bytes)
        fd.close()
    except IOError:
        return None

    return bytes_read

class FBIUpdateHandler(UpdateHandler):
    """
    Class to handle the updates to the FBI from newly ingested datasets.
    """

    def __init__(self, conf: dict, dryrun: bool = False, test: bool = False):

        logger.info('Loading FBI Updater')

        super().__init__(conf, dryrun=dryrun, test=test)

        if self._test:
            self._index = self._conf['fbi_files_test_index']
        else:
            self._index = self._conf['fbi_files_index']

        esconf = {
            'headers': {
                'x-api-key': self._conf['elasticsearch']['es_api_key']
            },
                'retry_on_timeout': True,
                'timeout': 30
        }

        # Initialise the Elasticsearch connection
        self.index_updater = CedaFbi(
            index=self._index['name'],
            **esconf
        )
        ldap_hosts = self._conf['ldap_configuration']['hosts']
        self.ldap_interface = LDAPIdentifier(server=ldap_hosts, auto_bind=True)

        self.pt = PathTools()

    def __process_file(self, path) -> None:
        """
        Take the given file path and add it to the FBI index
        """

        logger.info(f'Depositing {path}')

        handler = HandlerPicker.pick_best_handler(path)

        calculate_md5 = self._index['calculate_md5']
        scan_level = self._index['scan_level']

        if handler is None:
            raise HandlerError(filename=path)

        handler_instance = handler(path, scan_level, calculate_md5=calculate_md5)
        doc = handler_instance.get_metadata()

        if doc is None:
            raise DocMetadataError(filename=path)

        spot = self.pt.spots.get_spot(path)

        if spot is not None:
            doc[0]['info']['spot_name'] = spot

        # Replace the UID and GID with name and group
        uid = doc[0]['info']['user']
        gid = doc[0]['info']['group']

        doc[0]['info']['user'] = self.ldap_interface.get_user(uid)
        doc[0]['info']['group'] = self.ldap_interface.get_group(gid)

        indexing_list = [{
            'id': self.pt.generate_id(path),
            'document': self._create_body(doc)
        }]

        self.index_updater.add_files(indexing_list)

    @staticmethod
    def _create_body(file_data: Tuple[Dict]) -> Dict:
        """
        Create the fbi-index document body from the handler
        response

        :param file_data: extracted metadata from the FBI file handler
        :return: FBI document body
        """

        data_length = len(file_data)

        doc = file_data[0]
        if data_length > 1:
            if file_data[1] is not None:
                doc['info']['phenomena'] = file_data[1]

            if data_length == 3:
                if file_data[2] is not None:
                    doc['info']['spatial'] = file_data[2]

        return doc

    def _process_deletions(self, path: str) -> None:
        """
        Take the given file path and delete it from the FBI index

        :param path: File path
        """

        deletion_list = [
            {'id': self.pt.generate_id(path)}
        ]
        try:
            self.index_updater.delete_files(deletion_list)
        except BulkIndexError as e:
            pass
        """
        Creates the FBI document from the rabbit message.
        Does not touch the filesystem
        :param message: IngestMessage object
        :return: document to index to elasticsearch
        """

        filename = os.path.basename(message.filepath)
        dirname = os.path.dirname(message.filepath)
        file_type = os.path.splitext(filename)[1]

        if len(file_type) == 0:
            file_type = "File without extension."

        return [{
            'info': {
                'name_auto': filename,
                'type': file_type,
                'directory': dirname,
                'size': message.filesize,
                'name': filename,
                'location': 'on_disk'
            }
        }]

def _get_command_line_args():
    """
    Get the command line arguments for the facet scan
    """
    parser = argparse.ArgumentParser(description='Entrypoint for the CCI OS Worker on the CMD Line')
    parser.add_argument('datafile_path', type=str, help='Path to the "datasets.txt" file')
    parser.add_argument('conf', type=str, help='Path to Yaml config file for Elasticsearch')

    parser.add_argument('-d','--dryrun', dest=dryrun, action='store_true', help='Perform in dryrun mode')
    parser.add_argument('-t','--test', dest=test, action='store_true', help='Perform in test/staging mode')

    args = parser.parse_args()

    return {
        'datafile_path': args.datafile_path,
        'conf': args.conf,
        'dryrun': args.dryrun,
        'test': args.test
    }

def fbi_main(args: dict = None):
    if args is None:
        args = _get_command_line_args()
    if isinstance(args['conf'], str):
        conf = load_config(args['conf'])

    fb = FBIUpdateHandler(conf, dryrun=args['dryrun'], test=args['test'])
    fail_list = fb.process_deposits(args['datafile_path'])

if __name__ == '__main__':
    fbi_main()
