# encoding: utf-8
__author__ = 'Daniel Westwood'
__date__ = '11 Apr 2025'
__copyright__ = 'Copyright 2024 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'daniel.westwood@stfc.ac.uk'

from pathlib import Path
from pathlib import _ignore_error as pathlib_ignore_error
from cci_os_worker import logstream
import logging
import asyncio
import os
from typing import Union
import re
import argparse
import aiofiles.os as aos
import glob
import json

from .utils import set_verbose, load_config

logger = logging.getLogger(__name__)
logger.addHandler(logstream)
logger.propagate = False

SYMLINK = None
DEPOSIT = None

def check_valid_path(path):
    """
    Check that we have been given a real directory
    :param path:
    :return: boolean
    """
    if path == '/':
        raise Exception('Cannot scan from root')

    if not bool(os.path.exists(path) and os.path.isdir(path)):
        raise OSError('{} is not a directory'.format(path))
    
def walk_storage_links(path: str, depth: int = 0, max_depth: int = None):
    """
    Used within the archive to follow links to storage pots but ignore links which are
    back within the archive and could be circular.
    :param path:
    :param depth:
    :param max_depth:
    :return:
    """
    top = os.fspath(path)
    dirs = []
    nondirs = []

    # We may not have read permission for top, in which case we can't
    # get a list of the files the directory contains.  os.walk
    # always suppressed the exception then, rather than blow up for a
    # minor reason when (say) a thousand readable directories are still
    # left to visit.  That logic is copied here.
    try:
        scandir_it = os.scandir(top)
    except OSError:
        return

    if max_depth:
        if depth >= max_depth:
            return

    with scandir_it:
        while True:
            try:
                try:
                    entry = next(scandir_it)
                except StopIteration:
                    break
            except OSError as error:
                logger.error(error)
                return

            try:
                is_dir = entry.is_dir()
            except OSError:
                # If is_dir() raises an OSError, consider that the entry is not
                # a directory, same behaviour than os.path.isdir().
                is_dir = False

            if is_dir:
                dirs.append(entry.name)
            else:
                nondirs.append(entry.name)

    # Yield before recursion when going top down
    yield top, dirs, nondirs

    depth += 1
    # Recurse into sub-directories
    islink, join = os.path.islink, os.path.join
    for dirname in dirs:
        new_path = join(top, dirname)
        if islink(new_path):
            # Only follow links to storage locations
            if os.readlink(new_path).startswith('/datacentre'):
                yield from walk_storage_links(new_path, depth, max_depth)
        else:
            # If the path is not a link, recurse
            yield from walk_storage_links(new_path, depth, max_depth)

class RescanDirs:

    def __init__(
            self, 
            scan_path: str,
            scan_level: int = 1,
            use_rabbit: bool = False,
            conf: str = '',
            dryrun: bool = False, 
            skip_dirs: bool = False,
            skip_files: bool = False,
            recursive: bool = False,
            file_regex: Union[str,None] = None,
            extension: Union[str,None] = None,
            output: str = None
        ) -> None:

        if scan_path == '':
            self._init_from_args()
            return
        
        if scan_level == 1:
            # Directory level search
            check_valid_path(scan_path)

            self.scan_path = os.path.abspath(scan_path)
        else:
            self.scan_path = scan_path

        self.scan_level = scan_level
        self.use_rabbit = use_rabbit
        self.conf = load_config(conf)

        self.file_limit = self.conf.get('file_limit',None)

        # Default match any filename not starting with a . dot
        self._file_regex = file_regex
        self._extension  = extension

        self._dryrun = dryrun
        self._recursive = recursive
        self._output = output

        self.skip_dirs = skip_dirs
        self.skip_files = skip_files

        self.routing_key = 'elasticsearch_update_queue_opensearch_ingest'

    @property
    def file_regex(self):
        if self._file_regex is not None and self._extension is not None:
            regex = f'{self._file_regex}(.{self._extension})$'
            try:
                re.compile(regex)
                return regex
            except re.error:
                raise ValueError(
                    'Incompatible regex and extensions given - '
                    f'{regex} is not valid regular expression.'
                )
        if self._extension is not None:
            return f'.+?(.{self._extension})$'
        elif self._file_regex is not None:
            return self._file_regex
        else:
            return '.+'


    @property
    def max_depth(self):
        if self._recursive:
            return None
        else:
            return 1

    def _init_from_args(self):

        default_config = os.path.join(os.path.dirname(__file__), '../conf/rabbit_updater.ini')

        parser = argparse.ArgumentParser(description='Submit directories/items to be re-scanned.')
        parser.add_argument('dir', type=str, help='Directory to scan')
        parser.add_argument('-r', dest='recursive', action='store_true',
                            help='Recursive. Will include all directories below this point as well')

        parser.add_argument('-l','--scan-level',type=int, dest='scan_level',
                            help='Level of depth for scanning (1,2,3)')
        parser.add_argument('-R','--use-rabbit',dest='use_rabbit',
                            help='Deposit to rabbit queues or return list of paths')
        parser.add_argument('-v','--verbose', action='count', default=2, help='Set level of verbosity for logs')

        #parser.add_argument('--no-files', dest='nofiles', action='store_true', help='Ignore files')
        
        # Removed the ability to publish whole directories
        #parser.add_argument('--no-dirs', dest='nodirs', action='store_true', help='Ignore directories')
        parser.add_argument('--conf', type=str, default=default_config, help='Optional path to configuration file')
        parser.add_argument('--dry-run', dest='dryrun', action='store_true', help='Display log messages to screen rather than pushing to rabbit')

        parser.add_argument('-o','--output',dest='output', help='Store output list in a file.')

        parser.add_argument('--file-regex', dest='file_regex', 
                            help='Matching file regex, by default regex applies to all files not starting with "."',
                            default=None)
        parser.add_argument('--extension', dest='extension', 
                            help='Matching files by file extension.', default=None)
        args = parser.parse_args()

        set_verbose(args.verbose)

        self.__init__(
            args.dir,
            scan_level=args.scan_level,
            use_rabbit=args.use_rabbit,
            conf=args.conf,
            dryrun=args.dryrun,
            recursive=args.recursive,
            file_regex=args.file_regex,
            output=args.output,
            extension=args.extension
        )

    def _setup_rabbit(self):

        raise NotImplementedError(
            'Rabbit Queues have not been migrated to the CCI OS Worker package.' \
            'Please contact daniel.westwood@stfc.ac.uk for details on reimplementation if needed.'
        )

    def _submit_to_rabbit(self, item: str, itype = 'DEPOSIT') -> None:
        """
        Perform all operations for a specific file.
        All checks in relation to filepath should be checked
        before this stage.
        """
        raise NotImplementedError(
            'Rabbit Queues have not been migrated to the CCI OS Worker package.' \
            'Please contact daniel.westwood@stfc.ac.uk for details on reimplementation if needed.'
        )

        logger.info(f'Depositing {item} to Rabbit')

        msg = self.rabbit_connection.create_message(item, itype) #Deposit
        self.rabbit_connection.publish_message(msg, routing_key=self.routing_key) #'opensearch.tagger.cci')

    def _determine_paths(self):
        """
        Obtain the list of filepaths to enter
        into the facet scanner.

        This is either based on a file path, gathering
        all files under a directory (with a given regex),
        or based on a submission of JSON files.
        """

        scan_files = []

        if self.scan_level == 2: # All files under a directory
            logger.info('Scanning directories')
            for root, dirs, files in walk_storage_links(self.scan_path, max_depth=self.max_depth):
                for file in files:
                    if not re.match(self.file_regex, file):
                        continue

                    scan_files.append(f'{root}/{file}')

        else:
            # Pull files from json
            logger.info(f'Scanning JSON directory: {self.scan_path}')
            scanpath = f'{os.path.abspath(self.scan_path)}/**/*.json'
            jsons = glob.glob(scanpath, recursive=True)

            if self.file_limit:
                if len(jsons) != 0:
                    self.file_limit = int(self.file_limit/len(jsons))
                else:
                    raise FileNotFoundError(
                        'No JSON files were located.'
                    )
            else:
                self.file_limit = 99999999999999

            for js_count, file in enumerate(jsons):
                logger.info(f'Processing {file}')
                # Only want to track the changes in the JSON directory
                if not file.endswith('.json'):
                    continue

                with open(file) as reader:
                    data = json.load(reader)
                if 'datasets' not in data:
                    logger.warning(f'File {file}: missing "datasets" attribute')
                    continue
                ds = data['datasets']

                if not hasattr(ds, '__iter__'):
                    logger.warning(f'File {file}: "datasets" property is not iterable.')
                    continue

                add_files = []

                dfiles = []
                for ds_count, d in enumerate(ds):
                    # Find all single files
                    dfiles = [f for f in glob.glob(f'{d}/**/*.*', recursive=True) if re.match(self.file_regex,f)]
                    add_files += dfiles

                    logger.info(f' > (j: {js_count+1}/{len(jsons)}, d: {ds_count+1}/{len(ds)})')
                    logger.info(f' > {len(dfiles)} datasets ({file.split("/")[-1]}) ({len(scan_files)} total)')

                    if len(add_files) > self.file_limit:
                        add_files = add_files[:self.file_limit]
                        break

                scan_files += add_files

                logger.info(f'Added {len(add_files)} files from JSON file {js_count+1}')

        return scan_files

    def scan(self) -> list:
        output_files = 0

        if self.use_rabbit:
            self._setup_rabbit()

        deposit_paths = []

        for path in self._determine_paths():
            # Note the mkdir and symlink messages are no longer
            # required as all files have been ingested separately.

            output_files += 1

            # Create symlink message for file links
            if os.path.islink(path):
                action = SYMLINK
            else:
                action = DEPOSIT

            if self._dryrun:
                logger.info(f'{action}: {path}')
                continue

            if self.use_rabbit:
                self._submit_to_rabbit(path, itype=action)
            else:
                # Do something with the paths here.
                deposit_paths.append(path)

        logger.info(f'Submitted {output_files} files')
        return deposit_paths

    def save_data(self, outdata):

        if self._output is None:
            for line in outdata:
                print(line)
            return
        
        with open(self._output,'w') as f:
            f.write('\n'.join(outdata))

def check_timeout():

    async def path_exists(path) -> bool:
        try:
            await aos.stat(str(path))
        except OSError as e:
            if not pathlib_ignore_error(e):
                raise
            return ''
        except ValueError:
            # Non-encodable path
            return ''
        return True

    async def listfile():
        async with asyncio.timeout(10):
            await path_exists('/neodc/esacci/esacci_terms_and_conditions.txt')
    try:
        status = asyncio.run(listfile())
    except TimeoutError:
        logger.error('TIMEOUT: ESACCI Directories inaccessible')
        return True

    # If we didn't get a timeout error, can now perform a standard check.
    if not os.path.isfile('/neodc/esacci/esacci_terms_and_conditions.txt'):
        logger.error('INACCESSIBLE: ESACCI Directories inaccessible')
        return True
    return False

def rescan_directory():

    logger.info("Starting rescan check")
    if check_timeout():
        return
    logger.info("Archive access check: SUCCESS")

    r = RescanDirs('')
    if not r.use_rabbit:
        r.save_data(r.scan())
    else:
        _ = r.scan()